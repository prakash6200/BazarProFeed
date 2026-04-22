package middleware

import (
	"context"
	"log"
	"sync"
	"time"

	"feedprovider/models"

	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

// Audit logging must never block the request path. In production a
// saturated connection pool or a slow INSERT on admin_api_audit_logs
// would otherwise pin every admin request — which then pins every
// fasthttp worker — which makes the server unreachable on its open
// port. We decouple the write from the request with a buffered
// channel and a small fixed-size pool of writer goroutines. Drops
// are logged but never propagated back to the client.
const (
	auditQueueSize     = 4096
	auditWorkers       = 2
	auditWriteTimeout  = 3 * time.Second
	auditShutdownFlush = 5 * time.Second
)

type auditEntry struct {
	log *models.AdminAPIAuditLog
}

type auditWriter struct {
	db   *gorm.DB
	ch   chan auditEntry
	once sync.Once
	wg   sync.WaitGroup
	stop chan struct{}
}

var (
	auditWriterMu sync.Mutex
	activeWriter  *auditWriter
)

func newAuditWriter(db *gorm.DB) *auditWriter {
	w := &auditWriter{
		db:   db,
		ch:   make(chan auditEntry, auditQueueSize),
		stop: make(chan struct{}),
	}
	for i := 0; i < auditWorkers; i++ {
		w.wg.Add(1)
		go w.run()
	}
	return w
}

func (w *auditWriter) run() {
	defer w.wg.Done()
	for {
		select {
		case <-w.stop:
			// Drain any remaining entries best-effort before exiting.
			for {
				select {
				case entry := <-w.ch:
					w.write(entry)
				default:
					return
				}
			}
		case entry := <-w.ch:
			w.write(entry)
		}
	}
}

func (w *auditWriter) write(entry auditEntry) {
	if entry.log == nil || w.db == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), auditWriteTimeout)
	defer cancel()
	if err := models.CreateAdminAPIAuditLog(w.db.WithContext(ctx), entry.log); err != nil {
		// Swallow — the request has already completed and the client
		// does not care about audit-log failures. Log so ops can
		// still notice if writes are consistently failing.
		log.Printf("audit log write failed: %v", err)
	}
}

func (w *auditWriter) enqueue(entry auditEntry) {
	select {
	case w.ch <- entry:
	default:
		// Queue full — drop the oldest (lossy but bounded memory).
		// Preferable to blocking the request.
		select {
		case <-w.ch:
		default:
		}
		select {
		case w.ch <- entry:
		default:
		}
	}
}

// AdminAPIAccessAudit returns the Fiber middleware and lazily boots a
// shared audit writer pool. Each middleware invocation does only an
// O(1) channel send on the request path.
func AdminAPIAccessAudit(db *gorm.DB) fiber.Handler {
	auditWriterMu.Lock()
	if activeWriter == nil {
		activeWriter = newAuditWriter(db)
	}
	w := activeWriter
	auditWriterMu.Unlock()

	return func(c *fiber.Ctx) error {
		startedAt := time.Now()
		err := c.Next()
		latency := time.Since(startedAt)

		userValue := c.Locals("user")
		user, ok := userValue.(*models.User)
		if !ok || user == nil || !user.IsAdminRole() {
			return err
		}

		statusCode := c.Response().StatusCode()
		errorText := ""
		if err != nil {
			errorText = err.Error()
		}

		w.enqueue(auditEntry{log: &models.AdminAPIAuditLog{
			UserID:     user.ID,
			Username:   user.Username,
			Role:       user.EffectiveRole(),
			Method:     c.Method(),
			Path:       c.Path(),
			Query:      string(c.Request().URI().QueryString()),
			StatusCode: statusCode,
			IPAddress:  c.IP(),
			UserAgent:  c.Get("User-Agent"),
			LatencyMS:  latency.Milliseconds(),
			ErrorText:  errorText,
		}})

		return err
	}
}

// ShutdownAuditWriter signals the worker pool to flush pending
// entries and exit. Safe to call from main during graceful shutdown.
func ShutdownAuditWriter() {
	auditWriterMu.Lock()
	w := activeWriter
	auditWriterMu.Unlock()
	if w == nil {
		return
	}
	w.once.Do(func() { close(w.stop) })
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(auditShutdownFlush):
	}
}
