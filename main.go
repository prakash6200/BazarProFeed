package main

import (
	"context"
	"encoding/json"
	"log"
	"os/signal"
	"syscall"
	"time"

	"feedprovider/config"
	"feedprovider/controller"
	"feedprovider/middleware"
	"feedprovider/models"
	"feedprovider/router"
	"feedprovider/services"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/limiter"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"gorm.io/gorm"
)

func main() {
	config.LoadConfig()
	config.ConnectDatabase()
	config.ConnectRedis()

	// Bounded startup window — the seed + cleanup + expiry sweep
	// steps below all run before we install the signal handler, so
	// without a deadline any slow DB/Redis could wedge the process
	// forever at boot with a port that never opens. 60s is plenty
	// for every sweep combined; if we blow it we log.Fatalf so the
	// orchestrator restarts us instead of leaving a half-booted ghost.
	startupCtx, startupCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer startupCancel()

	runStartupStep := func(name string, fn func(ctx context.Context)) {
		done := make(chan struct{}, 1)
		stepCtx, stepCancel := context.WithTimeout(startupCtx, 30*time.Second)
		defer stepCancel()
		go func() {
			fn(stepCtx)
			done <- struct{}{}
		}()
		select {
		case <-done:
		case <-stepCtx.Done():
			log.Fatalf("startup: %s exceeded deadline: %v", name, stepCtx.Err())
		}
	}

	runStartupStep("SeedSecurityData", func(_ context.Context) {
		config.SeedSecurityData(config.DB)
	})

	// Create Redis caches early so we can clean up before marking expired.
	zerodhaCache := services.NewRedisTickCache(config.RedisClient)
	globalCache := services.NewRedisTickCache(config.RedisClient)

	runStartupStep("cleanupExpiredRedisKeys", func(ctx context.Context) {
		cleanupExpiredRedisKeys(ctx, config.DB, zerodhaCache, globalCache)
	})

	runStartupStep("MarkExpiredInstruments", func(ctx context.Context) {
		if count, err := models.MarkExpiredInstruments(config.DB.WithContext(ctx)); err != nil {
			log.Printf("failed to mark expired instruments: %v", err)
		} else if count > 0 {
			log.Printf("marked %d expired instruments as deleted/inactive", count)
		}
	})
	runStartupStep("MarkExpiredGlobalInstruments", func(ctx context.Context) {
		if count, err := models.MarkExpiredGlobalInstruments(config.DB.WithContext(ctx)); err != nil {
			log.Printf("failed to mark expired global instruments: %v", err)
		} else if count > 0 {
			log.Printf("marked %d expired global instruments as deleted/inactive", count)
		}
	})

	runStartupStep("PurgeOldFeedData", func(ctx context.Context) {
		deleted, err := services.RunFeedRetentionOnce(ctx, config.DB, 7*24*time.Hour, 5000)
		if err != nil {
			log.Printf("feed retention startup cleanup failed: %v", err)
			return
		}
		if deleted > 0 {
			log.Printf("feed retention startup cleanup deleted %d rows older than 7 days", deleted)
		}
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Start background writers for both Redis tick caches so the hot
	// feed goroutines can fire-and-forget their cache updates.
	zerodhaCache.StartWriter(ctx)
	globalCache.StartWriter(ctx)

	tickHub := services.NewTickHub()
	zerodhaFeedService := services.NewZerodhaFeedService(config.App.Zerodha, config.DB, tickHub, zerodhaCache)
	zerodhaFeedService.Start(ctx)

	globMarketTickHub := services.NewTickHub()
	globMarketFeedService := services.NewGlobalMarketFeedService(config.DB, globalCache)
	globMarketFeedService.Start(ctx, globMarketTickHub)

	// Background candle builders — consume live ticks and pre-compute OHLC
	// bars for all intervals (1m/3m/5m/15m/30m) in-memory, flushing to Redis
	// every 30s.  The candle API reads from Redis instead of running the
	// heavy Postgres GROUP BY query on every request.
	zerodhaCandleBuilder := services.NewCandleBuilder("zerodha", config.RedisClient)
	globalCandleBuilder := services.NewCandleBuilder("global", config.RedisClient)

	zerodhaCandleBuilder.Start(ctx, tickHub)
	globalCandleBuilder.Start(ctx, globMarketTickHub)

	// Defer 24h candle warmup until after boot so restart latency stays low.
	// Running warmup sequentially avoids hammering DB+Redis with two large jobs
	// at the same time while feed/socket connections are still stabilizing.
	go startDeferredCandleWarmup(ctx, config.DB, 60*time.Second, zerodhaCandleBuilder, globalCandleBuilder)

	globSocketHub := config.NewSocketHub(config.DB)

	socketHub := config.NewSocketHub(config.DB)

	defer func() {
		config.CloseRedis()
		if err := config.CloseDatabase(); err != nil {
			log.Printf("failed to close database: %v", err)
		}
	}()

	app := fiber.New(fiber.Config{
		BodyLimit:    50 * 1024 * 1024,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
		// Cap concurrent connections to a sane level for a 4GB box.
		// Each fasthttp worker holds a few KB of state and each WS
		// connection holds 2 goroutines + 1 FD. 65536 was the old
		// value but is larger than the machine can actually serve,
		// so an abusive client could still push goroutine count
		// past useful memory. 16384 leaves comfortable headroom
		// (~64 MB stacks worst case) while absorbing real traffic
		// spikes; the rate limiters drop anything above.
		Concurrency: 16384,
		// Disable Fiber's startup banner in production logs.
		DisableStartupMessage: true,
	})

	// Panic recovery MUST be registered first so it wraps every
	// subsequent handler and middleware. Without this, a single
	// nil-deref or bad type assertion anywhere in the request path
	// takes the entire process down — which is the canonical
	// "port open, no response" failure mode an orchestrator cannot
	// detect until the TCP FIN arrives.
	app.Use(recover.New(recover.Config{
		EnableStackTrace: true,
	}))

	// Register /health BEFORE any middleware so it always responds
	// quickly, even if downstream middleware or DB-backed handlers
	// are saturated. Nginx health-checks this endpoint; keeping it
	// lightweight avoids the Nginx fallback page when the app is busy.
	app.Get("/health", func(c *fiber.Ctx) error {
		return c.Status(fiber.StatusOK).JSON(fiber.Map{
			"status_code": fiber.StatusOK,
			"message":     "service is healthy",
			"status":      "ok",
			"service":     "feedprovider",
		})
	})

	// /ready is a DEEP readiness probe — it reports 503 when a
	// downstream dep (DB/Redis) is unreachable or slow so the
	// orchestrator can stop routing traffic. Keep timeouts tight
	// here; a slow /ready is itself a readiness failure.
	app.Get("/ready", func(c *fiber.Ctx) error {
		checkCtx, cancel := context.WithTimeout(c.UserContext(), 2*time.Second)
		defer cancel()

		dbOK := true
		dbErr := ""
		if config.DB == nil {
			dbOK = false
			dbErr = "database not initialised"
		} else if sqlDB, err := config.DB.DB(); err != nil {
			dbOK = false
			dbErr = err.Error()
		} else if err := sqlDB.PingContext(checkCtx); err != nil {
			dbOK = false
			dbErr = err.Error()
		}

		redisOK := true
		redisErr := ""
		if config.RedisClient != nil {
			if err := config.RedisClient.Ping(checkCtx).Err(); err != nil {
				redisOK = false
				redisErr = err.Error()
			}
		}

		body := fiber.Map{
			"service":    "feedprovider",
			"db":         fiber.Map{"ok": dbOK, "error": dbErr},
			"redis":      fiber.Map{"ok": redisOK, "error": redisErr},
			"market":     fiber.Map{"indian_open": services.IsIndianMarketOpen(), "global_open": services.IsGlobalMarketOpen()},
			"checked_at": time.Now().UTC(),
		}
		if !dbOK || !redisOK {
			body["status_code"] = fiber.StatusServiceUnavailable
			body["status"] = "not_ready"
			return c.Status(fiber.StatusServiceUnavailable).JSON(body)
		}
		body["status_code"] = fiber.StatusOK
		body["status"] = "ready"
		return c.Status(fiber.StatusOK).JSON(body)
	})

	allowOrigins := config.App.Server.AllowOrigins
	if allowOrigins == "" {
		allowOrigins = "*"
	}
	app.Use(cors.New(cors.Config{
		AllowOrigins: allowOrigins,
		AllowMethods: "GET,POST,PUT,DELETE,OPTIONS",
		AllowHeaders: "Origin,Content-Type,Accept,Authorization",
	}))

	// Shared 429 response factory. The limiter middleware calls
	// LimitReached for every throttled request; returning a small,
	// static JSON keeps the rejection path cheap and avoids
	// downstream work.
	tooManyRequests := func(c *fiber.Ctx) error {
		return c.Status(fiber.StatusTooManyRequests).JSON(fiber.Map{
			"status_code": fiber.StatusTooManyRequests,
			"message":     "too many requests, slow down",
			"error":       "too many requests",
		})
	}

	// Auth rate-limiter — credential endpoints are the cheapest way
	// for an attacker to DoS us via bcrypt. 10 req/min/IP is enough
	// for a real user; bots get flattened.
	authLimiter := limiter.New(limiter.Config{
		Max:          10,
		Expiration:   time.Minute,
		LimitReached: tooManyRequests,
		KeyGenerator: func(c *fiber.Ctx) string { return c.IP() },
		Next: func(c *fiber.Ctx) bool {
			p := c.Path()
			return !(p == "/auth/signup" || p == "/auth/login" || p == "/user/signup" || p == "/user/login")
		},
	})
	app.Use(authLimiter)

	// Admin rate-limiter — admin traffic is low-volume by nature;
	// 120 req/min/IP gives headroom for a dashboard while still
	// preventing an admin token leak from being abused to scrape
	// everything.
	adminLimiter := limiter.New(limiter.Config{
		Max:          120,
		Expiration:   time.Minute,
		LimitReached: tooManyRequests,
		KeyGenerator: func(c *fiber.Ctx) string { return c.IP() },
		Next: func(c *fiber.Ctx) bool {
			return !startsWith(c.Path(), "/admin")
		},
	})
	app.Use(adminLimiter)

	// Generic API rate-limiter — per-IP cap on every JSON endpoint.
	// WS upgrade paths (/feed, /globalfeed) are exempted because a
	// long-lived connection is one request.
	apiLimiter := limiter.New(limiter.Config{
		Max:          600,
		Expiration:   time.Minute,
		LimitReached: tooManyRequests,
		KeyGenerator: func(c *fiber.Ctx) string { return c.IP() },
		Next: func(c *fiber.Ctx) bool {
			p := c.Path()
			if p == "/health" || p == "/ready" {
				return true
			}
			if startsWith(p, "/feed") || startsWith(p, "/globalfeed") {
				return true
			}
			return false
		},
	})
	app.Use(apiLimiter)

	adminController := controller.NewAdminController(config.DB, socketHub)
	instrumentController := controller.NewAdminInstrumentController(config.DB, zerodhaFeedService)
	globalInstrumentController := controller.NewAdminGlobalInstrumentController(config.DB, globMarketFeedService)
	zerodhaController := controller.NewAdminZerodhaController(config.DB, zerodhaFeedService)
	userController := controller.NewUserController(config.DB, socketHub, config.RedisClient)

	router.RegisterAdminRoutes(app, adminController, config.DB)
	router.RegisterAdminInstrumentRoutes(app, instrumentController, config.DB)
	router.RegisterAdminGlobalInstrumentRoutes(app, globalInstrumentController, config.DB)
	router.RegisterAdminZerodhaRoutes(app, zerodhaController, config.DB)
	router.RegisterUserRoutes(app, userController, config.DB)

	socketHub.SetInitialStateGetter(func(ctx context.Context, filterSym string) []json.RawMessage {
		ticks := zerodhaCache.Snapshot(ctx, services.ZerodhaTickPrefix, filterSym)
		status := services.MarketStatusOpen
		if !services.IsIndianMarketOpen() {
			status = services.MarketStatusClosed
		}
		msgs := make([]json.RawMessage, 0, len(ticks))
		for _, tick := range ticks {
			tick.MarketStatus = status
			if raw, err := json.Marshal(tick); err == nil {
				msgs = append(msgs, raw)
			}
		}
		return msgs
	})

	globSocketHub.SetInitialStateGetter(func(ctx context.Context, filterSym string) []json.RawMessage {
		ticks := globMarketFeedService.RecentTicks(filterSym)
		if len(ticks) == 0 {
			ticks = globalCache.Snapshot(ctx, services.GlobalTickPrefix, filterSym)
		}
		status := services.MarketStatusOpen
		if !services.IsGlobalMarketOpen() {
			status = services.MarketStatusClosed
		}
		msgs := make([]json.RawMessage, 0, len(ticks))
		for _, tick := range ticks {
			tick.MarketStatus = status
			if raw, err := json.Marshal(tick); err == nil {
				msgs = append(msgs, raw)
			}
		}
		return msgs
	})

	socketHub.RegisterRoutes(app, "/feed")
	socketHub.RegisterSingleInstrumentRoutes(app, "/feed/one")
	socketHub.RegisterBulkInstrumentRoutes(app, "/feed/bulk")
	globSocketHub.RegisterRoutes(app, "/globalfeed")
	globSocketHub.RegisterSingleInstrumentRoutes(app, "/globalfeed/one")
	globSocketHub.RegisterBulkInstrumentRoutes(app, "/globalfeed/bulk")

	events, unsubscribe := tickHub.Subscribe(1024)
	defer unsubscribe()
	go broadcastLoop(ctx, events, socketHub)

	globEvents, globUnsubscribe := globMarketTickHub.Subscribe(1024)
	defer globUnsubscribe()
	go broadcastLoop(ctx, globEvents, globSocketHub)

	go func() {
		<-ctx.Done()
		socketHub.CloseAll()
		globSocketHub.CloseAll()
		// Bounded graceful shutdown: a stuck import handler or a
		// slow WS writePump must not be able to hold the process
		// indefinitely. If the grace period elapses, ShutdownWithTimeout
		// force-closes remaining connections and returns, so the
		// orchestrator sees a clean exit instead of escalating to
		// SIGKILL mid-transaction.
		if err := app.ShutdownWithTimeout(30 * time.Second); err != nil {
			log.Printf("failed to shutdown fiber app: %v", err)
		}
		// Drain audit-log worker pool before we let the DB handle close.
		// Audit entries buffered at shutdown would otherwise be lost or
		// hit a closed connection.
		middleware.ShutdownAuditWriter()
	}()

	// Daily cleanup: mark expired instruments and purge Redis keys after market close.
	go dailyExpiryCleanupLoop(ctx, config.DB, zerodhaCache, globalCache, zerodhaFeedService, globMarketFeedService)
	go services.StartFeedRetentionCron(ctx, config.DB, 7*24*time.Hour, 5000)

	if err := app.Listen(config.App.Server.Host + ":" + config.App.Server.Port); err != nil {
		if ctx.Err() == nil {
			log.Fatal(err)
		}
	}
}

// startDeferredCandleWarmup waits for a post-boot delay, then runs candle
// cache warmup jobs sequentially in background. If shutdown begins before the
// delay elapses, warmup is skipped.
func startDeferredCandleWarmup(ctx context.Context, db *gorm.DB, delay time.Duration, builders ...*services.CandleBuilder) {
	if len(builders) == 0 {
		return
	}

	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
	}

	for _, b := range builders {
		if b == nil {
			continue
		}
		if ctx.Err() != nil {
			return
		}
		b.WarmupFromDB(db)
	}
}

// startsWith reports whether s has the given prefix. Kept local so we
// don't import the strings package for a single call.
func startsWith(s, prefix string) bool {
	if len(s) < len(prefix) {
		return false
	}
	return s[:len(prefix)] == prefix
}

// broadcastLoop consumes ticks from the hub and fans them out to WS
// clients. It marshals ONCE per event (not once per client) and uses
// the tick's Symbol field directly, avoiding reflection on the hot
// path. Slow subscribers never back-pressure the feed: sendCh is a
// buffered channel written with a non-blocking select inside
// SocketHub.BroadcastRaw.
func broadcastLoop(ctx context.Context, events <-chan services.NormalizedTick, hub *config.SocketHub) {
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-events:
			if !ok {
				return
			}
			data, err := json.Marshal(event)
			if err != nil {
				continue
			}
			hub.BroadcastRaw(event.Symbol, data)
		}
	}
}

// cleanupExpiredRedisKeys removes Redis tick/circuit keys for instruments that
// have already been identified as expired. Callers pass a bounded ctx so a
// slow DB/Redis at boot or during the nightly run cannot wedge us.
func cleanupExpiredRedisKeys(ctx context.Context, db *gorm.DB, zerodhaCache, globalCache *services.RedisTickCache) {
	if ctx == nil {
		ctx = context.Background()
	}
	dbCtx := db.WithContext(ctx)

	symbols, tokens, err := models.GetExpiredInstrumentIdentifiers(dbCtx)
	if err != nil {
		log.Printf("expiry cleanup: failed to get expired zerodha identifiers: %v", err)
	} else if len(symbols) > 0 {
		del := zerodhaCache.DeleteBySymbols(ctx, services.ZerodhaTickPrefix, symbols)
		del += zerodhaCache.DeleteCircuitByTokens(ctx, tokens)
		log.Printf("expiry cleanup: removed %d zerodha redis keys for %d expired symbols", del, len(symbols))
	}

	globSymbols, err := models.GetExpiredGlobalInstrumentSymbols(dbCtx)
	if err != nil {
		log.Printf("expiry cleanup: failed to get expired global symbols: %v", err)
	} else if len(globSymbols) > 0 {
		del := globalCache.DeleteBySymbols(ctx, services.GlobalTickPrefix, globSymbols)
		globalCache.RemoveFromSymbolList(ctx, services.GlobalSymbolListKey, globSymbols)
		log.Printf("expiry cleanup: removed %d global redis keys for %d expired symbols", del, len(globSymbols))
	}
}

// dailyExpiryCleanupLoop runs once per day at 16:00 IST (after Indian market close at 15:30)
// to mark expired instruments in DB, purge their Redis cache keys, and unsubscribe
// them from live feed services so they stop receiving data.
func dailyExpiryCleanupLoop(
	ctx context.Context,
	db *gorm.DB,
	zerodhaCache, globalCache *services.RedisTickCache,
	zerodhaFeed *services.ZerodhaFeedService,
	globalFeed *services.GlobalMarketFeedService,
) {
	loc, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		log.Printf("expiry cleanup: failed to load IST timezone: %v, falling back to UTC+5:30", err)
		loc = time.FixedZone("IST", 5*3600+30*60)
	}

	for {
		now := time.Now().In(loc)
		// Next run at 16:00 IST today, or tomorrow if already past.
		next := time.Date(now.Year(), now.Month(), now.Day(), 16, 0, 0, 0, loc)
		if now.After(next) {
			next = next.Add(24 * time.Hour)
		}
		wait := time.Until(next)
		log.Printf("expiry cleanup: next run scheduled at %s (in %s)", next.Format("2006-01-02 15:04 MST"), wait.Round(time.Second))

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}

		// Bound each daily run so a wedged DB during the sweep cannot
		// pin this goroutine forever; the next tick retries cleanly.
		runCtx, runCancel := context.WithTimeout(ctx, 2*time.Minute)

		cleanupExpiredRedisKeys(runCtx, db, zerodhaCache, globalCache)

		if count, err := models.MarkExpiredInstruments(db.WithContext(runCtx)); err != nil {
			log.Printf("expiry cleanup: failed to mark expired instruments: %v", err)
		} else if count > 0 {
			log.Printf("expiry cleanup: marked %d expired instruments as deleted/inactive", count)
		}
		if count, err := models.MarkExpiredGlobalInstruments(db.WithContext(runCtx)); err != nil {
			log.Printf("expiry cleanup: failed to mark expired global instruments: %v", err)
		} else if count > 0 {
			log.Printf("expiry cleanup: marked %d expired global instruments as deleted/inactive", count)
		}
		runCancel()

		// 3. Refresh feed services so they unsubscribe expired symbols from
		//    their in-memory lists and live WebSocket connections.
		if err := zerodhaFeed.RefreshFromDatabase(ctx); err != nil {
			log.Printf("expiry cleanup: zerodha feed refresh failed: %v", err)
		}
		if err := globalFeed.RefreshFromDatabase(ctx); err != nil {
			log.Printf("expiry cleanup: global feed refresh failed: %v", err)
		}
	}
}
