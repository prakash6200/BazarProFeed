package services

import (
	"context"
	"feedprovider/models"
	"log"
	"strings"
	"sync"
	"time"

	kiteconnect "github.com/zerodha/gokiteconnect/v4"
	"gorm.io/gorm"
)

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const (
	CircuitEventTypeHit  = "CIRCUIT_HIT"
	CircuitEventTypeOpen = "CIRCUIT_OPEN"

	CircuitSideUpper = "UPPER"
	CircuitSideLower = "LOWER"

	CircuitSegmentEquity = "EQUITY"
	CircuitSegmentMCX    = "MCX"

	// How often to refresh circuit limits from Zerodha REST API.
	circuitLimitRefreshInterval = 15 * time.Minute

	// Max instruments per GetQuote batch (Zerodha allows up to ~500).
	circuitQuoteBatchSize = 400

	// Small pause between consecutive API batches to stay within rate limits.
	circuitBatchPause = 500 * time.Millisecond
)

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

// CircuitState tracks per-symbol circuit detection state (in-memory, O(1)).
type CircuitState struct {
	Symbol     string
	Segment    string
	Upper      float64
	Lower      float64
	LTP        float64
	IsUpperHit bool
	IsLowerHit bool
	UpdatedAt  time.Time
}

// CircuitEvent is the WebSocket payload broadcast to all connected clients.
type CircuitEvent struct {
	Type       string  `json:"type"`
	Symbol     string  `json:"symbol"`
	Segment    string  `json:"segment"`
	Side       string  `json:"side"`
	Price      float64 `json:"price"`
	UpperLimit float64 `json:"upperLimit"`
	LowerLimit float64 `json:"lowerLimit"`
	Timestamp  string  `json:"timestamp"`
}

// broadcaster is a minimal interface so the detector doesn't depend on the
// concrete SocketHub type from the config package.
type broadcaster interface {
	BroadcastJSON(payload any)
}

// zerodhaTokenProvider is a minimal interface to obtain Zerodha API credentials
// without coupling to ZerodhaFeedService directly.
type zerodhaTokenProvider interface {
	GetAccessToken() string
	GetAPIKey() string
}

// CircuitDetectorService detects circuit hits/opens for EQUITY and MCX
// instruments by comparing live LTP (from tickHub) against circuit limits
// fetched periodically from Zerodha's REST GetQuote API.
type CircuitDetectorService struct {
	tickHub  *TickHub
	hub      broadcaster
	provider zerodhaTokenProvider
	db       *gorm.DB

	// states holds per-symbol detection state. Key = trading symbol.
	states sync.Map // map[string]*CircuitState

	// limits holds the latest circuit limits. Key = "EXCHANGE:SYMBOL".
	limits sync.Map // map[string]circuitLimit
}

type circuitLimit struct {
	Upper float64
	Lower float64
}

// ---------------------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------------------

func NewCircuitDetectorService(
	tickHub *TickHub,
	hub broadcaster,
	provider zerodhaTokenProvider,
	db *gorm.DB,
) *CircuitDetectorService {
	return &CircuitDetectorService{
		tickHub:  tickHub,
		hub:      hub,
		provider: provider,
		db:       db,
	}
}

// ---------------------------------------------------------------------------
// Start — launches two goroutines: limit refresher + tick consumer
// ---------------------------------------------------------------------------

func (d *CircuitDetectorService) Start(ctx context.Context) {
	// Goroutine 1: periodic limit refresh from Zerodha REST API.
	go d.limitRefreshLoop(ctx)

	// Goroutine 2: consume ticks, detect circuits, broadcast events.
	go d.tickConsumerLoop(ctx)

	log.Println("circuit detector started")
}

// ---------------------------------------------------------------------------
// Goroutine 1 — Limit Refresher
// ---------------------------------------------------------------------------

func (d *CircuitDetectorService) limitRefreshLoop(ctx context.Context) {
	// Initial fetch at startup.
	d.refreshLimits(ctx)

	ticker := time.NewTicker(circuitLimitRefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.refreshLimits(ctx)
		}
	}
}

func (d *CircuitDetectorService) refreshLimits(ctx context.Context) {
	apiKey := d.provider.GetAPIKey()
	accessToken := d.provider.GetAccessToken()
	if apiKey == "" || accessToken == "" {
		log.Println("circuit detector: skipping limit refresh — zerodha credentials not available")
		return
	}

	// Build instrument keys ("EXCHANGE:SYMBOL") for all active instruments.
	instruments := d.loadActiveInstruments(ctx)
	if len(instruments) == 0 {
		log.Println("circuit detector: no active instruments found")
		return
	}

	kc := kiteconnect.New(apiKey)
	kc.SetAccessToken(accessToken)

	keys := make([]string, 0, len(instruments))
	for _, inst := range instruments {
		keys = append(keys, inst.Exchange+":"+inst.TradingSymbol)
	}

	fetched := 0
	for i := 0; i < len(keys); i += circuitQuoteBatchSize {
		if ctx.Err() != nil {
			return
		}

		end := i + circuitQuoteBatchSize
		if end > len(keys) {
			end = len(keys)
		}
		batch := keys[i:end]

		quotes, err := kc.GetQuote(batch...)
		if err != nil {
			log.Printf("circuit detector: GetQuote batch %d–%d failed: %v", i, end, err)
			continue
		}

		for key, q := range quotes {
			if q.UpperCircuitLimit > 0 && q.LowerCircuitLimit > 0 {
				d.limits.Store(key, circuitLimit{
					Upper: q.UpperCircuitLimit,
					Lower: q.LowerCircuitLimit,
				})
				fetched++
			}
		}

		// Rate-limit pause between batches (skip after last batch).
		if end < len(keys) {
			select {
			case <-ctx.Done():
				return
			case <-time.After(circuitBatchPause):
			}
		}
	}

	log.Printf("circuit detector: refreshed limits for %d/%d instruments", fetched, len(keys))
}

// loadActiveInstruments fetches all active instruments without exchange/segment filtering.
func (d *CircuitDetectorService) loadActiveInstruments(ctx context.Context) []models.Instrument {
	if d.db == nil {
		return nil
	}

	dbCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var instruments []models.Instrument
	err := d.db.WithContext(dbCtx).
		Select("instrument_token", "trading_symbol", "exchange", "segment").
		Where("is_deleted = ?", false).
		Where("status = ?", models.InstrumentStatusActive).
		Find(&instruments).Error
	if err != nil {
		log.Printf("circuit detector: failed to load instruments: %v", err)
		return nil
	}
	return instruments
}

// ---------------------------------------------------------------------------
// Goroutine 2 — Tick Consumer (hot path)
// ---------------------------------------------------------------------------

func (d *CircuitDetectorService) tickConsumerLoop(ctx context.Context) {
	events, unsubscribe := d.tickHub.Subscribe(1024)
	defer unsubscribe()

	for {
		select {
		case <-ctx.Done():
			return
		case tick, ok := <-events:
			if !ok {
				return
			}
			d.processTick(tick)
		}
	}
}

func (d *CircuitDetectorService) processTick(tick NormalizedTick) {
	segment := d.classifySegment(tick.Exchange)
	if segment == "" {
		return // not EQUITY or MCX — skip
	}

	// O(1) limit lookup.
	key := tick.Exchange + ":" + tick.Symbol
	limVal, ok := d.limits.Load(key)
	if !ok {
		return // no limits fetched yet for this symbol
	}
	lim := limVal.(circuitLimit)

	if lim.Upper <= 0 || lim.Lower <= 0 {
		return // invalid limits
	}

	// Load or create per-symbol state.
	stateVal, _ := d.states.LoadOrStore(tick.Symbol, &CircuitState{
		Symbol:  tick.Symbol,
		Segment: segment,
	})
	state := stateVal.(*CircuitState)

	state.LTP = tick.LTP
	state.Upper = lim.Upper
	state.Lower = lim.Lower
	state.UpdatedAt = time.Now().UTC()

	now := time.Now().UTC().Format(time.RFC3339)

	// --- UPPER circuit detection ---
	if tick.LTP >= lim.Upper && !state.IsUpperHit {
		state.IsUpperHit = true
		d.hub.BroadcastJSON(CircuitEvent{
			Type:       CircuitEventTypeHit,
			Symbol:     tick.Symbol,
			Segment:    segment,
			Side:       CircuitSideUpper,
			Price:      tick.LTP,
			UpperLimit: lim.Upper,
			LowerLimit: lim.Lower,
			Timestamp:  now,
		})
	} else if tick.LTP < lim.Upper && state.IsUpperHit {
		state.IsUpperHit = false
		d.hub.BroadcastJSON(CircuitEvent{
			Type:       CircuitEventTypeOpen,
			Symbol:     tick.Symbol,
			Segment:    segment,
			Side:       CircuitSideUpper,
			Price:      tick.LTP,
			UpperLimit: lim.Upper,
			LowerLimit: lim.Lower,
			Timestamp:  now,
		})
	}

	// --- LOWER circuit detection ---
	if tick.LTP <= lim.Lower && !state.IsLowerHit {
		state.IsLowerHit = true
		d.hub.BroadcastJSON(CircuitEvent{
			Type:       CircuitEventTypeHit,
			Symbol:     tick.Symbol,
			Segment:    segment,
			Side:       CircuitSideLower,
			Price:      tick.LTP,
			UpperLimit: lim.Upper,
			LowerLimit: lim.Lower,
			Timestamp:  now,
		})
	} else if tick.LTP > lim.Lower && state.IsLowerHit {
		state.IsLowerHit = false
		d.hub.BroadcastJSON(CircuitEvent{
			Type:       CircuitEventTypeOpen,
			Symbol:     tick.Symbol,
			Segment:    segment,
			Side:       CircuitSideLower,
			Price:      tick.LTP,
			UpperLimit: lim.Upper,
			LowerLimit: lim.Lower,
			Timestamp:  now,
		})
	}
}

// classifySegment returns "EQUITY" or "MCX" based on the exchange string.
// Returns "" for any other exchange (F&O, CDS, etc.) — those are skipped.
func (d *CircuitDetectorService) classifySegment(exchange string) string {
	switch strings.ToUpper(exchange) {
	case models.InstrumentExchangeNSE, models.InstrumentExchangeNSEEqu:
		return CircuitSegmentEquity
	case models.InstrumentExchangeMCX, models.InstrumentExchangeMCXMini:
		return CircuitSegmentMCX
	default:
		return ""
	}
}
