package services

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"feedprovider/models"

	"github.com/gorilla/websocket"
	"gorm.io/gorm"
)

type MarketFeedProvider interface {
	Subscribe(symbols []string) error
	Unsubscribe(symbols []string) error
}

type GlobalMarketFeedService struct {
	wsURL   string
	apiKey  string
	db      *gorm.DB
	conn    *websocket.Conn
	writeMu sync.Mutex
	ctx     context.Context
	cancel  context.CancelFunc

	tickHub *TickHub

	marketState       *MarketStateManager
	lastInboundTickAt atomic.Int64
}

const (
	globalStaleTickCheckInterval = 15 * time.Second
	globalStaleTickMaxSilence    = 60 * time.Second
)

func NewGlobalMarketFeedService(db *gorm.DB) *GlobalMarketFeedService {
	wsURL := os.Getenv("GLOBAL_MARKET_WS_URL")
	apiKey := os.Getenv("GLOBAL_MARKET_X_API_KEY")
	return &GlobalMarketFeedService{
		wsURL:       wsURL,
		apiKey:      apiKey,
		db:          db,
		marketState: NewMarketStateManager(),
	}
}

// fallbackSymbols are used only when DB has no active global instruments configured.
var fallbackSymbols = []string{"EURUSD", "USDCHF", "GBPUSD", "XAUUSD", "XAGUSD"}

func (g *GlobalMarketFeedService) Start(ctx context.Context, tickHub *TickHub) {
	g.ctx, g.cancel = context.WithCancel(ctx)
	g.tickHub = tickHub
	dialer := websocket.DefaultDialer
	headers := http.Header{}
	headers.Set("x-api-key", g.apiKey)
	conn, _, err := dialer.Dial(g.wsURL, headers)
	if err != nil {
		log.Printf("global market feed: failed to connect to %s: %v", g.wsURL, err)
		return
	}
	g.conn = conn

	// Load ACTIVE symbols from DB; fall back to defaults if DB is empty or unavailable.
	symbols := g.loadSymbolsFromDB()
	if err := g.Subscribe(symbols); err != nil {
		log.Printf("global market feed: failed to subscribe to symbols: %v", err)
	}

	go g.readLoop(tickHub)
	go g.rebroadcastStaleStateLoop()
}

func (g *GlobalMarketFeedService) loadSymbolsFromDB() []string {
	if g.db == nil {
		log.Printf("global market feed: no DB configured, using fallback symbols")
		return fallbackSymbols
	}
	symbols, err := models.GetActiveGlobalInstrumentSymbols(g.db)
	if err != nil {
		log.Printf("global market feed: failed to load symbols from DB, using fallback: %v", err)
		return fallbackSymbols
	}
	if len(symbols) == 0 {
		log.Printf("global market feed: no active symbols in DB, using fallback symbols")
		return fallbackSymbols
	}
	log.Printf("global market feed: loaded %d symbol(s) from DB: %v", len(symbols), symbols)
	return symbols
}

func (g *GlobalMarketFeedService) readLoop(tickHub *TickHub) {
	for {
		_, message, err := g.conn.ReadMessage()
		if err != nil {
			return
		}
		var raw map[string]interface{}
		if err := json.Unmarshal(message, &raw); err != nil {
			continue
		}
		tick := NormalizedTick{
			Exchange:         asString(raw["exchange"]),
			Symbol:           asString(raw["name"]),
			LTP:              asFloat(raw["ltp"]),
			Open:             asFloat(raw["open"]),
			High:             asFloat(raw["high"]),
			Low:              asFloat(raw["low"]),
			Close:            asFloat(raw["close"]),
			BidPrice:         asFloat(raw["bid"]),
			AskPrice:         asFloat(raw["ask"]),
			BidQty:           0,
			AskQty:           0,
			TBQ:              int64(asFloat(raw["tbq"])),
			TSQ:              int64(asFloat(raw["tsq"])),
			OI:               int64(asFloat(raw["oi"])),
			Timestamp:        msToTime(raw["timestamp"]),
			NetChange:        asFloat(raw["ch"]),
			NetChangePercent: asFloat(raw["chp"]),
		}
		updated := g.marketState.Update(tick)
		g.lastInboundTickAt.Store(time.Now().UnixNano())
		tickHub.Publish(updated)
	}
}

func (g *GlobalMarketFeedService) rebroadcastStaleStateLoop() {
	ticker := time.NewTicker(globalStaleTickCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-g.ctx.Done():
			return
		case <-ticker.C:
			g.rebroadcastIfStale()
		}
	}
}

func (g *GlobalMarketFeedService) rebroadcastIfStale() {
	if g.tickHub == nil {
		return
	}

	lastTickAt := g.lastInboundTickAt.Load()
	if lastTickAt == 0 {
		return
	}

	if time.Since(time.Unix(0, lastTickAt)) < globalStaleTickMaxSilence {
		return
	}

	ticks := g.marketState.Snapshot()
	if len(ticks) == 0 {
		return
	}

	for _, tick := range ticks {
		g.tickHub.Publish(tick)
	}
}

// Helper functions for type conversion
func asString(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
func asFloat(v interface{}) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case float32:
		return float64(t)
	case int:
		return float64(t)
	case int64:
		return float64(t)
	case json.Number:
		f, _ := t.Float64()
		return f
	}
	return 0
}
func msToTime(v interface{}) time.Time {
	ms := int64(asFloat(v))
	return time.Unix(0, ms*int64(time.Millisecond))
}

func (g *GlobalMarketFeedService) Subscribe(symbols []string) error {
	if g.conn == nil {
		return nil
	}
	msg := map[string]interface{}{
		"event":   "subscribe",
		"symbols": symbols,
	}
	g.writeMu.Lock()
	defer g.writeMu.Unlock()
	return g.conn.WriteJSON(msg)
}

func (g *GlobalMarketFeedService) Unsubscribe(symbols []string) error {
	if g.conn == nil {
		return nil
	}
	msg := map[string]interface{}{
		"event":   "unsubscribe",
		"symbols": symbols,
	}
	g.writeMu.Lock()
	defer g.writeMu.Unlock()
	return g.conn.WriteJSON(msg)
}

func (g *GlobalMarketFeedService) Stop() {
	if g.cancel != nil {
		g.cancel()
	}
	if g.conn != nil {
		g.conn.Close()
	}
	// log removed: stopped
}
