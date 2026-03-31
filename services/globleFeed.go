package services

import (
	"context"
	"encoding/json"
	"feedprovider/models"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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

	tickHub    *TickHub
	redisCache *RedisTickCache

	marketState       *MarketStateManager
	lastInboundTickAt atomic.Int64
}

func uniqueSymbols(symbols []string) []string {
	if len(symbols) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(symbols))
	result := make([]string, 0, len(symbols))
	for _, s := range symbols {
		norm := strings.ToUpper(strings.TrimSpace(s))
		if norm == "" {
			continue
		}
		if _, ok := seen[norm]; ok {
			continue
		}
		seen[norm] = struct{}{}
		result = append(result, norm)
	}
	return result
}

const (
	globalStaleTickCheckInterval = 15 * time.Second
	globalStaleTickMaxSilence    = 60 * time.Second
)

func NewGlobalMarketFeedService(db *gorm.DB, redisCache *RedisTickCache) *GlobalMarketFeedService {
	wsURL := os.Getenv("GLOBAL_MARKET_WS_URL")
	apiKey := os.Getenv("GLOBAL_MARKET_X_API_KEY")
	svc := &GlobalMarketFeedService{
		wsURL:       wsURL,
		apiKey:      apiKey,
		db:          db,
		marketState: NewMarketStateManager(),
		redisCache:  redisCache,
	}
	if redisCache != nil {
		ticks := redisCache.Snapshot(context.Background(), GlobalTickPrefix, "")
		for _, tick := range ticks {
			svc.marketState.Update(tick)
		}
		if len(ticks) > 0 {
			log.Printf("global market state pre-populated from redis: %d symbols", len(ticks))
		}
	}
	return svc
}

func (g *GlobalMarketFeedService) Start(ctx context.Context, tickHub *TickHub) {
	g.ctx, g.cancel = context.WithCancel(ctx)
	g.tickHub = tickHub
	if g.wsURL == "" {
		log.Printf("global market feed: GLOBAL_MARKET_WS_URL is not configured")
		return
	}

	dialer := websocket.DefaultDialer
	headers := http.Header{}
	headers.Set("x-api-key", g.apiKey)
	conn, _, err := dialer.Dial(g.wsURL, headers)
	if err != nil {
		log.Printf("global market feed: dial failed: %v", err)
		return
	}
	g.conn = conn

	var symbols []string
	if g.db != nil {
		dbSymbols, err := models.GetActiveGlobalInstrumentSymbols(g.db)
		if err != nil {
			log.Printf("global market feed: failed to load active symbols: %v", err)
		} else {
			symbols = uniqueSymbols(dbSymbols)
		}
	}

	// Fallback: if DB has no ACTIVE symbols, subscribe symbols from redis snapshot.
	if len(symbols) == 0 && g.redisCache != nil {
		cached := g.redisCache.Snapshot(context.Background(), GlobalTickPrefix, "")
		cachedSymbols := make([]string, 0, len(cached))
		for _, tick := range cached {
			cachedSymbols = append(cachedSymbols, tick.Symbol)
		}
		symbols = uniqueSymbols(cachedSymbols)
		if len(symbols) > 0 {
			log.Printf("global market feed: DB has no ACTIVE symbols, using %d redis-cached symbols", len(symbols))
		}
	}

	if len(symbols) == 0 {
		log.Printf("global market feed: no symbols available for subscription")
	} else {
		if err := g.Subscribe(symbols); err != nil {
			log.Printf("global market feed: subscribe failed: %v", err)
		} else {
			log.Printf("global market feed: subscribed %d symbols", len(symbols))
		}
	}

	go g.readLoop(tickHub)
	go g.rebroadcastStaleStateLoop()
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
			MarketStatus:     MarketStatusOpen,
		}
		updated := g.marketState.Update(tick)
		updated.MarketStatus = MarketStatusOpen
		g.lastInboundTickAt.Store(time.Now().UnixNano())
		tickHub.Publish(updated)
		if g.redisCache != nil && g.ctx != nil && g.ctx.Err() == nil {
			g.redisCache.Set(g.ctx, GlobalTickPrefix, updated)
		}
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

	ticks := g.marketState.Snapshot()
	if len(ticks) == 0 {
		return
	}

	if !IsGlobalMarketOpen() {
		for _, tick := range ticks {
			tick.MarketStatus = MarketStatusClosed
			g.tickHub.Publish(tick)
		}
		return
	}

	lastTickAt := g.lastInboundTickAt.Load()
	if lastTickAt == 0 {
		return
	}
	if time.Since(time.Unix(0, lastTickAt)) < globalStaleTickMaxSilence {
		return
	}

	for _, tick := range ticks {
		tick.MarketStatus = MarketStatusOpen
		g.tickHub.Publish(tick)
	}
}

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
}
