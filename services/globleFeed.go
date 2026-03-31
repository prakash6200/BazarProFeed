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
	metaMu     sync.RWMutex
	metaBySym  map[string]globalInstrumentMeta

	marketState       *MarketStateManager
	lastInboundTickAt atomic.Int64
}

type globalInstrumentMeta struct {
	Expiry *time.Time
	Strike float64
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
		metaBySym:   make(map[string]globalInstrumentMeta),
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
	dialer.HandshakeTimeout = 10 * time.Second
	headers := http.Header{}
	headers.Set("x-api-key", g.apiKey)
	
	dialCtx, dialCancel := context.WithTimeout(ctx, 15*time.Second)
	defer dialCancel()
	
	conn, _, err := dialer.DialContext(dialCtx, g.wsURL, headers)
	if err != nil {
		log.Printf("global market feed: dial failed: %v", err)
		return
	}
	g.conn = conn

	var symbols []string
	if g.db != nil {
		g.loadInstrumentMeta()
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
			log.Printf("global market feed: readLoop error: %v", err)
			return
		}
		var raw map[string]interface{}
		if err := json.Unmarshal(message, &raw); err != nil {
			log.Printf("global market feed: json unmarshal error: %v", err)
			continue
		}
		now := time.Now().UTC()
		symbol := strings.ToUpper(strings.TrimSpace(asString(raw["name"])))
		if symbol == "" {
			symbol = strings.ToUpper(strings.TrimSpace(asString(raw["symbol"])))
		}
		meta := g.getMeta(symbol)
		bidPrice := firstNonZero(asFloat(raw["bidPrice"]), asFloat(raw["bid"]))
		askPrice := firstNonZero(asFloat(raw["askPrice"]), asFloat(raw["ask"]))
		bidQty := int64(firstNonZero(asFloat(raw["bidQty"]), asFloat(raw["buyQty"])))
		askQty := int64(firstNonZero(asFloat(raw["askQty"]), asFloat(raw["sellQty"])))
		tick := NormalizedTick{
			Exchange:         asString(raw["exchange"]),
			Symbol:           symbol,
			Expiry:           meta.Expiry,
			StrikePrice:      firstNonZero(asFloat(raw["strikePrice"]), asFloat(raw["strike"]), meta.Strike),
			LTP:              asFloat(raw["ltp"]),
			Open:             asFloat(raw["open"]),
			High:             asFloat(raw["high"]),
			Low:              asFloat(raw["low"]),
			Close:            asFloat(raw["close"]),
			BidPrice:         bidPrice,
			AskPrice:         askPrice,
			BidQty:           bidQty,
			AskQty:           askQty,
			BuyPrice:         bidPrice,
			BuyQty:           bidQty,
			SellPrice:        askPrice,
			SellQty:          askQty,
			TBQ:              int64(asFloat(raw["tbq"])),
			TSQ:              int64(asFloat(raw["tsq"])),
			OI:               int64(asFloat(raw["oi"])),
			LowerCircuit:     firstNonZero(asFloat(raw["lowerCkt"]), asFloat(raw["lower_ckt"]), asFloat(raw["lowerCircuit"])),
			UpperCircuit:     firstNonZero(asFloat(raw["upperCkt"]), asFloat(raw["upper_ckt"]), asFloat(raw["upperCircuit"])),
			LUT:              now,
			Timestamp:        msToTimeOrNow(raw["timestamp"], now),
			NetChange:        asFloat(raw["ch"]),
			NetChangePercent: asFloat(raw["chp"]),
			MarketStatus:     MarketStatusOpen,
		}
		if tick.Symbol == "" {
			log.Printf("global market feed: symbol empty, raw keys: %v", getMapKeys(raw))
			continue
		}
		updated := g.marketState.Update(tick)
		updated.MarketStatus = MarketStatusOpen
		g.lastInboundTickAt.Store(time.Now().UnixNano())
		if tickHub != nil {
			tickHub.Publish(updated)
		}
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

func firstNonZero(values ...float64) float64 {
	for _, v := range values {
		if v != 0 {
			return v
		}
	}
	return 0
}

func getMapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func (g *GlobalMarketFeedService) getMeta(symbol string) globalInstrumentMeta {
	g.metaMu.RLock()
	meta := g.metaBySym[symbol]
	g.metaMu.RUnlock()
	return meta
}

func (g *GlobalMarketFeedService) loadInstrumentMeta() {
	if g.db == nil {
		return
	}

	var instruments []models.GlobalInstrument
	if err := g.db.
		Select("symbol", "expiry", "strike", "status", "is_deleted").
		Where("status = ? AND is_deleted = ?", models.GlobalInstrumentStatusActive, false).
		Find(&instruments).Error; err != nil {
		log.Printf("global market feed: failed to load instrument metadata: %v", err)
		return
	}

	meta := make(map[string]globalInstrumentMeta, len(instruments))
	for _, inst := range instruments {
		symbol := strings.ToUpper(strings.TrimSpace(inst.Symbol))
		if symbol == "" {
			continue
		}
		meta[symbol] = globalInstrumentMeta{
			Expiry: inst.Expiry,
			Strike: inst.Strike,
		}
	}

	g.metaMu.Lock()
	g.metaBySym = meta
	g.metaMu.Unlock()
}

func msToTime(v interface{}) time.Time {
	ms := int64(asFloat(v))
	return time.Unix(0, ms*int64(time.Millisecond))
}

func msToTimeOrNow(v interface{}, fallback time.Time) time.Time {
	parsed := msToTime(v)
	if parsed.IsZero() || parsed.Unix() <= 0 {
		return fallback
	}
	return parsed
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
