package services

import (
	"context"
	"encoding/json"
	"feedprovider/models"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
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
	RefreshFromDatabase(ctx context.Context) error
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
	subsMu     sync.RWMutex
	subscribed map[string]struct{}

	marketState       *MarketStateManager
	lastInboundTickAt atomic.Int64
	recentMu          sync.RWMutex
	recentBySym       map[string][]NormalizedTick
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
	globalRecentTickHistory      = 5
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
		subscribed:  make(map[string]struct{}),
		recentBySym: make(map[string][]NormalizedTick),
	}
	if redisCache != nil {
		ticks := redisCache.Snapshot(context.Background(), GlobalTickPrefix, "")
		for _, tick := range ticks {
			svc.marketState.Update(tick)
			svc.recordRecentTick(tick)
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
	g.syncActiveSymbolsToRedis(ctx)

	symbols := g.subscriptionSymbols()

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
	backoff := 1 * time.Second
	maxBackoff := 30 * time.Second

	for {
		if g.ctx.Err() != nil {
			log.Printf("global market feed: readLoop context cancelled, exiting")
			return
		}

		_, message, err := g.conn.ReadMessage()
		if err != nil {
			log.Printf("global market feed: readLoop error: %v, reconnecting in %v...", err, backoff)

			// Try to reconnect
			select {
			case <-time.After(backoff):
				if reconnectErr := g.reconnect(); reconnectErr != nil {
					log.Printf("global market feed: reconnect failed: %v", reconnectErr)
					backoff = time.Duration(float64(backoff) * 1.5)
					if backoff > maxBackoff {
						backoff = maxBackoff
					}
					continue
				}
				log.Printf("global market feed: reconnected successfully, resetting backoff")
				backoff = 1 * time.Second
			case <-g.ctx.Done():
				log.Printf("global market feed: readLoop context cancelled during reconnect")
				return
			}
			continue
		}

		var raw map[string]interface{}
		if err := json.Unmarshal(message, &raw); err != nil {
			log.Printf("global market feed: json unmarshal error: %v, raw message: %s", err, string(message)[:200])
			continue
		}
		now := time.Now().UTC()
		symbol := strings.ToUpper(strings.TrimSpace(asString(raw["name"])))
		if symbol == "" {
			symbol = strings.ToUpper(strings.TrimSpace(asString(raw["symbol"])))
		}
		meta := g.getMeta(symbol)
		bidDepth := parseDepthLevels(firstNonNil(raw["bidDepth"], raw["bids"], raw["buyDepth"]))
		askDepth := parseDepthLevels(firstNonNil(raw["askDepth"], raw["asks"], raw["sellDepth"]))

		bidPrice := firstNonZero(asFloat(raw["bidPrice"]), asFloat(raw["bid"]))
		askPrice := firstNonZero(asFloat(raw["askPrice"]), asFloat(raw["ask"]))
		bidQty := int64(firstNonZero(asFloat(raw["bidQty"]), asFloat(raw["buyQty"])))
		askQty := int64(firstNonZero(asFloat(raw["askQty"]), asFloat(raw["sellQty"])))
		if len(bidDepth) > 0 {
			if bidPrice == 0 {
				bidPrice = bidDepth[0].Price
			}
			if bidQty == 0 {
				bidQty = bidDepth[0].Quantity
			}
		}
		if len(askDepth) > 0 {
			if askPrice == 0 {
				askPrice = askDepth[0].Price
			}
			if askQty == 0 {
				askQty = askDepth[0].Quantity
			}
		}

		if len(bidDepth) == 0 || len(askDepth) == 0 {
			recent := g.recentForSymbol(symbol)
			if len(bidDepth) == 0 {
				bidDepth = buildSyntheticDepth(recent, true, bidPrice, bidQty)
			}
			if len(askDepth) == 0 {
				askDepth = buildSyntheticDepth(recent, false, askPrice, askQty)
			}
		}

		lastTradedTime := parseTimeFields(raw["lastTradedTime"], raw["ltt"], raw["last_trade_time"], raw["lastTradedTimestamp"])
		exchangeTime := parseTimeFields(raw["exchangeTime"], raw["exchange_timestamp"], raw["exchangeTs"])
		tick := NormalizedTick{
			Exchange:         asString(raw["exchange"]),
			Symbol:           symbol,
			Expiry:           meta.Expiry,
			StrikePrice:      firstNonZero(asFloat(raw["strikePrice"]), asFloat(raw["strike"]), meta.Strike),
			LTP:              asFloat(raw["ltp"]),
			LastTradedQty:    int64(firstNonZero(asFloat(raw["lastTradedQty"]), asFloat(raw["ltq"]))),
			AvgPrice:         firstNonZero(asFloat(raw["avgPrice"]), asFloat(raw["averagePrice"]), asFloat(raw["avg_price"])),
			Volume:           int64(asFloat(raw["volume"])),
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
			BidDepth:         bidDepth,
			AskDepth:         askDepth,
			TBQ:              int64(asFloat(raw["tbq"])),
			TSQ:              int64(asFloat(raw["tsq"])),
			OI:               int64(asFloat(raw["oi"])),
			OIDayHigh:        int64(firstNonZero(asFloat(raw["oiDayHigh"]), asFloat(raw["oi_high"]))),
			OIDayLow:         int64(firstNonZero(asFloat(raw["oiDayLow"]), asFloat(raw["oi_low"]))),
			LowerCircuit:     firstNonZero(asFloat(raw["lowerCkt"]), asFloat(raw["lower_ckt"]), asFloat(raw["lowerCircuit"])),
			UpperCircuit:     firstNonZero(asFloat(raw["upperCkt"]), asFloat(raw["upper_ckt"]), asFloat(raw["upperCircuit"])),
			LastTradedTime:   valueOrNow(lastTradedTime, now),
			ExchangeTime:     valueOrNow(exchangeTime, now),
			LUT:              now,
			Timestamp:        msToTimeOrNow(raw["timestamp"], now),
			NetChange:        asFloat(raw["ch"]),
			NetChangePercent: asFloat(raw["chp"]),
			MarketStatus:     MarketStatusOpen,
		}
		if tick.Symbol == "" {
			// Ignore non-tick control frames (subscribe ack, heartbeat) that don't carry symbol fields.
			if _, hasEvent := raw["event"]; !hasEvent {
				log.Printf("global market feed: symbol empty, raw keys: %v", getMapKeys(raw))
			}
			continue
		}
		updated := g.marketState.Update(tick)
		updated.MarketStatus = MarketStatusOpen
		g.lastInboundTickAt.Store(time.Now().UnixNano())
		if tickHub != nil {
			tickHub.Publish(updated)
		}
		g.recordRecentTick(updated)
		if g.redisCache != nil && g.ctx != nil && g.ctx.Err() == nil {
			g.redisCache.Set(g.ctx, GlobalTickPrefix, updated)
		}
	}
}

func (g *GlobalMarketFeedService) recordRecentTick(tick NormalizedTick) {
	if strings.TrimSpace(tick.Symbol) == "" {
		return
	}
	symbol := strings.ToUpper(strings.TrimSpace(tick.Symbol))

	g.recentMu.Lock()
	history := append(g.recentBySym[symbol], tick)
	if len(history) > globalRecentTickHistory {
		history = history[len(history)-globalRecentTickHistory:]
	}
	g.recentBySym[symbol] = history
	g.recentMu.Unlock()
}

func (g *GlobalMarketFeedService) recentForSymbol(symbol string) []NormalizedTick {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	if symbol == "" {
		return nil
	}

	g.recentMu.RLock()
	history := g.recentBySym[symbol]
	g.recentMu.RUnlock()

	if len(history) == 0 {
		return nil
	}

	result := make([]NormalizedTick, len(history))
	copy(result, history)
	return result
}

func (g *GlobalMarketFeedService) RecentTicks(filterSym string) []NormalizedTick {
	g.recentMu.RLock()
	defer g.recentMu.RUnlock()

	if strings.TrimSpace(filterSym) != "" {
		symbol := strings.ToUpper(strings.TrimSpace(filterSym))
		history := g.recentBySym[symbol]
		result := make([]NormalizedTick, len(history))
		copy(result, history)
		return result
	}

	result := make([]NormalizedTick, 0, len(g.recentBySym)*globalRecentTickHistory)
	for _, history := range g.recentBySym {
		result = append(result, history...)
	}
	return result
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

func (g *GlobalMarketFeedService) reconnect() error {
	if g.wsURL == "" {
		return fmt.Errorf("ws url not configured")
	}

	// Close old connection
	if g.conn != nil {
		_ = g.conn.Close()
	}

	// Establish new connection
	dialer := websocket.DefaultDialer
	dialer.HandshakeTimeout = 10 * time.Second
	headers := http.Header{}
	headers.Set("x-api-key", g.apiKey)

	dialCtx, dialCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer dialCancel()

	conn, _, err := dialer.DialContext(dialCtx, g.wsURL, headers)
	if err != nil {
		return fmt.Errorf("dial failed: %w", err)
	}
	g.conn = conn
	g.clearSubscribed()
	g.syncActiveSymbolsToRedis(context.Background())

	// Re-load metadata and rebuild symbol list.
	symbols := g.subscriptionSymbols()

	if len(symbols) > 0 {
		if err := g.Subscribe(symbols); err != nil {
			return fmt.Errorf("subscribe failed: %w", err)
		}
		log.Printf("global market feed: reconnected and re-subscribed to %d symbols", len(symbols))
	}

	return nil
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
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(t), 64)
		if err == nil {
			return f
		}
	}
	return 0
}

func firstNonNil(values ...interface{}) interface{} {
	for _, v := range values {
		if v != nil {
			return v
		}
	}
	return nil
}

func parseDepthLevels(v interface{}) []DepthLevel {
	arr, ok := v.([]interface{})
	if !ok || len(arr) == 0 {
		return nil
	}
	depth := make([]DepthLevel, 0, len(arr))
	for _, item := range arr {
		entry, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		level := DepthLevel{
			Quantity: int64(firstNonZero(asFloat(entry["quantity"]), asFloat(entry["qty"]))),
			Price:    firstNonZero(asFloat(entry["price"]), asFloat(entry["rate"])),
			Orders:   int64(firstNonZero(asFloat(entry["orders"]), asFloat(entry["orderCount"]))),
		}
		if level.Quantity == 0 && level.Price == 0 && level.Orders == 0 {
			continue
		}
		depth = append(depth, level)
	}
	if len(depth) == 0 {
		return nil
	}
	if len(depth) > 5 {
		depth = depth[:5]
	}
	return depth
}

func buildSyntheticDepth(history []NormalizedTick, isBid bool, currentPrice float64, currentQty int64) []DepthLevel {
	depth := make([]DepthLevel, 0, globalRecentTickHistory)
	seen := make(map[float64]struct{}, globalRecentTickHistory)

	if currentPrice > 0 {
		orders := int64(0)
		if currentQty > 0 {
			orders = 1
		}
		depth = append(depth, DepthLevel{Price: currentPrice, Quantity: currentQty, Orders: orders})
		seen[currentPrice] = struct{}{}
	}

	for i := len(history) - 1; i >= 0 && len(depth) < globalRecentTickHistory; i-- {
		t := history[i]
		price := t.AskPrice
		qty := t.AskQty
		if isBid {
			price = t.BidPrice
			qty = t.BidQty
		}
		if price <= 0 {
			continue
		}
		if _, ok := seen[price]; ok {
			continue
		}
		orders := int64(0)
		if qty > 0 {
			orders = 1
		}
		depth = append(depth, DepthLevel{Price: price, Quantity: qty, Orders: orders})
		seen[price] = struct{}{}
	}

	if len(depth) == 0 {
		return nil
	}
	return depth
}

func parseTimeFields(values ...interface{}) time.Time {
	for _, value := range values {
		switch t := value.(type) {
		case string:
			raw := strings.TrimSpace(t)
			if raw == "" {
				continue
			}
			if parsed, err := time.Parse(time.RFC3339Nano, raw); err == nil {
				return parsed.UTC()
			}
			if parsed, err := time.Parse(time.RFC3339, raw); err == nil {
				return parsed.UTC()
			}
		case float64:
			if t > 0 {
				return msToTimeOrNow(t, time.Now().UTC())
			}
		case int64:
			if t > 0 {
				return msToTimeOrNow(t, time.Now().UTC())
			}
		}
	}
	return time.Time{}
}

func valueOrNow(t time.Time, fallback time.Time) time.Time {
	if t.IsZero() {
		return fallback
	}
	return t
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

func (g *GlobalMarketFeedService) subscriptionSymbols() []string {
	merged := make([]string, 0)

	if g.db != nil {
		g.loadInstrumentMeta()
		dbSymbols, err := models.GetActiveGlobalInstrumentSymbols(g.db)
		if err != nil {
			log.Printf("global market feed: failed to load active symbols: %v", err)
		} else {
			merged = append(merged, dbSymbols...)
		}
	}

	symbols := uniqueSymbols(merged)
	if len(symbols) > 0 {
		log.Printf("global market feed: prepared %d subscription symbols", len(symbols))
	} else {
		log.Printf("global market feed: no active DB symbols available")
	}
	return symbols
}

func (g *GlobalMarketFeedService) syncActiveSymbolsToRedis(ctx context.Context) {
	if g.db == nil || g.redisCache == nil {
		return
	}
	active, err := models.GetActiveGlobalInstrumentSymbols(g.db)
	if err != nil {
		log.Printf("global market feed: failed to sync active symbols to redis: %v", err)
		return
	}
	norm := uniqueSymbols(active)
	g.redisCache.SetSymbolList(ctx, GlobalSymbolListKey, norm)
	log.Printf("global market feed: synced %d active symbols to redis", len(norm))
}

func (g *GlobalMarketFeedService) snapshotSubscribed() []string {
	g.subsMu.RLock()
	defer g.subsMu.RUnlock()
	result := make([]string, 0, len(g.subscribed))
	for s := range g.subscribed {
		result = append(result, s)
	}
	return result
}

func (g *GlobalMarketFeedService) clearSubscribed() {
	g.subsMu.Lock()
	g.subscribed = make(map[string]struct{})
	g.subsMu.Unlock()
}

func (g *GlobalMarketFeedService) RefreshFromDatabase(ctx context.Context) error {
	if g.db == nil {
		return nil
	}
	g.loadInstrumentMeta()

	active, err := models.GetActiveGlobalInstrumentSymbols(g.db)
	if err != nil {
		return fmt.Errorf("load active symbols failed: %w", err)
	}
	desired := uniqueSymbols(active)

	if g.redisCache != nil {
		g.redisCache.SetSymbolList(ctx, GlobalSymbolListKey, desired)
	}

	current := uniqueSymbols(g.snapshotSubscribed())
	curSet := make(map[string]struct{}, len(current))
	for _, s := range current {
		curSet[s] = struct{}{}
	}
	newSet := make(map[string]struct{}, len(desired))
	for _, s := range desired {
		newSet[s] = struct{}{}
	}

	toUnsub := make([]string, 0)
	for s := range curSet {
		if _, ok := newSet[s]; !ok {
			toUnsub = append(toUnsub, s)
		}
	}

	toSub := make([]string, 0)
	for s := range newSet {
		if _, ok := curSet[s]; !ok {
			toSub = append(toSub, s)
		}
	}

	if len(toUnsub) > 0 {
		if err := g.Unsubscribe(toUnsub); err != nil {
			return fmt.Errorf("unsubscribe failed: %w", err)
		}
	}
	if len(toSub) > 0 {
		if err := g.Subscribe(toSub); err != nil {
			return fmt.Errorf("subscribe failed: %w", err)
		}
	}

	log.Printf("global market feed: refreshed from DB (active=%d, subscribed=%d)", len(desired), len(newSet))
	return nil
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
	if g.db != nil {
		g.loadInstrumentMeta()
	}
	symbols = uniqueSymbols(symbols)
	if len(symbols) == 0 {
		return nil
	}
	msg := map[string]interface{}{
		"event":   "subscribe",
		"symbols": symbols,
	}
	g.writeMu.Lock()
	defer g.writeMu.Unlock()
	if err := g.conn.WriteJSON(msg); err != nil {
		return err
	}
	g.subsMu.Lock()
	for _, s := range symbols {
		g.subscribed[s] = struct{}{}
	}
	g.subsMu.Unlock()
	return nil
}

func (g *GlobalMarketFeedService) Unsubscribe(symbols []string) error {
	if g.conn == nil {
		return nil
	}
	symbols = uniqueSymbols(symbols)
	if len(symbols) == 0 {
		return nil
	}
	msg := map[string]interface{}{
		"event":   "unsubscribe",
		"symbols": symbols,
	}
	g.writeMu.Lock()
	defer g.writeMu.Unlock()
	if err := g.conn.WriteJSON(msg); err != nil {
		return err
	}
	g.subsMu.Lock()
	for _, s := range symbols {
		delete(g.subscribed, s)
	}
	g.subsMu.Unlock()
	return nil
}

func (g *GlobalMarketFeedService) Stop() {
	if g.cancel != nil {
		g.cancel()
	}
	if g.conn != nil {
		g.conn.Close()
	}
}
