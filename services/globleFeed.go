package services

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"time"

	"github.com/gorilla/websocket"
)

type MarketFeedProvider interface {
	Subscribe(symbols []string) error
	Unsubscribe(symbols []string) error
}

type GlobalMarketFeedService struct {
	wsURL  string
	apiKey string
	conn   *websocket.Conn
	ctx    context.Context
	cancel context.CancelFunc
}

func NewGlobalMarketFeedService() *GlobalMarketFeedService {
	wsURL := os.Getenv("GLOBAL_MARKET_WS_URL")
	apiKey := os.Getenv("GLOBAL_MARKET_X_API_KEY")
	return &GlobalMarketFeedService{
		wsURL:  wsURL,
		apiKey: apiKey,
	}
}

func (g *GlobalMarketFeedService) Start(ctx context.Context, tickHub *TickHub) {
	g.ctx, g.cancel = context.WithCancel(ctx)
	dialer := websocket.DefaultDialer
	headers := http.Header{}
	headers.Set("x-api-key", g.apiKey)
	conn, _, err := dialer.Dial(g.wsURL, headers)
	if err != nil {
		return
	}
	g.conn = conn

	// Subscribe to default forex symbols
	defaultSymbols := []string{"EURUSD", "USDCHF", "GBPUSD", "XAUUSD", "XAGUSD"}
	_ = g.Subscribe(defaultSymbols)

	go g.readLoop(tickHub)
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
		tickHub.Publish(tick)
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
	return g.conn.WriteJSON(msg)
}

func (g *GlobalMarketFeedService) Unsubscribe(symbols []string) error {
	// Implement unsubscribe logic if required by the API
	return nil
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
