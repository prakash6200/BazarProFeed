package services

import (
	"math"
	"sync"
	"time"
)

const (
	MarketStatusOpen   = "OPEN"
	MarketStatusClosed = "CLOSED"
)

// IsIndianMarketOpen returns true when the current time falls within NSE regular
// trading hours (Monday–Friday, 09:15–15:30 IST).
func IsIndianMarketOpen() bool {
	loc, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		return true // fallback: treat as open
	}
	now := time.Now().In(loc)
	if now.Weekday() == time.Saturday || now.Weekday() == time.Sunday {
		return false
	}
	open := time.Date(now.Year(), now.Month(), now.Day(), 9, 15, 0, 0, loc)
	close_ := time.Date(now.Year(), now.Month(), now.Day(), 15, 30, 0, 0, loc)
	return now.After(open) && now.Before(close_)
}

// IsGlobalMarketOpen returns true on weekdays (forex / global instruments trade 24×5).
func IsGlobalMarketOpen() bool {
	now := time.Now().UTC()
	wd := now.Weekday()
	return wd != time.Saturday && wd != time.Sunday
}

type NormalizedTick struct {
	Exchange         string       `json:"exchange"`
	Symbol           string       `json:"symbol"`
	Expiry           *time.Time   `json:"expiry,omitempty"`
	StrikePrice      float64      `json:"strikePrice"`
	LTP              float64      `json:"ltp"`
	LastTradedQty    int64        `json:"lastTradedQty"`
	AvgPrice         float64      `json:"avgPrice"`
	Volume           int64        `json:"volume"`
	Open             float64      `json:"open"`
	High             float64      `json:"high"`
	Low              float64      `json:"low"`
	Close            float64      `json:"close"`
	BidPrice         float64      `json:"bidPrice"`
	BidQty           int64        `json:"bidQty"`
	BuyPrice         float64      `json:"buyPrice"`
	BuyQty           int64        `json:"buyQty"`
	AskPrice         float64      `json:"askPrice"`
	AskQty           int64        `json:"askQty"`
	SellPrice        float64      `json:"sellPrice"`
	SellQty          int64        `json:"sellQty"`
	BidDepth         []DepthLevel `json:"bidDepth,omitempty"`
	AskDepth         []DepthLevel `json:"askDepth,omitempty"`
	TBQ              int64        `json:"tbq"`
	TSQ              int64        `json:"tsq"`
	OI               int64        `json:"oi"`
	OIDayHigh        int64        `json:"oiDayHigh"`
	OIDayLow         int64        `json:"oiDayLow"`
	LowerCircuit     float64      `json:"lowerCkt"`
	UpperCircuit     float64      `json:"upperCkt"`
	LastTradedTime   time.Time    `json:"lastTradedTime,omitempty"`
	ExchangeTime     time.Time    `json:"exchangeTime,omitempty"`
	LUT              time.Time    `json:"lut"`
	Timestamp        time.Time    `json:"timestamp"`
	NetChange        float64      `json:"netChange"`
	NetChangePercent float64      `json:"netChangePercent"`
	MarketStatus     string       `json:"market_status,omitempty"`
}

type DepthLevel struct {
	Quantity int64   `json:"quantity"`
	Price    float64 `json:"price"`
	Orders   int64   `json:"orders"`
}

type MarketStateManager struct {
	mu     sync.RWMutex
	latest map[string]NormalizedTick
}

func NewMarketStateManager() *MarketStateManager {
	return &MarketStateManager{latest: make(map[string]NormalizedTick)}
}

func (m *MarketStateManager) Update(tick NormalizedTick) NormalizedTick {
	tick = applyDerivedFields(tick)

	m.mu.Lock()
	m.latest[tick.Symbol] = tick
	m.mu.Unlock()

	return tick
}

func (m *MarketStateManager) UpdateWithPrevious(tick NormalizedTick) (NormalizedTick, NormalizedTick, bool) {
	tick = applyDerivedFields(tick)

	m.mu.Lock()
	previous, ok := m.latest[tick.Symbol]
	m.latest[tick.Symbol] = tick
	m.mu.Unlock()

	return previous, tick, ok
}

func (m *MarketStateManager) Get(symbol string) (NormalizedTick, bool) {
	m.mu.RLock()
	tick, ok := m.latest[symbol]
	m.mu.RUnlock()
	return tick, ok
}

func (m *MarketStateManager) Snapshot() []NormalizedTick {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ticks := make([]NormalizedTick, 0, len(m.latest))
	for _, tick := range m.latest {
		ticks = append(ticks, tick)
	}

	return ticks
}

func roundTo2(value float64) float64 {
	return math.Round(value*100) / 100
}

func applyDerivedFields(tick NormalizedTick) NormalizedTick {
	if tick.BuyPrice == 0 {
		tick.BuyPrice = tick.BidPrice
	}
	if tick.BuyQty == 0 {
		tick.BuyQty = tick.BidQty
	}
	if tick.SellPrice == 0 {
		tick.SellPrice = tick.AskPrice
	}
	if tick.SellQty == 0 {
		tick.SellQty = tick.AskQty
	}

	if tick.Close > 0 {
		tick.NetChange = tick.LTP - tick.Close
		tick.NetChangePercent = (tick.NetChange / tick.Close) * 100
	} else {
		tick.NetChange = 0
		tick.NetChangePercent = 0
	}

	tick.NetChange = roundTo2(tick.NetChange)
	tick.NetChangePercent = roundTo2(tick.NetChangePercent)
	return tick
}
