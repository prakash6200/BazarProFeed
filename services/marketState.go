package services

import (
	"math"
	"sync"
	"time"
)

type NormalizedTick struct {
	Exchange         string    `json:"exchange"`
	Symbol           string    `json:"symbol"`
	LTP              float64   `json:"ltp"`
	Open             float64   `json:"open"`
	High             float64   `json:"high"`
	Low              float64   `json:"low"`
	Close            float64   `json:"close"`
	BidPrice         float64   `json:"bidPrice"`
	BidQty           int64     `json:"bidQty"`
	AskPrice         float64   `json:"askPrice"`
	AskQty           int64     `json:"askQty"`
	TBQ              int64     `json:"tbq"`
	TSQ              int64     `json:"tsq"`
	OI               int64     `json:"oi"`
	Timestamp        time.Time `json:"timestamp"`
	NetChange        float64   `json:"netChange"`
	NetChangePercent float64   `json:"netChangePercent"`
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
