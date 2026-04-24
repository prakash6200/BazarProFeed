package services

// CandleBuilder subscribes to a TickHub and incrementally builds
// OHLC candle bars for every configured interval in memory.  Once
// per minute it flushes the current state to Redis so the candle API
// can serve requests without touching Postgres at all.
//
// Redis key layout
//   candles:{source}:{SYMBOL}:{intervalMinutes}:{bucketUnixSec}
//   e.g. candles:zerodha:NIFTY:5:1745481600
//
// Each key holds a JSON-encoded CandleBar.  TTL is 25 h so the full
// last-24 h window is always available.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"feedprovider/models"

	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

// Intervals that are pre-computed. Matches the options exposed by the API.
var candleIntervals = []int{1, 3, 5, 15, 30}

const (
	candleKeyPrefix = "candles"
	candleTTL       = 25 * time.Hour
	// How often the in-memory state is flushed to Redis.
	candleFlushInterval = 30 * time.Second
)

// CandleBar is the OHLC snapshot stored in Redis.
type CandleBar struct {
	Exchange      string    `json:"exchange"`
	Symbol        string    `json:"symbol"`
	IntervalStart time.Time `json:"interval_start"`
	IntervalMins  int       `json:"interval_mins"`
	Open          float64   `json:"open"`
	High          float64   `json:"high"`
	Low           float64   `json:"low"`
	Close         float64   `json:"close"`
	TickCount     int64     `json:"tick_count"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// candleState is the mutable in-memory accumulator for one bucket.
type candleState struct {
	exchange string
	symbol   string
	open     float64
	high     float64
	low      float64
	close_   float64
	count    int64
	dirty    bool // true when state has changed since last flush
}

// bucketKey uniquely identifies one candle bucket.
type bucketKey struct {
	symbol      string
	intervalMin int
	bucketUnix  int64 // UTC unix seconds of the bucket start
}

// CandleBuilder consumes ticks from a TickHub and maintains per-symbol
// OHLC candles for all configured intervals.
type CandleBuilder struct {
	source      string // "zerodha" or "global"
	redisClient *redis.Client

	mu     sync.Mutex
	states map[bucketKey]*candleState
}

// NewCandleBuilder creates a builder.  source is used as part of the
// Redis key (e.g. "zerodha" or "global").
func NewCandleBuilder(source string, redisClient *redis.Client) *CandleBuilder {
	return &CandleBuilder{
		source:      source,
		redisClient: redisClient,
		states:      make(map[bucketKey]*candleState),
	}
}

// WarmupFromDB fetches the last 24h of pre-computed candle bars from Postgres
// for every configured interval and writes them into Redis using the same key
// format that the live CandleBuilder uses.  Call this once at startup before
// Start() so the API always has a populated Redis cache even on a cold boot.
//
// Only symbols that already exist in Redis as an active tick (via the tick
// cache) are warmed up; everything else is hydrated lazily as ticks arrive.
// If db or redisClient is nil the call is a no-op.
// WarmupFromDB fetches 24h of candle bars from Postgres, writes them to Redis,
// and seeds the in-memory state map so live ticks continue on top of historical
// data without overwriting the open price.  Uses its own 2-minute timeout so it
// is safe to call outside runStartupStep.
func (cb *CandleBuilder) WarmupFromDB(db *gorm.DB) {
	if db == nil || cb.redisClient == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	total := 0
	const warmupPage = 1000
	for _, intervalMin := range candleIntervals {
		var written int
		var fetchErr error

		switch cb.source {
		case "zerodha":
			written, fetchErr = cb.warmupZerodha(ctx, db, intervalMin, warmupPage)
		case "global":
			written, fetchErr = cb.warmupGlobal(ctx, db, intervalMin, warmupPage)
		}

		if fetchErr != nil {
			log.Printf("candle builder [%s]: warmup error interval=%dm: %v", cb.source, intervalMin, fetchErr)
			continue
		}
		total += written
	}

	if total > 0 {
		log.Printf("candle builder [%s]: warmup complete — %d candle buckets loaded into Redis + memory", cb.source, total)
	} else {
		log.Printf("candle builder [%s]: warmup complete — no historical data found (empty tables or first run)", cb.source)
	}
}

func (cb *CandleBuilder) warmupZerodha(ctx context.Context, db *gorm.DB, intervalMin, pageSize int) (int, error) {
	written := 0
	for page := 1; ; page++ {
		rows, _, err := models.ListZerodhaCandlesLast24h(db, intervalMin, page, pageSize, models.ZerodhaTickQueryFilters{})
		if err != nil {
			return written, err
		}
		if len(rows) == 0 {
			break
		}
		bars := make([]warmupBar, len(rows))
		for i, r := range rows {
			bars[i] = warmupBar{
				exchange:      r.Exchange,
				symbol:        r.Symbol,
				intervalStart: r.IntervalStart,
				open:          r.Open,
				high:          r.High,
				low:           r.Low,
				close_:        r.Close,
				count:         r.TickCount,
			}
		}
		cb.writeWarmupBars(ctx, intervalMin, bars)
		written += len(rows)
		if len(rows) < pageSize {
			break // last page
		}
	}
	return written, nil
}

func (cb *CandleBuilder) warmupGlobal(ctx context.Context, db *gorm.DB, intervalMin, pageSize int) (int, error) {
	written := 0
	for page := 1; ; page++ {
		rows, _, err := models.ListGlobalCandlesLast24h(db, intervalMin, page, pageSize, models.GlobalTickQueryFilters{})
		if err != nil {
			return written, err
		}
		if len(rows) == 0 {
			break
		}
		bars := make([]warmupBar, len(rows))
		for i, r := range rows {
			bars[i] = warmupBar{
				exchange:      r.Exchange,
				symbol:        r.Symbol,
				intervalStart: r.IntervalStart,
				open:          r.Open,
				high:          r.High,
				low:           r.Low,
				close_:        r.Close,
				count:         r.TickCount,
			}
		}
		cb.writeWarmupBars(ctx, intervalMin, bars)
		written += len(rows)
		if len(rows) < pageSize {
			break // last page
		}
	}
	return written, nil
}

type warmupBar struct {
	exchange      string
	symbol        string
	intervalStart time.Time
	open          float64
	high          float64
	low           float64
	close_        float64
	count         int64
}

func (cb *CandleBuilder) writeWarmupBars(ctx context.Context, intervalMin int, bars []warmupBar) {
	if len(bars) == 0 {
		return
	}

	// Use an independent timeout — ctx may be near expiry when called from a
	// tightly-bounded startup step.
	writeCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Seed in-memory state so live ticks correctly continue building on top of
	// historical data (preserving the historical open price, not replacing it).
	cb.mu.Lock()
	for _, b := range bars {
		k := bucketKey{
			symbol:      b.symbol,
			intervalMin: intervalMin,
			bucketUnix:  b.intervalStart.UTC().Unix(),
		}
		// Only seed if a live tick hasn't already created a fresher state.
		if _, exists := cb.states[k]; !exists {
			cb.states[k] = &candleState{
				exchange: b.exchange,
				symbol:   b.symbol,
				open:     b.open,
				high:     b.high,
				low:      b.low,
				close_:   b.close_,
				count:    b.count,
				dirty:    false,
			}
		}
	}
	cb.mu.Unlock()

	pipe := cb.redisClient.Pipeline()
	for _, b := range bars {
		bar := CandleBar{
			Exchange:      b.exchange,
			Symbol:        b.symbol,
			IntervalStart: b.intervalStart.UTC(),
			IntervalMins:  intervalMin,
			Open:          b.open,
			High:          b.high,
			Low:           b.low,
			Close:         b.close_,
			TickCount:     b.count,
			UpdatedAt:     time.Now().UTC(),
		}
		payload, err := json.Marshal(bar)
		if err != nil {
			continue
		}
		redisKey := fmt.Sprintf("%s:%s:%s:%d:%d",
			candleKeyPrefix, cb.source, b.symbol, intervalMin, b.intervalStart.UTC().Unix())
		pipe.Set(writeCtx, redisKey, payload, candleTTL)
	}

	if _, err := pipe.Exec(writeCtx); err != nil && writeCtx.Err() == nil {
		log.Printf("candle builder [%s]: warmup redis pipeline error: %v", cb.source, err)
	}
}

// Start subscribes to hub and runs the flush loop until ctx is cancelled.
func (cb *CandleBuilder) Start(ctx context.Context, hub *TickHub) {
	if cb.redisClient == nil {
		log.Printf("candle builder [%s]: no redis client, skipping", cb.source)
		return
	}

	ticks, unsub := hub.Subscribe(4096)
	go func() {
		defer unsub()
		cb.consumeLoop(ctx, ticks)
	}()

	go cb.flushLoop(ctx)

	log.Printf("candle builder [%s]: started (%d intervals)", cb.source, len(candleIntervals))
}

// consumeLoop processes incoming ticks and updates in-memory candle state.
func (cb *CandleBuilder) consumeLoop(ctx context.Context, ticks <-chan NormalizedTick) {
	for {
		select {
		case <-ctx.Done():
			return
		case tick, ok := <-ticks:
			if !ok {
				return
			}
			if tick.LTP <= 0 || tick.Symbol == "" {
				continue
			}
			cb.applyTick(tick)
		}
	}
}

// applyTick updates all interval buckets for the given tick.
func (cb *CandleBuilder) applyTick(tick NormalizedTick) {
	ts := tick.Timestamp
	if ts.IsZero() {
		ts = time.Now().UTC()
	} else {
		ts = ts.UTC()
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	for _, intervalMin := range candleIntervals {
		bucketStart := bucketStartTime(ts, intervalMin)
		key := bucketKey{
			symbol:      tick.Symbol,
			intervalMin: intervalMin,
			bucketUnix:  bucketStart.Unix(),
		}

		st, exists := cb.states[key]
		if !exists {
			st = &candleState{
				exchange: tick.Exchange,
				symbol:   tick.Symbol,
				open:     tick.LTP,
				high:     tick.LTP,
				low:      tick.LTP,
				close_:   tick.LTP,
				count:    1,
				dirty:    true,
			}
			cb.states[key] = st
			continue
		}

		if tick.LTP > st.high {
			st.high = tick.LTP
		}
		if tick.LTP < st.low {
			st.low = tick.LTP
		}
		st.close_ = tick.LTP
		st.count++
		st.dirty = true
	}
}

// flushLoop periodically writes dirty candle states to Redis.
func (cb *CandleBuilder) flushLoop(ctx context.Context) {
	ticker := time.NewTicker(candleFlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Final flush on shutdown so in-flight data is not lost.
			cb.flush(context.Background())
			return
		case <-ticker.C:
			cb.flush(ctx)
		}
	}
}

// flush writes all dirty states to Redis and cleans up expired buckets.
func (cb *CandleBuilder) flush(ctx context.Context) {
	now := time.Now().UTC()
	cutoff := now.Add(-25 * time.Hour).Unix()

	cb.mu.Lock()
	// Snapshot dirty states and drop stale buckets in one pass.
	toFlush := make(map[bucketKey]*candleState)
	for k, st := range cb.states {
		if k.bucketUnix < cutoff {
			delete(cb.states, k)
			continue
		}
		if st.dirty {
			// Clone to avoid holding the lock during Redis IO.
			clone := *st
			toFlush[k] = &clone
			st.dirty = false
		}
	}
	cb.mu.Unlock()

	if len(toFlush) == 0 {
		return
	}

	writeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	pipe := cb.redisClient.Pipeline()
	for k, st := range toFlush {
		bar := CandleBar{
			Exchange:      st.exchange,
			Symbol:        st.symbol,
			IntervalStart: time.Unix(k.bucketUnix, 0).UTC(),
			IntervalMins:  k.intervalMin,
			Open:          st.open,
			High:          st.high,
			Low:           st.low,
			Close:         st.close_,
			TickCount:     st.count,
			UpdatedAt:     time.Now().UTC(),
		}
		payload, err := json.Marshal(bar)
		if err != nil {
			log.Printf("candle builder [%s]: marshal error: %v", cb.source, err)
			continue
		}
		redisKey := fmt.Sprintf("%s:%s:%s:%d:%d",
			candleKeyPrefix, cb.source, k.symbol, k.intervalMin, k.bucketUnix)
		pipe.Set(writeCtx, redisKey, payload, candleTTL)
	}

	if _, err := pipe.Exec(writeCtx); err != nil && writeCtx.Err() == nil {
		log.Printf("candle builder [%s]: redis pipeline error: %v", cb.source, err)
	}
}

// bucketStartTime returns the UTC start of the candle bucket that contains ts.
func bucketStartTime(ts time.Time, intervalMinutes int) time.Time {
	ts = ts.UTC()
	minuteOfDay := ts.Hour()*60 + ts.Minute()
	bucketMinute := (minuteOfDay / intervalMinutes) * intervalMinutes
	return time.Date(ts.Year(), ts.Month(), ts.Day(), bucketMinute/60, bucketMinute%60, 0, 0, time.UTC)
}

// GetCandlesFromRedis fetches pre-built candle bars for a symbol+interval
// from Redis.  start/end are inclusive bucket boundaries (UTC).
// This replaces the heavy Postgres GROUP BY query on the hot API path.
func GetCandlesFromRedis(
	ctx context.Context,
	redisClient *redis.Client,
	source, symbol string,
	intervalMinutes int,
	start, end time.Time,
) ([]CandleBar, error) {
	if redisClient == nil {
		return nil, nil
	}

	// Enumerate all bucket starts in the requested window.
	var keys []string
	for t := bucketStartTime(start, intervalMinutes); !t.After(end); t = t.Add(time.Duration(intervalMinutes) * time.Minute) {
		key := fmt.Sprintf("%s:%s:%s:%d:%d",
			candleKeyPrefix, source, symbol, intervalMinutes, t.Unix())
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		return nil, nil
	}

	vals, err := redisClient.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}

	bars := make([]CandleBar, 0, len(vals))
	for _, v := range vals {
		if v == nil {
			continue
		}
		raw, ok := v.(string)
		if !ok || raw == "" {
			continue
		}
		var bar CandleBar
		if err := json.Unmarshal([]byte(raw), &bar); err != nil {
			continue
		}
		bars = append(bars, bar)
	}
	return bars, nil
}
