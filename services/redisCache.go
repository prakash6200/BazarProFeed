package services

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const tickCacheTTL = 24 * time.Hour

const (
	ZerodhaTickPrefix    = "feed:zerodha:tick:"
	ZerodhaCircuitPrefix = "feed:zerodha:circuit:"
	GlobalTickPrefix     = "feed:global:tick:"
	GlobalSymbolListKey  = "feed:global:symbols"

	// Async cache writer — size of the per-cache write channel. The hot
	// tick goroutine pushes marshalled payloads here without blocking;
	// the writer goroutine drains them to Redis. On overflow newer ticks
	// replace older pending writes for the same symbol (latest-wins),
	// which is exactly what a per-symbol cache needs.
	cacheWriteQueueSize = 8192

	// Upper bound on time a single Redis SET is allowed to take before
	// the writer gives up and drops it. Kept tight so a Redis stall
	// cannot back-pressure the tick pipeline.
	cacheWriteTimeout = 1500 * time.Millisecond
)

type cacheWriteJob struct {
	key     string
	payload []byte
}

// RedisTickCache caches NormalizedTick values in Redis per symbol.
// All methods are safe to call with a nil client (no-ops).
//
// Writes on the hot tick path go through a bounded, non-blocking
// channel drained by a background writer goroutine — the caller never
// waits on Redis.
type RedisTickCache struct {
	client *redis.Client

	writerOnce sync.Once
	writeCh    chan cacheWriteJob
	// pending is a latest-wins map: multiple updates to the same symbol
	// while the writer is slow collapse into a single write.
	pendingMu sync.Mutex
	pending   map[string][]byte
}

func NewRedisTickCache(client *redis.Client) *RedisTickCache {
	c := &RedisTickCache{
		client:  client,
		writeCh: make(chan cacheWriteJob, cacheWriteQueueSize),
		pending: make(map[string][]byte),
	}
	return c
}

// StartWriter launches the background goroutine that drains pending
// writes to Redis. Safe to call multiple times; only the first call
// starts the goroutine. It is a no-op when the Redis client is nil.
func (c *RedisTickCache) StartWriter(ctx context.Context) {
	if c == nil || c.client == nil {
		return
	}
	c.writerOnce.Do(func() {
		go c.runWriter(ctx)
	})
}

func (c *RedisTickCache) runWriter(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-c.writeCh:
			if !ok {
				return
			}

			// Collapse any newer pending write for this key.
			c.pendingMu.Lock()
			if latest, found := c.pending[job.key]; found {
				job.payload = latest
				delete(c.pending, job.key)
			}
			c.pendingMu.Unlock()

			writeCtx, cancel := context.WithTimeout(context.Background(), cacheWriteTimeout)
			if err := c.client.Set(writeCtx, job.key, job.payload, tickCacheTTL).Err(); err != nil {
				// Demote to debug-level noise — Redis outages are common.
				log.Printf("redis async cache set error: key=%s err=%v", job.key, err)
			}
			cancel()
		}
	}
}

// enqueue schedules an async write. Never blocks the caller.
func (c *RedisTickCache) enqueue(key string, payload []byte) {
	if c == nil || c.client == nil || key == "" || len(payload) == 0 {
		return
	}

	c.pendingMu.Lock()
	c.pending[key] = payload
	c.pendingMu.Unlock()

	select {
	case c.writeCh <- cacheWriteJob{key: key, payload: payload}:
	default:
		// Writer can't keep up. The pending map still holds the
		// freshest payload for this key; drop this wake-up and the
		// next Enqueue for any key will pick up the latest value.
	}
}

// SetAsync enqueues a tick cache update without blocking the caller.
// Safe to call from the hot feed goroutine. Returns immediately.
func (c *RedisTickCache) SetAsync(tick NormalizedTick) {
	if c == nil || c.client == nil || tick.Symbol == "" {
		return
	}
	data, err := json.Marshal(tick)
	if err != nil {
		return
	}
	// Default prefix picker: callers in this codebase set a dedicated
	// cache instance per prefix (zerodhaCache / globalCache) but share
	// the same Redis client, so the prefix is passed explicitly via
	// SetAsyncWithPrefix. Retained for API compatibility.
	c.enqueue(strings.ToUpper(tick.Symbol), data)
}

// SetAsyncWithPrefix is the preferred non-blocking hot-path API.
func (c *RedisTickCache) SetAsyncWithPrefix(prefix string, tick NormalizedTick) {
	if c == nil || c.client == nil || tick.Symbol == "" {
		return
	}
	data, err := json.Marshal(tick)
	if err != nil {
		return
	}
	c.enqueue(prefix+strings.ToUpper(tick.Symbol), data)
}

func (c *RedisTickCache) Client() *redis.Client {
	if c == nil {
		return nil
	}
	return c.client
}

// Set stores a tick in Redis. The key is prefix+SYMBOL, TTL is 24 h.
func (c *RedisTickCache) Set(ctx context.Context, prefix string, tick NormalizedTick) {
	if c == nil || c.client == nil || tick.Symbol == "" {
		return
	}
	key := prefix + strings.ToUpper(tick.Symbol)
	data, err := json.Marshal(tick)
	if err != nil {
		return
	}
	if err := c.client.Set(ctx, key, data, tickCacheTTL).Err(); err != nil {
		log.Printf("redis cache set error: key=%s err=%v", key, err)
	}
}

// Snapshot returns all ticks stored under the given prefix.
// If filterSym is non-empty, only the single matching tick is returned.
func (c *RedisTickCache) Snapshot(ctx context.Context, prefix, filterSym string) []NormalizedTick {
	if c == nil || c.client == nil {
		return nil
	}

	var keys []string
	if filterSym != "" {
		key := prefix + strings.ToUpper(filterSym)
		// Check existence to avoid a MGet nil
		exists, err := c.client.Exists(ctx, key).Result()
		if err != nil || exists == 0 {
			return nil
		}
		keys = []string{key}
	} else {
		var err error
		pattern := prefix + "*"
		iter := c.client.Scan(ctx, 0, pattern, 100).Iterator()
		for iter.Next(ctx) {
			keys = append(keys, iter.Val())
		}
		if err = iter.Err(); err != nil || len(keys) == 0 {
			return nil
		}
	}

	vals, err := c.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil
	}

	ticks := make([]NormalizedTick, 0, len(vals))
	for _, v := range vals {
		if v == nil {
			continue
		}
		var tick NormalizedTick
		if err := json.Unmarshal([]byte(v.(string)), &tick); err != nil {
			continue
		}
		ticks = append(ticks, tick)
	}
	return ticks
}

// SetSymbolList stores normalized symbols as a JSON array under key.
func (c *RedisTickCache) SetSymbolList(ctx context.Context, key string, symbols []string) {
	if c == nil || c.client == nil || strings.TrimSpace(key) == "" {
		return
	}
	norm := make([]string, 0, len(symbols))
	seen := make(map[string]struct{}, len(symbols))
	for _, s := range symbols {
		v := strings.ToUpper(strings.TrimSpace(s))
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		norm = append(norm, v)
	}
	if len(norm) == 0 {
		if err := c.client.Del(ctx, key).Err(); err != nil {
			log.Printf("redis symbol list delete error: key=%s err=%v", key, err)
		}
		return
	}
	data, err := json.Marshal(norm)
	if err != nil {
		return
	}
	if err := c.client.Set(ctx, key, data, tickCacheTTL).Err(); err != nil {
		log.Printf("redis symbol list set error: key=%s err=%v", key, err)
	}
}

// GetSymbolList returns symbols stored by SetSymbolList.
func (c *RedisTickCache) GetSymbolList(ctx context.Context, key string) []string {
	if c == nil || c.client == nil || strings.TrimSpace(key) == "" {
		return nil
	}
	value, err := c.client.Get(ctx, key).Result()
	if err != nil {
		return nil
	}
	var symbols []string
	if err := json.Unmarshal([]byte(value), &symbols); err != nil {
		return nil
	}
	return symbols
}

// SetZerodhaCircuit stores upper/lower circuit by instrument token.
func (c *RedisTickCache) SetZerodhaCircuit(ctx context.Context, token int64, limit zerodhaCircuitLimit) {
	if c == nil || c.client == nil || token <= 0 {
		return
	}
	key := ZerodhaCircuitPrefix + strconv.FormatInt(token, 10)
	data, err := json.Marshal(limit)
	if err != nil {
		return
	}
	if err := c.client.Set(ctx, key, data, tickCacheTTL).Err(); err != nil {
		log.Printf("redis circuit set error: key=%s err=%v", key, err)
	}
}

// GetZerodhaCircuit reads upper/lower circuit by instrument token.
func (c *RedisTickCache) GetZerodhaCircuit(ctx context.Context, token int64) (zerodhaCircuitLimit, bool) {
	if c == nil || c.client == nil || token <= 0 {
		return zerodhaCircuitLimit{}, false
	}
	key := ZerodhaCircuitPrefix + strconv.FormatInt(token, 10)
	value, err := c.client.Get(ctx, key).Result()
	if err != nil {
		return zerodhaCircuitLimit{}, false
	}
	var limit zerodhaCircuitLimit
	if err := json.Unmarshal([]byte(value), &limit); err != nil {
		return zerodhaCircuitLimit{}, false
	}
	if limit.Upper <= 0 || limit.Lower <= 0 {
		return zerodhaCircuitLimit{}, false
	}
	return limit, true
}

// RemoveFromSymbolList removes the given symbols from a stored symbol list key.
func (c *RedisTickCache) RemoveFromSymbolList(ctx context.Context, key string, toRemove []string) {
	if c == nil || c.client == nil || len(toRemove) == 0 {
		return
	}
	current := c.GetSymbolList(ctx, key)
	if len(current) == 0 {
		return
	}
	removeSet := make(map[string]struct{}, len(toRemove))
	for _, s := range toRemove {
		removeSet[strings.ToUpper(strings.TrimSpace(s))] = struct{}{}
	}
	filtered := make([]string, 0, len(current))
	for _, s := range current {
		if _, ok := removeSet[s]; !ok {
			filtered = append(filtered, s)
		}
	}
	c.SetSymbolList(ctx, key, filtered)
}

// DeleteBySymbols removes tick cache keys for the given symbols under the prefix.
func (c *RedisTickCache) DeleteBySymbols(ctx context.Context, prefix string, symbols []string) int {
	if c == nil || c.client == nil || len(symbols) == 0 {
		return 0
	}
	keys := make([]string, 0, len(symbols))
	for _, sym := range symbols {
		norm := strings.ToUpper(strings.TrimSpace(sym))
		if norm != "" {
			keys = append(keys, prefix+norm)
		}
	}
	if len(keys) == 0 {
		return 0
	}
	del, err := c.client.Del(ctx, keys...).Result()
	if err != nil {
		log.Printf("redis cache delete error: prefix=%s count=%d err=%v", prefix, len(keys), err)
	}
	return int(del)
}

// DeleteCircuitByTokens removes circuit cache keys for the given instrument tokens.
func (c *RedisTickCache) DeleteCircuitByTokens(ctx context.Context, tokens []int64) int {
	if c == nil || c.client == nil || len(tokens) == 0 {
		return 0
	}
	keys := make([]string, 0, len(tokens))
	for _, token := range tokens {
		if token > 0 {
			keys = append(keys, ZerodhaCircuitPrefix+strconv.FormatInt(token, 10))
		}
	}
	if len(keys) == 0 {
		return 0
	}
	del, err := c.client.Del(ctx, keys...).Result()
	if err != nil {
		log.Printf("redis circuit delete error: count=%d err=%v", len(keys), err)
	}
	return int(del)
}
