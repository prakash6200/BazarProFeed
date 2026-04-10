package services

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const tickCacheTTL = 24 * time.Hour

const (
	ZerodhaTickPrefix    = "feed:zerodha:tick:"
	ZerodhaCircuitPrefix = "feed:zerodha:circuit:"
	GlobalTickPrefix     = "feed:global:tick:"
	GlobalSymbolListKey  = "feed:global:symbols"
)

// RedisTickCache caches NormalizedTick values in Redis per symbol.
// All methods are safe to call with a nil client (no-ops).
type RedisTickCache struct {
	client *redis.Client
}

func NewRedisTickCache(client *redis.Client) *RedisTickCache {
	return &RedisTickCache{client: client}
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
		keys, err = c.client.Keys(ctx, prefix+"*").Result()
		if err != nil || len(keys) == 0 {
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
