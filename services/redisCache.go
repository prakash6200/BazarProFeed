package services

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const tickCacheTTL = 24 * time.Hour

const (
	ZerodhaTickPrefix = "feed:zerodha:tick:"
	GlobalTickPrefix  = "feed:global:tick:"
)

// RedisTickCache caches NormalizedTick values in Redis per symbol.
// All methods are safe to call with a nil client (no-ops).
type RedisTickCache struct {
	client *redis.Client
}

func NewRedisTickCache(client *redis.Client) *RedisTickCache {
	return &RedisTickCache{client: client}
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
