package services

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const redisQueueMaxLen = 20000

type RedisTickEventBatcher[T any] struct {
	name       string
	queueKey   string
	client     *redis.Client
	flushEvery time.Duration
	batchSize  int
	flushFn    func([]T) error
	startOnce  sync.Once
}

func NewRedisTickEventBatcher[T any](name, queueKey string, client *redis.Client, flushEvery time.Duration, batchSize int, flushFn func([]T) error) *RedisTickEventBatcher[T] {
	if flushEvery <= 0 {
		flushEvery = tickEventFlushInterval
	}
	if batchSize <= 0 {
		batchSize = tickEventBatchSize
	}
	return &RedisTickEventBatcher[T]{
		name:       name,
		queueKey:   queueKey,
		client:     client,
		flushEvery: flushEvery,
		batchSize:  batchSize,
		flushFn:    flushFn,
	}
}

func (b *RedisTickEventBatcher[T]) Start(ctx context.Context) {
	if b == nil || b.client == nil || b.flushFn == nil || b.queueKey == "" {
		return
	}
	b.startOnce.Do(func() {
		go b.run(ctx)
	})
}

func (b *RedisTickEventBatcher[T]) Enqueue(item T) {
	if b == nil || b.client == nil || b.queueKey == "" {
		return
	}

	payload, err := json.Marshal(item)
	if err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	qLen, err := b.client.LLen(ctx, b.queueKey).Result()
	if err == nil && qLen >= redisQueueMaxLen {
		log.Printf("%s redis queue full (len=%d), dropping event", b.name, qLen)
		return
	}
	if err := b.client.RPush(ctx, b.queueKey, payload).Err(); err != nil {
		log.Printf("%s redis enqueue failed: %v", b.name, err)
	}
}

func (b *RedisTickEventBatcher[T]) run(ctx context.Context) {
	ticker := time.NewTicker(b.flushEvery)
	defer ticker.Stop()

	batch := make([]T, 0, b.batchSize)

	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := b.flushFn(batch); err != nil {
			log.Printf("%s redis batch flush failed (size=%d): %v", b.name, len(batch), err)
		}
		batch = batch[:0]
	}

	blpopBackoff := time.Duration(0)

	for {
		if ctx.Err() != nil {
			flush()
			return
		}

		if blpopBackoff > 0 {
			select {
			case <-time.After(blpopBackoff):
			case <-ctx.Done():
				flush()
				return
			}
		}

		res, err := b.client.BLPop(ctx, time.Second, b.queueKey).Result()
		if err == nil && len(res) == 2 {
			blpopBackoff = 0
			var item T
			if unmarshalErr := json.Unmarshal([]byte(res[1]), &item); unmarshalErr == nil {
				batch = append(batch, item)
			}
		} else if err != nil && !errors.Is(err, redis.Nil) && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			log.Printf("%s redis dequeue failed: %v", b.name, err)
			if blpopBackoff == 0 {
				blpopBackoff = 500 * time.Millisecond
			} else {
				blpopBackoff *= 2
				if blpopBackoff > 10*time.Second {
					blpopBackoff = 10 * time.Second
				}
			}
		} else {
			blpopBackoff = 0
		}

		if len(batch) >= b.batchSize {
			flush()
		}

		select {
		case <-ticker.C:
			flush()
		default:
		}
	}
}
