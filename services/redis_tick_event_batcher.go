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

const (
	redisQueueMaxLen = 20000

	// Local buffer between the hot feed goroutine and the Redis RPush
	// worker. Enqueue() is lock-free and NEVER blocks the tick path.
	redisLocalQueueSize = 20000

	// Soft deadline for each Redis RPush/LLen attempt issued by the
	// writer goroutine. Kept short so Redis outages cannot wedge the
	// pipeline — items are simply dropped until Redis recovers.
	redisWriteTimeout = 2 * time.Second

	// Number of items coalesced into a single RPUSH pipeline call.
	redisWriteBatch = 200
)

type RedisTickEventBatcher[T any] struct {
	name       string
	queueKey   string
	client     *redis.Client
	flushEvery time.Duration
	batchSize  int
	flushFn    func([]T) error

	startOnce sync.Once

	// localQueue buffers marshalled payloads on the producer side so the
	// hot feed loop never waits on Redis. The writer goroutine drains
	// this channel and pushes to Redis in the background.
	localQueue chan []byte
	dropped    uint64
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
		localQueue: make(chan []byte, redisLocalQueueSize),
	}
}

func (b *RedisTickEventBatcher[T]) Start(ctx context.Context) {
	if b == nil || b.client == nil || b.flushFn == nil || b.queueKey == "" {
		return
	}
	b.startOnce.Do(func() {
		go b.producerLoop(ctx)
		go b.consumerLoop(ctx)
	})
}

// Enqueue is lock-free and never blocks. If the local buffer is full the
// event is silently dropped — it is far better to lose a tick than to
// stall the WebSocket reader goroutine.
func (b *RedisTickEventBatcher[T]) Enqueue(item T) {
	if b == nil || b.client == nil || b.queueKey == "" {
		return
	}

	payload, err := json.Marshal(item)
	if err != nil {
		return
	}

	select {
	case b.localQueue <- payload:
	default:
		// Local buffer full — Redis writer is lagging. Drop.
		if b.dropped++; b.dropped%1000 == 1 {
			log.Printf("%s local queue full (cap=%d), dropped=%d", b.name, cap(b.localQueue), b.dropped)
		}
	}
}

// producerLoop pulls marshalled items off the local buffer and pushes
// them to Redis in batches. Redis slowness only affects this goroutine
// — it can never back-pressure the feed reader.
func (b *RedisTickEventBatcher[T]) producerLoop(ctx context.Context) {
	batch := make([][]byte, 0, redisWriteBatch)
	flushTicker := time.NewTicker(200 * time.Millisecond)
	defer flushTicker.Stop()

	push := func() {
		if len(batch) == 0 {
			return
		}
		writeCtx, cancel := context.WithTimeout(context.Background(), redisWriteTimeout)
		// Cheap length check so a backed-up queue doesn't grow unbounded.
		if qLen, err := b.client.LLen(writeCtx, b.queueKey).Result(); err == nil && qLen >= redisQueueMaxLen {
			cancel()
			log.Printf("%s redis queue full (len=%d), dropping batch=%d", b.name, qLen, len(batch))
			batch = batch[:0]
			return
		}
		args := make([]interface{}, len(batch))
		for i, p := range batch {
			args[i] = p
		}
		if err := b.client.RPush(writeCtx, b.queueKey, args...).Err(); err != nil {
			log.Printf("%s redis RPush failed (size=%d): %v", b.name, len(batch), err)
		}
		cancel()
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			push()
			return
		case item := <-b.localQueue:
			batch = append(batch, item)
			if len(batch) >= redisWriteBatch {
				push()
			}
		case <-flushTicker.C:
			push()
		}
	}
}

// consumerLoop drains the Redis queue on a separate goroutine and
// periodically flushes to Postgres via flushFn. Redis BLPop is done
// with a short timeout so ctx cancellation is honoured promptly.
func (b *RedisTickEventBatcher[T]) consumerLoop(ctx context.Context) {
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
