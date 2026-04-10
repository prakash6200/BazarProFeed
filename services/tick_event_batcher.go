package services

import (
	"context"
	"log"
	"sync"
	"time"
)

const (
	tickEventFlushInterval = 10 * time.Second
	tickEventBatchSize     = 500
	tickEventQueueSize     = 20000
)

type TickEventEnqueuer[T any] interface {
	Start(ctx context.Context)
	Enqueue(item T)
}

type TickEventBatcher[T any] struct {
	name       string
	flushEvery time.Duration
	batchSize  int
	queue      chan T
	flushFn    func([]T) error
	startOnce  sync.Once
}

func NewTickEventBatcher[T any](name string, flushEvery time.Duration, batchSize, queueSize int, flushFn func([]T) error) *TickEventBatcher[T] {
	if flushEvery <= 0 {
		flushEvery = tickEventFlushInterval
	}
	if batchSize <= 0 {
		batchSize = tickEventBatchSize
	}
	if queueSize <= 0 {
		queueSize = tickEventQueueSize
	}

	return &TickEventBatcher[T]{
		name:       name,
		flushEvery: flushEvery,
		batchSize:  batchSize,
		queue:      make(chan T, queueSize),
		flushFn:    flushFn,
	}
}

func (b *TickEventBatcher[T]) Start(ctx context.Context) {
	if b == nil || b.flushFn == nil {
		return
	}

	b.startOnce.Do(func() {
		go b.run(ctx)
	})
}

func (b *TickEventBatcher[T]) Enqueue(item T) {
	if b == nil {
		return
	}

	select {
	case b.queue <- item:
	default:
		// Keep data loss minimal by applying backpressure only when queue is fully saturated.
		log.Printf("%s batch queue saturated, applying backpressure", b.name)
		b.queue <- item
	}
}

func (b *TickEventBatcher[T]) run(ctx context.Context) {
	ticker := time.NewTicker(b.flushEvery)
	defer ticker.Stop()

	batch := make([]T, 0, b.batchSize)

	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := b.flushFn(batch); err != nil {
			log.Printf("%s batch flush failed (size=%d): %v", b.name, len(batch), err)
		}
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			for {
				select {
				case item := <-b.queue:
					batch = append(batch, item)
					if len(batch) >= b.batchSize {
						flush()
					}
				default:
					flush()
					return
				}
			}
		case item := <-b.queue:
			batch = append(batch, item)
			if len(batch) >= b.batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}
