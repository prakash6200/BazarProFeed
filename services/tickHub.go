package services

import (
	"sync"
)

type TickHub struct {
	mu          sync.RWMutex
	subscribers map[chan NormalizedTick]struct{}
}

func NewTickHub() *TickHub {
	return &TickHub{
		subscribers: make(map[chan NormalizedTick]struct{}),
	}
}

func (h *TickHub) Subscribe(buffer int) (<-chan NormalizedTick, func()) {
	if buffer <= 0 {
		buffer = 1
	}

	ch := make(chan NormalizedTick, buffer)
	h.mu.Lock()
	h.subscribers[ch] = struct{}{}
	h.mu.Unlock()

	unsubscribe := func() {
		h.mu.Lock()
		if _, ok := h.subscribers[ch]; ok {
			delete(h.subscribers, ch)
			close(ch)
		}
		h.mu.Unlock()
	}

	return ch, unsubscribe
}

func (h *TickHub) Publish(event NormalizedTick) {
	h.mu.RLock()
	subscribers := make([]chan NormalizedTick, 0, len(h.subscribers))
	for ch := range h.subscribers {
		subscribers = append(subscribers, ch)
	}
	h.mu.RUnlock()

	for _, ch := range subscribers {
		select {
		case ch <- event:
		default:
		}
	}
}

func (h *TickHub) SubscribersCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.subscribers)
}
