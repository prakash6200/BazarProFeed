package services

import (
	"sync"
	"time"
)

type TickEvent struct {
	InstrumentToken int64     `json:"instrumentToken"`
	LTP             float64   `json:"ltp"`
	Timestamp       time.Time `json:"timestamp"`
}

type TickHub struct {
	mu          sync.RWMutex
	subscribers map[chan TickEvent]struct{}
}

func NewTickHub() *TickHub {
	return &TickHub{
		subscribers: make(map[chan TickEvent]struct{}),
	}
}

func (h *TickHub) Subscribe(buffer int) (<-chan TickEvent, func()) {
	if buffer <= 0 {
		buffer = 1
	}

	ch := make(chan TickEvent, buffer)
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

func (h *TickHub) Publish(event TickEvent) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for ch := range h.subscribers {
		select {
		case ch <- event:
		default:
		}
	}
}
