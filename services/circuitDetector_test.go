package services

import (
	"encoding/json"
	"sync"
	"testing"
	"time"
)

type mockBroadcaster struct {
	mu     sync.Mutex
	events []CircuitEvent
}

func (m *mockBroadcaster) BroadcastJSON(payload any) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return
	}
	var evt CircuitEvent
	if err := json.Unmarshal(raw, &evt); err != nil {
		return
	}
	if evt.Type == CircuitEventTypeHit || evt.Type == CircuitEventTypeOpen {
		m.mu.Lock()
		m.events = append(m.events, evt)
		m.mu.Unlock()
	}
}

func (m *mockBroadcaster) getEvents() []CircuitEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]CircuitEvent, len(m.events))
	copy(out, m.events)
	return out
}

func TestCircuitDetector_FullLifecycle(t *testing.T) {
	hub := &mockBroadcaster{}
	detector := &CircuitDetectorService{hub: hub}
	detector.limits.Store("NSE:RELIANCE", circuitLimit{Upper: 3000.0, Lower: 2500.0})

	// Tick 1: Normal price — no event
	detector.processTick(NormalizedTick{Exchange: "NSE", Symbol: "RELIANCE", LTP: 2800.0, Timestamp: time.Now().UTC()})
	if evts := hub.getEvents(); len(evts) != 0 {
		t.Fatalf("tick1: expected 0 events, got %d", len(evts))
	}

	// Tick 2: LTP hits upper circuit — CIRCUIT_HIT UPPER
	detector.processTick(NormalizedTick{Exchange: "NSE", Symbol: "RELIANCE", LTP: 3000.0, Timestamp: time.Now().UTC()})
	evts := hub.getEvents()
	if len(evts) != 1 {
		t.Fatalf("tick2: expected 1 event, got %d", len(evts))
	}
	if evts[0].Type != CircuitEventTypeHit || evts[0].Side != CircuitSideUpper {
		t.Fatalf("tick2: expected CIRCUIT_HIT UPPER, got %s %s", evts[0].Type, evts[0].Side)
	}
	if evts[0].Symbol != "RELIANCE" || evts[0].Segment != CircuitSegmentEquity {
		t.Fatalf("tick2: expected RELIANCE EQUITY, got %s %s", evts[0].Symbol, evts[0].Segment)
	}
	if evts[0].UpperLimit != 3000.0 || evts[0].LowerLimit != 2500.0 {
		t.Fatalf("tick2: wrong limits %f/%f", evts[0].UpperLimit, evts[0].LowerLimit)
	}

	// Tick 3: Still at upper — NO duplicate (edge trigger)
	detector.processTick(NormalizedTick{Exchange: "NSE", Symbol: "RELIANCE", LTP: 3000.0, Timestamp: time.Now().UTC()})
	if evts := hub.getEvents(); len(evts) != 1 {
		t.Fatalf("tick3: expected still 1 event (no dup), got %d", len(evts))
	}

	// Tick 4: Above upper — still no new event
	detector.processTick(NormalizedTick{Exchange: "NSE", Symbol: "RELIANCE", LTP: 3050.0, Timestamp: time.Now().UTC()})
	if evts := hub.getEvents(); len(evts) != 1 {
		t.Fatalf("tick4: expected still 1 event, got %d", len(evts))
	}

	// Tick 5: Drops below upper — CIRCUIT_OPEN UPPER
	detector.processTick(NormalizedTick{Exchange: "NSE", Symbol: "RELIANCE", LTP: 2950.0, Timestamp: time.Now().UTC()})
	evts = hub.getEvents()
	if len(evts) != 2 {
		t.Fatalf("tick5: expected 2 events, got %d", len(evts))
	}
	if evts[1].Type != CircuitEventTypeOpen || evts[1].Side != CircuitSideUpper {
		t.Fatalf("tick5: expected CIRCUIT_OPEN UPPER, got %s %s", evts[1].Type, evts[1].Side)
	}

	// Tick 6: LTP hits lower circuit — CIRCUIT_HIT LOWER
	detector.processTick(NormalizedTick{Exchange: "NSE", Symbol: "RELIANCE", LTP: 2500.0, Timestamp: time.Now().UTC()})
	evts = hub.getEvents()
	if len(evts) != 3 {
		t.Fatalf("tick6: expected 3 events, got %d", len(evts))
	}
	if evts[2].Type != CircuitEventTypeHit || evts[2].Side != CircuitSideLower {
		t.Fatalf("tick6: expected CIRCUIT_HIT LOWER, got %s %s", evts[2].Type, evts[2].Side)
	}

	// Tick 7: Still at lower — no duplicate
	detector.processTick(NormalizedTick{Exchange: "NSE", Symbol: "RELIANCE", LTP: 2500.0, Timestamp: time.Now().UTC()})
	if evts := hub.getEvents(); len(evts) != 3 {
		t.Fatalf("tick7: expected still 3 events, got %d", len(evts))
	}

	// Tick 8: Recovers above lower — CIRCUIT_OPEN LOWER
	detector.processTick(NormalizedTick{Exchange: "NSE", Symbol: "RELIANCE", LTP: 2600.0, Timestamp: time.Now().UTC()})
	evts = hub.getEvents()
	if len(evts) != 4 {
		t.Fatalf("tick8: expected 4 events, got %d", len(evts))
	}
	if evts[3].Type != CircuitEventTypeOpen || evts[3].Side != CircuitSideLower {
		t.Fatalf("tick8: expected CIRCUIT_OPEN LOWER, got %s %s", evts[3].Type, evts[3].Side)
	}

	t.Logf("Full lifecycle: %d events", len(evts))
	for i, e := range evts {
		t.Logf("  [%d] %s %s %s @ %.2f", i, e.Type, e.Side, e.Symbol, e.Price)
	}
}

func TestCircuitDetector_SegmentFilter(t *testing.T) {
	hub := &mockBroadcaster{}
	detector := &CircuitDetectorService{hub: hub}
	detector.limits.Store("NSE:EQUITY_STOCK", circuitLimit{Upper: 100.0, Lower: 50.0})
	detector.limits.Store("MCX:GOLD", circuitLimit{Upper: 80000.0, Lower: 70000.0})

	detector.processTick(NormalizedTick{Exchange: "NSE", Symbol: "EQUITY_STOCK", LTP: 100.0})
	detector.processTick(NormalizedTick{Exchange: "MCX", Symbol: "GOLD", LTP: 80000.0})
	detector.processTick(NormalizedTick{Exchange: "NFO", Symbol: "NIFTY_FUT", LTP: 25000.0})
	detector.processTick(NormalizedTick{Exchange: "CDS", Symbol: "USDINR", LTP: 85.0})

	evts := hub.getEvents()
	if len(evts) != 2 {
		t.Fatalf("expected 2 events (EQUITY+MCX only), got %d", len(evts))
	}
	if evts[0].Segment != CircuitSegmentEquity {
		t.Fatalf("expected EQUITY, got %s", evts[0].Segment)
	}
	if evts[1].Segment != CircuitSegmentMCX {
		t.Fatalf("expected MCX, got %s", evts[1].Segment)
	}
}

func TestCircuitDetector_NoLimits(t *testing.T) {
	hub := &mockBroadcaster{}
	detector := &CircuitDetectorService{hub: hub}

	detector.processTick(NormalizedTick{Exchange: "NSE", Symbol: "UNKNOWN", LTP: 999.0})

	if evts := hub.getEvents(); len(evts) != 0 {
		t.Fatalf("expected 0 events for unknown symbol, got %d", len(evts))
	}
}
