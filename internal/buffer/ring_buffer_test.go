package buffer

import (
	"context"
	"sync"
	"testing"
	"time"

	"git.famapp.in/fampay-inc/wal-cake/internal/ack"
	"git.famapp.in/fampay-inc/wal-cake/internal/model"
)

// gatedProcessor blocks each segment until its gate is released. Segments are
// keyed by the LSN of their first event.
type gatedProcessor struct {
	mu        sync.Mutex
	gates     map[uint64]chan struct{}
	processed []uint64
}

func newGatedProcessor() *gatedProcessor {
	return &gatedProcessor{gates: map[uint64]chan struct{}{}}
}

func (p *gatedProcessor) gate(first uint64) chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	g, ok := p.gates[first]
	if !ok {
		g = make(chan struct{})
		p.gates[first] = g
	}
	return g
}

func (p *gatedProcessor) release(first uint64) { close(p.gate(first)) }

func (p *gatedProcessor) Process(_ context.Context, events []*model.CDCEvent) error {
	<-p.gate(events[0].LSN)
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, e := range events {
		p.processed = append(p.processed, e.LSN)
	}
	return nil
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func send(ch chan<- *model.CDCEvent, from, to uint64) {
	for lsn := from; lsn <= to; lsn++ {
		ch <- &model.CDCEvent{Operation: model.InsertOp, LSN: lsn}
	}
}

// Segments finish out of order; the acknowledged position only covers the
// contiguous finished prefix.
func TestRingAcksContiguousPrefix(t *testing.T) {
	proc := newGatedProcessor()
	acked := &ack.Position{}
	rb := NewRingBuffer(2, 4, time.Hour, proc, acked)
	events := make(chan *model.CDCEvent, 16)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { rb.Start(ctx, events); close(done) }()

	send(events, 1, 8) // S1=[1,2] S2=[3,4] S3=[5,6] S4=[7,8]

	proc.release(3) // S2
	proc.release(7) // S4
	time.Sleep(50 * time.Millisecond)
	if got := acked.Load(); got != 0 {
		t.Fatalf("acked %d with S1 still running, want 0", got)
	}
	proc.release(1) // S1: crosses S1 and S2
	eventually(t, "ACK of S1+S2", func() bool { return acked.Load() == 4 })
	proc.release(5) // S3: crosses S3 and S4
	eventually(t, "ACK of S3+S4", func() bool { return acked.Load() == 8 })

	cancel()
	<-done
}

// On shutdown the ring sends the partial segment, waits for in-flight work,
// and acknowledges everything that finished.
func TestRingDrainsOnShutdown(t *testing.T) {
	proc := newGatedProcessor()
	acked := &ack.Position{}
	rb := NewRingBuffer(4, 2, time.Hour, proc, acked)
	events := make(chan *model.CDCEvent, 16)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { rb.Start(ctx, events); close(done) }()

	send(events, 1, 6) // one full segment [1..4], two pending events [5,6]
	eventually(t, "events admitted", func() bool { return rb.writeIdx.Load() == 6 })

	cancel()
	proc.release(1)
	proc.release(5)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after drain")
	}
	if got := acked.Load(); got != 6 {
		t.Fatalf("acked %d after drain, want 6", got)
	}
	if len(proc.processed) != 6 {
		t.Fatalf("processed %d events, want 6", len(proc.processed))
	}
}

// When the ring is full, the writer resumes as soon as space frees up, not on
// a polling timer.
func TestRingWriterWakesWhenSpaceFrees(t *testing.T) {
	proc := newGatedProcessor()
	acked := &ack.Position{}
	rb := NewRingBuffer(1, 1, time.Hour, proc, acked) // 2 slots
	events := make(chan *model.CDCEvent, 16)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go rb.Start(ctx, events)

	send(events, 1, 3) // slots hold 1 and 2; 3 waits for space
	eventually(t, "ring full", func() bool { return rb.writeIdx.Load() == 2 })

	start := time.Now()
	proc.release(1)
	eventually(t, "event 3 admitted", func() bool { return rb.writeIdx.Load() == 3 })
	if waited := time.Since(start); waited > 50*time.Millisecond {
		t.Fatalf("writer resumed after %s, want no polling delay", waited)
	}
	proc.release(2)
	proc.release(3)
}
