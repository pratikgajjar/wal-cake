package buffer

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"pgregory.net/rapid"

	"git.famapp.in/fampay-inc/wal-cake/internal/ack"
	"git.famapp.in/fampay-inc/wal-cake/internal/model"
)

// holdingProcessor holds every segment until the test releases it. Segments
// are identified by the Table field of their first event, which the test sets
// to a unique id.
type holdingProcessor struct {
	mu      sync.Mutex
	waiting map[string]chan struct{} // segments inside Process
	all     bool                     // release everything, now and later
}

func (p *holdingProcessor) Process(_ context.Context, events []*model.CDCEvent) error {
	id := events[0].Table
	gate := make(chan struct{})
	p.mu.Lock()
	if p.all {
		p.mu.Unlock()
		return nil
	}
	p.waiting[id] = gate
	p.mu.Unlock()
	<-gate
	return nil
}

func (p *holdingProcessor) waitingIDs() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	ids := make([]string, 0, len(p.waiting))
	for id := range p.waiting {
		ids = append(ids, id)
	}
	sort.Strings(ids) // deterministic, so rapid can replay and shrink
	return ids
}

func (p *holdingProcessor) release(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	close(p.waiting[id])
	delete(p.waiting, id)
}

func (p *holdingProcessor) releaseAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.all = true
	for id, g := range p.waiting {
		close(g)
		delete(p.waiting, id)
	}
}

// ringMachine drives the real ring and keeps a model of what it must ACK.
type ringMachine struct {
	batch, size int
	rb          *RingBuffer
	proc        *holdingProcessor
	acked       *ack.Position
	events      chan *model.CDCEvent
	cancel      context.CancelFunc
	done        chan struct{}

	sent     []*model.CDCEvent // every event sent, in order
	finished map[string]bool   // segment id -> released
	prefix   int               // events in the contiguous finished prefix
}

func newRingMachine(t *rapid.T) *ringMachine {
	m := &ringMachine{
		batch:    rapid.IntRange(1, 5).Draw(t, "batch"),
		proc:     &holdingProcessor{waiting: map[string]chan struct{}{}},
		acked:    &ack.Position{},
		events:   make(chan *model.CDCEvent, 10000),
		finished: map[string]bool{},
		done:     make(chan struct{}),
	}
	concurrency := rapid.IntRange(1, 4).Draw(t, "concurrency")
	m.size = 2 * concurrency * m.batch
	m.rb = NewRingBuffer(m.batch, concurrency, time.Hour, m.proc, m.acked)
	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel
	go func() { m.rb.Start(ctx, m.events); close(m.done) }()
	return m
}

// Send sends events with arbitrary, possibly non-monotonic LSNs.
func (m *ringMachine) Send(t *rapid.T) {
	n := rapid.IntRange(1, 3*m.batch).Draw(t, "n")
	for range n {
		ev := &model.CDCEvent{Table: fmt.Sprintf("e%d", len(m.sent)), LSN: rapid.Uint64Range(1, 1000).Draw(t, "lsn")}
		m.sent = append(m.sent, ev)
		m.events <- ev
	}
	m.settle(t)
}

// Release lets one held segment finish.
func (m *ringMachine) Release(t *rapid.T) {
	ids := m.proc.waitingIDs()
	if len(ids) == 0 {
		t.Skip("no segment in flight")
	}
	id := rapid.SampledFrom(ids).Draw(t, "segment")
	m.proc.release(id)
	m.finished[id] = true
	m.advanceModel()
	m.settle(t)
}

// advanceModel moves the model's prefix over contiguous finished segments.
func (m *ringMachine) advanceModel() {
	for m.prefix+m.batch <= len(m.sent) && m.finished[m.sent[m.prefix].Table] {
		m.prefix += m.batch
	}
}

// ackable reports whether lsn may be acknowledged: it must belong to an event
// inside the finished prefix. When the walker crosses several segments at once
// it publishes only the last one, so which prefix LSNs appear depends on
// timing, but nothing outside the prefix may.
func (m *ringMachine) ackable(lsn uint64) bool {
	if lsn == 0 {
		return true
	}
	for _, ev := range m.sent[:m.prefix] {
		if ev.LSN == lsn {
			return true
		}
	}
	return false
}

// settle waits until the ring has admitted what it can and the walker has
// caught up with the model.
func (m *ringMachine) settle(t *rapid.T) {
	deadline := time.Now().Add(2 * time.Second)
	for {
		admitted := min(len(m.sent), int(m.rb.readIdx.Load())+m.size)
		caughtUp := m.prefix == 0 || m.acked.Load() >= m.sent[m.prefix-1].LSN
		if int(m.rb.writeIdx.Load()) == admitted && int(m.rb.readIdx.Load()) == m.prefix && caughtUp {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("ring did not settle: writeIdx=%d want %d, readIdx=%d want %d, acked=%d, finished=%v, in flight=%v",
				m.rb.writeIdx.Load(), admitted, m.rb.readIdx.Load(), m.prefix, m.acked.Load(), m.finished, m.proc.waitingIDs())
		}
		time.Sleep(time.Millisecond)
	}
}

func (m *ringMachine) Check(t *rapid.T) {
	if got := m.acked.Load(); !m.ackable(got) {
		t.Fatalf("acked %d is not the LSN of any event in the finished prefix of %d events", got, m.prefix)
	}
	if got := m.rb.readIdx.Load(); got > int64(m.prefix) {
		t.Fatalf("readIdx %d is past the finished prefix %d", got, m.prefix)
	}
	if used := m.rb.writeIdx.Load() - m.rb.readIdx.Load(); used > int64(m.size) {
		t.Fatalf("ring holds %d events, capacity %d", used, m.size)
	}
}

// drain finishes everything and checks the final ACK covers every event.
func (m *ringMachine) drain(t *rapid.T) {
	m.cancel()
	m.proc.releaseAll()
	select {
	case <-m.done:
	case <-time.After(5 * time.Second):
		t.Fatal("ring did not drain")
	}
	admitted := int(m.rb.writeIdx.Load())
	if int(m.rb.readIdx.Load()) != admitted {
		t.Fatalf("after drain readIdx %d, want %d", m.rb.readIdx.Load(), admitted)
	}
	m.prefix = admitted
	got := m.acked.Load()
	if admitted > 0 && (got < m.sent[admitted-1].LSN || !m.ackable(got)) {
		t.Fatalf("after drain acked %d, want an LSN from the %d admitted events, at least %d", got, admitted, m.sent[admitted-1].LSN)
	}
}

func TestRingStateMachine(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		m := newRingMachine(t)
		t.Repeat(rapid.StateMachineActions(m))
		m.drain(t)
	})
}
