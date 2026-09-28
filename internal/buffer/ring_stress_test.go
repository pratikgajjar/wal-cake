package buffer

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"git.famapp.in/fampay-inc/wal-cake/internal/ack"
	"git.famapp.in/fampay-inc/wal-cake/internal/model"
)

type nopProcessor struct{}

func (nopProcessor) Process(context.Context, []*model.CDCEvent) error { return nil }

// Many tiny segments finishing concurrently. If the walker ever loses track of
// a segment, readIdx stops and the ring stalls forever.
func TestRingDoesNotStallUnderLoad(t *testing.T) {
	const n = 200_000
	acked := &ack.Position{}
	rb := NewRingBuffer(1, 4, time.Hour, nopProcessor{}, acked)
	events := make(chan *model.CDCEvent, 1024)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go rb.Start(ctx, events)

	go func() {
		for i := uint64(1); i <= n; i++ {
			events <- &model.CDCEvent{LSN: i}
		}
	}()

	deadline := time.Now().Add(30 * time.Second)
	for acked.Load() != n {
		if time.Now().After(deadline) {
			t.Fatalf("stalled: readIdx=%d writeIdx=%d acked=%d of %d", rb.readIdx.Load(), rb.writeIdx.Load(), acked.Load(), n)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// The tracker is written by the receiver and read and deleted by the walker.
// Get must never miss a key that is present.
func TestSegmentTrackerConcurrentGet(t *testing.T) {
	tr := newSegmentTracker()
	const n = 200_000
	var inserted, deleted atomicInt
	go func() {
		for k := int64(0); k < n; k++ {
			for k-deleted.load() >= 16 {
			}
			tr.Set(k, &Segment{StartIdx: k})
			inserted.store(k + 1)
		}
	}()
	for k := int64(0); k < n; k++ {
		for inserted.load() <= k {
		}
		if _, ok := tr.Get(k); !ok {
			t.Fatalf("Get(%d) missed a present key", k)
		}
		tr.Del(k)
		deleted.store(k + 1)
	}
}

type atomicInt struct{ v atomic.Int64 }

func (a *atomicInt) load() int64   { return a.v.Load() }
func (a *atomicInt) store(x int64) { a.v.Store(x) }
