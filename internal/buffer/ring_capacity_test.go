package buffer

import (
	"context"
	"math"
	"math/rand"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"git.famapp.in/fampay-inc/wal-cake/internal/ack"
	"git.famapp.in/fampay-inc/wal-cake/internal/model"
)

// sleepProcessor stands in for Parquet + S3: 7 ms encode plus a lognormal PUT
// with p50 = 100 ms and p99 ≈ 200 ms.
type sleepProcessor struct{ events atomic.Int64 }

func (p *sleepProcessor) Process(_ context.Context, evs []*model.CDCEvent) error {
	put := time.Duration(float64(100*time.Millisecond) * math.Exp(0.298*rand.NormFloat64()))
	time.Sleep(7*time.Millisecond + put)
	p.events.Add(int64(len(evs)))
	return nil
}

// TestRingCapacity measures the highest input rate the ring sustains with the
// default 4 workers × 1,000 events. It takes about a minute, so it only runs
// with WALCAKE_CAPACITY=1.
func TestRingCapacity(t *testing.T) {
	if os.Getenv("WALCAKE_CAPACITY") == "" {
		t.Skip("WALCAKE_CAPACITY not set")
	}
	for _, rate := range []int{20000, 30000, 36000} {
		proc := &sleepProcessor{}
		rb := NewRingBuffer(1000, 4, 30*time.Second, proc, &ack.Position{})
		events := make(chan *model.CDCEvent, 4000)
		ctx, cancel := context.WithCancel(context.Background())
		go rb.Start(ctx, events)

		const dur = 12 * time.Second
		start := time.Now()
		var sent int64
		ev := &model.CDCEvent{}
		for time.Since(start) < dur {
			for target := int64(time.Since(start).Seconds() * float64(rate)); sent < target; sent++ {
				events <- ev
			}
			time.Sleep(time.Millisecond)
		}
		elapsed := time.Since(start).Seconds()
		t.Logf("offered %6d/s: admitted %3.0f%%, uploaded %6.0f/s", rate, 100*float64(sent)/(float64(rate)*elapsed), float64(proc.events.Load())/elapsed)
		cancel()
	}
}
