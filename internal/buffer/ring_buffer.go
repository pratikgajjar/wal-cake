package buffer

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"git.famapp.in/fampay-inc/wal-cake/internal/ack"
	"git.famapp.in/fampay-inc/wal-cake/internal/model"
)

type RingBufEvent *model.CDCEvent

// Segment represents a batch of events to be processed together
type Segment struct {
	StartIdx int64 // Start index in the ring buffer
	EndIdx   int64 // End index in the ring buffer
	done     bool
}

// BatchProcessor defines the interface for processing batches of events
type BatchProcessor interface {
	Process(ctx context.Context, events []*model.CDCEvent) error
}

// RingBuffer implements a circular buffer optimized for batch processing
type RingBuffer struct {
	buffer       []RingBufEvent
	size         int64
	writeIdx     atomic.Int64 // Current write position
	readIdx      atomic.Int64 // Current read position
	lastSegIdx   atomic.Int64 // Last segment end position
	nextSegSeq   atomic.Int64 // Next segment sequence number to assign
	batchSize    int
	concurrency  int
	segments     chan Segment
	ticker       *time.Ticker
	tickInterval time.Duration
	processor    BatchProcessor
	ackSeg       chan Segment
	acked        *ack.Position
	space        chan struct{}   // signalled when readIdx moves, so a blocked writer retries at once
	tracker      *segmentTracker // StartIdx -> segment, for every segment not yet crossed by readIdx
}

// NewRingBuffer creates a ring buffer. Durable progress is published to acked.
func NewRingBuffer(batchSize, concurrency int, tickInterval time.Duration, processor BatchProcessor, acked *ack.Position) *RingBuffer {
	// Twice the in-flight capacity: while every worker holds a segment, the
	// writer can still fill the next ones instead of stalling.
	size := 2 * concurrency * batchSize

	rb := &RingBuffer{
		buffer:       make([]RingBufEvent, size),
		size:         int64(size),
		batchSize:    batchSize,
		concurrency:  concurrency,
		segments:     make(chan Segment, concurrency),
		ticker:       time.NewTicker(tickInterval),
		tickInterval: tickInterval,
		processor:    processor,
		ackSeg:       make(chan Segment, concurrency),
		acked:        acked,
		space:        make(chan struct{}, 1),
		tracker:      newSegmentTracker(),
	}
	return rb
}

// Add adds a new event to the ring buffer
// Returns false if the buffer is full
func (rb *RingBuffer) Add(event *model.CDCEvent) bool {
	w := rb.writeIdx.Load()
	if w-rb.readIdx.Load() >= rb.size {
		return false
	}
	rb.buffer[w%rb.size] = event
	rb.writeIdx.Add(1)
	return true
}

// findHighestContiguous finds the highest contiguous LSN after acknowledging a segment
func (rb *RingBuffer) findHighestContiguous(segStartIdx, curReadIdx int64) int64 {
	s, ok := rb.tracker.Get(segStartIdx)
	if !ok {
		return curReadIdx
	}
	s.done = true
	if s.StartIdx == curReadIdx {
		cur := s
		for cur.done {
			rb.tracker.Del(cur.StartIdx)
			curReadIdx = cur.EndIdx
			next, ok := rb.tracker.Get(cur.EndIdx)
			if !ok {
				break
			}
			cur = next
		}
	}
	return curReadIdx
}

// checkForNewSegment checks if there are enough events to create a new segment
// return true if there are enough events to create a new segment
func (rb *RingBuffer) checkForNewSegment() bool {
	writePos := rb.writeIdx.Load()
	lastSegPos := rb.lastSegIdx.Load()

	// Safety check: ensure lastSegIdx never exceeds writeIdx
	if lastSegPos > writePos {
		log.Warn().
			Int64("lastSegPos", lastSegPos).
			Int64("writePos", writePos).
			Msg("lastSegIdx exceeded writeIdx, correcting")
		rb.lastSegIdx.Store(writePos)
		return false
	}

	if int(writePos-lastSegPos) >= rb.batchSize {
		segment := Segment{
			StartIdx: lastSegPos,
			EndIdx:   writePos,
		}
		rb.lastSegIdx.Store(writePos)
		rb.tracker.Set(segment.StartIdx, &segment)
		rb.segments <- segment
		log.Debug().
			Int64("startIdx", segment.StartIdx).
			Int64("endIdx", segment.EndIdx).
			Msg("Created new segment")
		return true
	}
	return false
}

// createTickerSegment creates a segment based on ticker event
func (rb *RingBuffer) createTickerSegment() {
	writePos := rb.writeIdx.Load()
	lastSegPos := rb.lastSegIdx.Load()
	readPos := rb.readIdx.Load()

	// Safety check: ensure lastSegIdx never exceeds writeIdx
	if lastSegPos > writePos {
		log.Warn().
			Int64("lastSegPos", lastSegPos).
			Int64("writePos", writePos).
			Msg("lastSegIdx exceeded writeIdx, correcting")
		rb.lastSegIdx.Store(writePos)
		return
	}

	if lastSegPos < readPos {
		log.Error().
			Int64("lastSegPos", lastSegPos).
			Int64("readPos", readPos).
			Msg("Last segment position is less than read position")
		rb.lastSegIdx.Store(readPos)
		return
	}

	// If there are no pending events, do nothing
	if writePos <= lastSegPos {
		return
	}

	// Create a segment with all pending events
	segment := Segment{
		StartIdx: lastSegPos,
		EndIdx:   writePos,
	}

	// Update last segment position
	rb.lastSegIdx.Store(writePos)
	// Send segment for processing
	rb.tracker.Set(segment.StartIdx, &segment)
	rb.segments <- segment
	log.Debug().
		Int64("startIdx", segment.StartIdx).
		Int64("endIdx", segment.EndIdx).
		Int("pendingSegMap", rb.tracker.Len()).
		Msg("Created ticker-based segment")
}

// Start runs the ring until ctx is cancelled, then drains: it stops taking
// events, sends the last partial segment, waits for the workers, and advances
// the ACK position over everything that finished. It returns nil after the
// drain. Workers are not cancelled by ctx, so in-flight uploads can complete.
func (rb *RingBuffer) Start(ctx context.Context, eventsCh <-chan *model.CDCEvent) error {
	workCtx := context.WithoutCancel(ctx)

	var walker sync.WaitGroup
	walker.Add(1)
	go func() {
		defer walker.Done()
		for seg := range rb.ackSeg {
			rb.handleSegmentAck(seg)
		}
	}()

	var workers sync.WaitGroup
	for i := range rb.concurrency {
		workers.Add(1)
		go func(workerID int) {
			defer workers.Done()
			rb.worker(workCtx, workerID)
		}(i)
	}

	rb.receive(ctx, eventsCh)

	// Drain: the receiver is the only sender on segments, so it closes it.
	rb.createTickerSegment()
	close(rb.segments)
	workers.Wait()
	close(rb.ackSeg)
	walker.Wait()
	log.Info().Int64("readIdx", rb.readIdx.Load()).Uint64("ackedLSN", rb.acked.Load()).Msg("Ring buffer drained")
	return nil
}

// receive admits events and cuts segments until ctx is cancelled.
func (rb *RingBuffer) receive(ctx context.Context, eventsCh <-chan *model.CDCEvent) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-rb.ticker.C:
			rb.createTickerSegment()
		case event := <-eventsCh:
			for !rb.Add(event) {
				// Full. The event is not in the ring yet, so dropping it on
				// shutdown is safe: it was never confirmed.
				select {
				case <-ctx.Done():
					return
				case <-rb.space:
				}
			}
			// Don't create ticker segment if we create one
			if rb.checkForNewSegment() {
				rb.ticker.Reset(rb.tickInterval)
			}
		}
	}
}

// worker processes segments until the segments channel is closed.
func (rb *RingBuffer) worker(ctx context.Context, workerID int) {
	log.Info().Int("workerID", workerID).Msg("Starting ring buffer worker")

	for segment := range rb.segments {
		events := make([]*model.CDCEvent, 0, segment.EndIdx-segment.StartIdx)
		for pos := segment.StartIdx; pos < segment.EndIdx; pos++ {
			if ev := rb.buffer[pos%rb.size]; ev != nil {
				events = append(events, ev)
			}
		}

		if len(events) > 0 {
			log.Debug().
				Int("workerID", workerID).
				Int("eventCount", len(events)).
				Int64("startIdx", segment.StartIdx).
				Int64("endIdx", segment.EndIdx).
				Msg("Processing segment")

			for i := range 3 {
				err := rb.processor.Process(ctx, events)
				if err == nil {
					break
				}
				log.Error().Err(err).Int("retry", i+1).Msg("Error processing segment")
				if i == 2 {
					// The segment stays unconfirmed, so the slot replays it on restart.
					log.Fatal().Err(err).Msg("Failed to process segment")
				}
				time.Sleep(time.Second * (2 << i))
			}
		}
		rb.ackSeg <- segment
	}
}

// handleSegmentAck marks a segment done, moves readIdx over the contiguous
// finished prefix, and publishes the prefix's last LSN.
func (rb *RingBuffer) handleSegmentAck(segment Segment) {
	previousReadIdx := rb.readIdx.Load()
	highContiguous := rb.findHighestContiguous(segment.StartIdx, previousReadIdx)
	log.Debug().
		Any("segment", segment).
		Int64("prevRead", previousReadIdx).
		Int64("highCont", highContiguous).
		Msg("Ack segment")
	if highContiguous <= previousReadIdx {
		return
	}
	// Read the last event before publishing readIdx: once readIdx moves, the
	// writer may reuse that slot.
	lastEvent := rb.buffer[(highContiguous-1)%rb.size]
	rb.readIdx.Store(highContiguous)
	select {
	case rb.space <- struct{}{}:
	default:
	}
	if lastEvent != nil {
		rb.acked.Advance(lastEvent.LSN)
		log.Debug().Uint64("lsn", lastEvent.LSN).Msg("Advanced acknowledged position")
	}
}
