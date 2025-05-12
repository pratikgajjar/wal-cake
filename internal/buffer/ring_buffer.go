package buffer

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

	"git.famapp.in/fampay-inc/wal-cake/internal/model"
)

type RingBufEvent *model.CDCEvent

// Segment represents a batch of events to be processed together
type Segment struct {
	StartIdx int64 // Start index in the ring buffer
	EndIdx   int64 // End index in the ring buffer
}

// BatchProcessor defines the interface for processing batches of events
type BatchProcessor interface {
	Process(ctx context.Context, events []*model.CDCEvent) error
}

// SegmentTracker tracks completed segments for efficient acknowledgment processing
type SegmentTracker struct {
	mu      sync.Mutex
	pending map[int64]*Segment
}

func (st *SegmentTracker) Add(segment *Segment) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.pending[segment.StartIdx] = segment
}

func (st *SegmentTracker) Remove(seq int64) {
	st.mu.Lock()
	defer st.mu.Unlock()
	delete(st.pending, seq)
}

// RingBuffer implements a circular buffer optimized for batch processing
type RingBuffer struct {
	buffer       []RingBufEvent
	size         int
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
	ackCh        chan<- uint64
	tracker      *SegmentTracker // Tracks completed segments
}

// NewRingBuffer creates a new ring buffer with the specified size
func NewRingBuffer(batchSize, concurrency int, tickInterval time.Duration, processor BatchProcessor, ackCh chan<- uint64) *RingBuffer {
	// Size = concurrency * batchSize to ensure we have enough space
	size := concurrency * batchSize
	rb := &RingBuffer{
		buffer:       make([]RingBufEvent, size),
		size:         size,
		batchSize:    batchSize,
		concurrency:  concurrency,
		segments:     make(chan Segment, concurrency),
		ticker:       time.NewTicker(tickInterval),
		tickInterval: tickInterval,
		processor:    processor,
		ackSeg:       make(chan Segment),
		ackCh:        ackCh,
		tracker:      &SegmentTracker{pending: make(map[int64]*Segment)},
	}
	return rb
}

// Add adds a new event to the ring buffer
// Returns false if the buffer is full
func (rb *RingBuffer) Add(event *model.CDCEvent) bool {
	writePos := int(rb.writeIdx.Load()) % rb.size
	readPos := int(rb.readIdx.Load()) % rb.size
	// Check if buffer is full
	nextWritePos := (writePos + 1) % rb.size
	if nextWritePos == readPos {
		return false
	}
	rb.buffer[writePos] = event
	rb.writeIdx.Add(1)
	return true
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
		rb.tracker.Add(&segment)
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
		panic("Last segment position is less than read position")
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
	rb.tracker.Add(&segment)
	rb.segments <- segment
	log.Debug().
		Int64("startIdx", segment.StartIdx).
		Int64("endIdx", segment.EndIdx).
		Int("pendingSegMap", len(rb.tracker.pending)).
		Msg("Created ticker-based segment")
}

// Start starts the ring buffer processing
func (rb *RingBuffer) Start(ctx context.Context, eventsCh <-chan *model.CDCEvent) error {
	// Start the worker pool
	g, ctx := errgroup.WithContext(ctx)

	// Start segment acknowledgment processor
	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case seg := <-rb.ackSeg:
				// Process the acknowledged segment and update readIdx
				rb.handleSegmentAck(seg)
			}
		}
	})

	// Start workers to process segments
	for i := range rb.concurrency {
		workerID := i
		g.Go(func() error {
			return rb.worker(ctx, workerID)
		})
	}

	// Start event receiver
	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-rb.ticker.C:
				rb.createTickerSegment()
			case event := <-eventsCh:
				// Try to add the event to the buffer
				for !rb.Add(event) {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(100 * time.Millisecond):
					}
				}
				// Don't create ticker segment if we create one
				if rb.checkForNewSegment() {
					rb.ticker.Reset(rb.tickInterval)
				}
			}
		}
	})

	// Wait for all goroutines to complete
	return g.Wait()
}

// worker processes segments from the segments channel
func (rb *RingBuffer) worker(ctx context.Context, workerID int) error {
	log.Info().Int("workerID", workerID).Msg("Starting ring buffer worker")

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case segment := <-rb.segments:
			events := make([]*model.CDCEvent, 0, rb.batchSize)
			start := segment.StartIdx
			for start < segment.EndIdx {
				idx := start % int64(rb.size)
				if rb.buffer[idx] != nil {
					events = append(events, rb.buffer[idx])
				}
				start++
			}

			if len(events) > 0 {
				log.Info().
					Int("workerID", workerID).
					Int("eventCount", len(events)).
					Int64("startIdx", segment.StartIdx).
					Int64("endIdx", segment.EndIdx).
					Msg("Processing segment")

				for range 3 {
					err := rb.processor.Process(ctx, events)
					if err == nil {
						break
					}
					time.Sleep(time.Second * 2)
				}
			}
			rb.ackSeg <- segment
		}
	}
}

// handleSegmentAck processes acknowledged segments and updates readIdx
func (rb *RingBuffer) handleSegmentAck(segment Segment) {
	var lastEvent *model.CDCEvent
	rb.tracker.mu.Lock()
	defer rb.tracker.mu.Unlock()

	log.Debug().Any("segment", segment).Msg("Ack segment")

	minSeq := segment.StartIdx
	for seq := range rb.tracker.pending {
		minSeq = min(minSeq, seq)
	}
	if minSeq == segment.StartIdx {
		lastEvent = rb.buffer[(segment.EndIdx)%int64(rb.size)-1]
		log.Debug().
			Any("segment", segment).
			Msg("First segment acknowledged, moving readIdx")
		rb.readIdx.Swap(segment.StartIdx)
	}
	delete(rb.tracker.pending, segment.StartIdx)
	// Send LSN acknowledgment for the last event in the segment
	if lastEvent != nil {
		select {
		case rb.ackCh <- lastEvent.LSN:
			log.Debug().Uint64("lsn", lastEvent.LSN).Msg("Acknowledged LSN after segment completion")
		default:
			log.Warn().Msg("Ack channel is full, could not send LSN after segment completion")
		}
	}

	log.Debug().
		Int64("seq", segment.StartIdx).
		Int("pendingSegments", len(rb.tracker.pending)).
		Msg("Processed segment acknowledgment")
}

// Stop stops the ring buffer processing
func (rb *RingBuffer) Stop() {
	if rb.ticker != nil {
		rb.ticker.Stop()
	}
	// Close the segments channel
	close(rb.segments)
	// Close the ack segment channel
	close(rb.ackSeg)
}
