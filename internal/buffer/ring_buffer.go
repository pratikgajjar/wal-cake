package buffer

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/alphadose/haxmap"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

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
	ackCh        chan<- uint64
	tracker      *haxmap.Map[int64, *Segment] // Tracks completed segments
}

// NewRingBuffer creates a new ring buffer with the specified size
func NewRingBuffer(batchSize, concurrency int, tickInterval time.Duration, processor BatchProcessor, ackCh chan<- uint64) *RingBuffer {
	// Size = concurrency * batchSize to ensure we have enough space
	size := concurrency * batchSize

	rb := &RingBuffer{
		buffer:       make([]RingBufEvent, size),
		size:         int64(size),
		batchSize:    batchSize,
		concurrency:  concurrency,
		segments:     make(chan Segment, concurrency),
		ticker:       time.NewTicker(tickInterval),
		tickInterval: tickInterval,
		processor:    processor,
		ackSeg:       make(chan Segment, concurrency), // Buffered to prevent deadlock during shutdown
		ackCh:        ackCh,
		tracker:      haxmap.New[int64, *Segment](),
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
		Int("pendingSegMap", int(rb.tracker.Len())).
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
				log.Debug().Any("event", event).Msg("RX:eventsCh")
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
				idx := start % rb.size
				if rb.buffer[idx] != nil {
					events = append(events, rb.buffer[idx])
				}
				start++
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
						log.Fatal().Err(err).Msg("Failed to process segment")
					}
					time.Sleep(time.Second * (2 << i))
				}
			}
			rb.ackSeg <- segment
		}
	}
}

// handleSegmentAck processes acknowledged segments and updates readIdx
func (rb *RingBuffer) handleSegmentAck(segment Segment) {
	previousReadIdx := rb.readIdx.Load()
	highContiguous := rb.findHighestContiguous(segment.StartIdx, previousReadIdx)
	log.Debug().
		Any("segment", segment).
		Int64("prevRead", previousReadIdx).
		Int64("highCont", highContiguous).
		Msg("Ack segment")
	if highContiguous > previousReadIdx {
		rb.readIdx.Store(highContiguous)
		idx := (highContiguous - 1) % rb.size
		lastEvent := rb.buffer[idx]
		if lastEvent != nil {
			log.Debug().
				Uint64("lsn", lastEvent.LSN).
				Msg("Advancing contiguous position")

			select {
			case rb.ackCh <- lastEvent.LSN:
				log.Debug().Uint64("lsn", lastEvent.LSN).Msg("Acknowledged LSN after contiguous advancement")
			default:
				log.Warn().Msg("Ack channel is full, could not send LSN after contiguous advancement")
			}
		}
	}
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
