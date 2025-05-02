package buffer

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

	"git.famapp.in/fampay-inc/wal-cake/internal/model"
)

type RingBufferEvent struct {
	Event *model.CDCEvent
}

// Segment represents a batch of events to be processed together
type Segment struct {
	StartIdx int
	EndIdx   int
}

// BatchProcessor defines the interface for processing batches of events
type BatchProcessor interface {
	Process(ctx context.Context, events []*model.CDCEvent) error
}

// RingBuffer implements a circular buffer optimized for batch processing
type RingBuffer struct {
	buffer       []*RingBufferEvent
	size         int
	writeIdx     atomic.Int64 // Current write position
	readIdx      atomic.Int64 // Current read position
	lastSegIdx   atomic.Int64 // Last segment end position
	batchSize    int
	concurrency  int
	segments     chan Segment
	ticker       *time.Ticker
	tickInterval time.Duration
	processor    BatchProcessor
	ackSeg       chan Segment
	ackCh        chan<- uint64
}

// NewRingBuffer creates a new ring buffer with the specified size
func NewRingBuffer(batchSize, concurrency int, tickInterval time.Duration, processor BatchProcessor, ackCh chan<- uint64) *RingBuffer {
	// Size = concurrency * batchSize to ensure we have enough space
	size := concurrency * batchSize
	rb := &RingBuffer{
		buffer:       make([]*RingBufferEvent, size),
		size:         size,
		batchSize:    batchSize,
		concurrency:  concurrency,
		segments:     make(chan Segment, concurrency),
		ticker:       time.NewTicker(tickInterval),
		tickInterval: tickInterval,
		processor:    processor,
		ackSeg:       make(chan Segment),
		ackCh:        ackCh,
	}

	// Initialize the buffer with empty events
	for i := 0; i < size; i++ {
		rb.buffer[i] = &RingBufferEvent{
			Event: nil,
		}
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

	rb.buffer[writePos] = &RingBufferEvent{
		Event: event,
	}
	rb.writeIdx.Add(1)

	rb.checkForNewSegment()
	return true
}

// checkForNewSegment checks if there are enough events to create a new segment
func (rb *RingBuffer) checkForNewSegment() {
	writePos := int(rb.writeIdx.Load())
	lastSegPos := int(rb.lastSegIdx.Load())

	if writePos-lastSegPos >= rb.batchSize {
		segment := Segment{
			StartIdx: lastSegPos % rb.size,
			EndIdx:   (lastSegPos + rb.batchSize - 1) % rb.size,
		}

		rb.lastSegIdx.Add(int64(rb.batchSize))

		rb.segments <- segment
		log.Debug().
			Int("startIdx", segment.StartIdx).
			Int("endIdx", segment.EndIdx).
			Int("batchSize", rb.batchSize).
			Msg("Created new segment")
	}
}

// createTickerSegment creates a segment based on ticker event
func (rb *RingBuffer) createTickerSegment() {
	writePos := int(rb.writeIdx.Load())
	lastSegPos := int(rb.lastSegIdx.Load())

	// If there are no pending events, do nothing
	if writePos <= lastSegPos {
		return
	}

	// Calculate how many events we have pending
	pendingCount := writePos - lastSegPos

	// Create a segment with all pending events
	segment := Segment{
		StartIdx: lastSegPos % rb.size,
		EndIdx:   (lastSegPos + pendingCount - 1) % rb.size,
	}

	// Update last segment position
	rb.lastSegIdx.Add(int64(pendingCount))

	// Send segment for processing
	rb.segments <- segment
	log.Debug().
		Int("startIdx", segment.StartIdx).
		Int("endIdx", segment.EndIdx).
		Int("pendingCount", pendingCount).
		Msg("Created ticker-based segment")
}

// getMaxProcessedLSN returns the maximum LSN that has been processed
func (rb *RingBuffer) getMaxProcessedLSN() uint64 {
	readPos := int(rb.readIdx.Load())
	return rb.buffer[readPos].Event.LSN
}

// Start starts the ring buffer processing
func (rb *RingBuffer) Start(ctx context.Context, eventsCh <-chan *model.CDCEvent) error {
	// Start the worker pool
	g, ctx := errgroup.WithContext(ctx)
	// Start LSN acknowledgment handler
	g.Go(func() error {
		ackTicker := time.NewTicker(time.Second * 10)
		defer ackTicker.Stop()

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ackTicker.C:
				// Get the maximum processed LSN and send it for acknowledgment
				maxLSN := rb.getMaxProcessedLSN()
				if maxLSN > 0 {
					select {
					case rb.ackCh <- maxLSN:
						log.Debug().Uint64("lsn", maxLSN).Msg("Acknowledged LSN")
					default:
						log.Warn().Msg("Ack channel is full, could not send LSN")
					}
				}
			}
		}
	})

	// Start workers to process segments
	for i := 0; i < rb.concurrency; i++ {
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
					case <-time.After(10 * time.Millisecond):
					}
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
			var count int
			if segment.EndIdx >= segment.StartIdx {
				count = segment.EndIdx - segment.StartIdx + 1
			} else {
				count = rb.size - segment.StartIdx + segment.EndIdx + 1
			}

			for i := 0; i < count; i++ {
				idx := (segment.StartIdx + i) % rb.size
				if rb.buffer[idx] != nil && rb.buffer[idx].Event != nil {
					events = append(events, rb.buffer[idx].Event)
				}
			}

			if len(events) > 0 {
				log.Info().
					Int("workerID", workerID).
					Int("eventCount", len(events)).
					Int("startIdx", segment.StartIdx).
					Int("endIdx", segment.EndIdx).
					Msg("Processing segment")

				err := rb.processor.Process(ctx, events)
				if err != nil {
					log.Fatal().Err(err).Int("workerID", workerID).Msg("Error processing segment")
				}
			}
			rb.ackSeg <- segment
		}
	}
}

// Stop stops the ring buffer processing
func (rb *RingBuffer) Stop() {
	if rb.ticker != nil {
		rb.ticker.Stop()
	}
}
