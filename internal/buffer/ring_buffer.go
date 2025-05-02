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

// EventStatus represents the processing status of an event in the ring buffer
type EventStatus uint32

const (
	// StatusPending indicates the event is waiting to be processed
	StatusPending EventStatus = iota
	// StatusProcessing indicates the event is currently being processed
	StatusProcessing
	// StatusProcessed indicates the event has been successfully processed
	StatusProcessed
)

// RingBufferEvent wraps a CDC event with its processing status
type RingBufferEvent struct {
	Event  *model.CDCEvent
	Status atomic.Uint32 // Using atomic operations to avoid locks
}

// SetStatus sets the status of the event atomically
func (e *RingBufferEvent) SetStatus(status EventStatus) {
	e.Status.Store(uint32(status))
}

// GetStatus gets the current status of the event atomically
func (e *RingBufferEvent) GetStatus() EventStatus {
	return EventStatus(e.Status.Load())
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
	batchSize    int
	concurrency  int
	segments     chan Segment
	segmentsLock sync.Mutex
	ticker       *time.Ticker
	tickInterval time.Duration
	processor    BatchProcessor
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
		segments:     make(chan Segment, concurrency), // Buffer channel to avoid blocking
		ticker:       time.NewTicker(tickInterval),
		tickInterval: tickInterval,
		processor:    processor,
		ackCh:        ackCh,
	}
	
	// Initialize the buffer with empty events
	for i := 0; i < size; i++ {
		rb.buffer[i] = &RingBufferEvent{
			Event: nil,
		}
		rb.buffer[i].SetStatus(StatusPending)
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
	
	// Add the event to the buffer
	rb.buffer[writePos] = &RingBufferEvent{
		Event: event,
	}
	rb.buffer[writePos].SetStatus(StatusPending)
	
	// Update write position
	rb.writeIdx.Add(1)
	
	// Check if we have enough events to create a new segment
	rb.checkForNewSegment()
	
	return true
}

// checkForNewSegment checks if there are enough events to create a new segment
func (rb *RingBuffer) checkForNewSegment() {
	rb.segmentsLock.Lock()
	defer rb.segmentsLock.Unlock()
	
	writePos := int(rb.writeIdx.Load())
	readPos := int(rb.readIdx.Load())
	
	// If we have at least batchSize events, create a new segment
	if writePos-readPos >= rb.batchSize {
		segment := Segment{
			StartIdx: readPos % rb.size,
			EndIdx:   (readPos + rb.batchSize - 1) % rb.size,
		}
		
		// Mark events in this segment as processing
		for i := 0; i < rb.batchSize; i++ {
			idx := (readPos + i) % rb.size
			rb.buffer[idx].SetStatus(StatusProcessing)
		}
		
		// Update read position
		rb.readIdx.Add(int64(rb.batchSize))
		
		// Send segment for processing
		select {
		case rb.segments <- segment:
			log.Debug().
				Int("startIdx", segment.StartIdx).
				Int("endIdx", segment.EndIdx).
				Int("batchSize", rb.batchSize).
				Msg("Created new segment")
		default:
			// If channel is full, we'll try again later
			// This shouldn't happen with proper concurrency settings
			log.Warn().Msg("Segment channel is full, could not add new segment")
			// Roll back the read position
			rb.readIdx.Add(-int64(rb.batchSize))
			// Reset event status
			for i := 0; i < rb.batchSize; i++ {
				idx := (readPos + i) % rb.size
				rb.buffer[idx].SetStatus(StatusPending)
			}
		}
	}
}

// createTickerSegment creates a segment based on ticker event
func (rb *RingBuffer) createTickerSegment() {
	rb.segmentsLock.Lock()
	defer rb.segmentsLock.Unlock()
	
	writePos := int(rb.writeIdx.Load())
	readPos := int(rb.readIdx.Load())
	
	// If there are no pending events, do nothing
	if writePos <= readPos {
		return
	}
	
	// Calculate how many events we have pending
	pendingCount := writePos - readPos
	
	// Create a segment with all pending events
	segment := Segment{
		StartIdx: readPos % rb.size,
		EndIdx:   (readPos + pendingCount - 1) % rb.size,
	}
	
	// Mark events in this segment as processing
	for i := 0; i < pendingCount; i++ {
		idx := (readPos + i) % rb.size
		rb.buffer[idx].SetStatus(StatusProcessing)
	}
	
	// Update read position
	rb.readIdx.Add(int64(pendingCount))
	
	// Send segment for processing
	select {
	case rb.segments <- segment:
		log.Debug().
			Int("startIdx", segment.StartIdx).
			Int("endIdx", segment.EndIdx).
			Int("pendingCount", pendingCount).
			Msg("Created ticker-based segment")
	default:
		// If channel is full, we'll try again later
		log.Warn().Msg("Segment channel is full, could not add ticker segment")
		// Roll back the read position
		rb.readIdx.Add(-int64(pendingCount))
		// Reset event status
		for i := 0; i < pendingCount; i++ {
			idx := (readPos + i) % rb.size
			rb.buffer[idx].SetStatus(StatusPending)
		}
	}
}

// getMaxProcessedLSN returns the maximum LSN that has been processed
// It finds the highest contiguous LSN where all events have been processed
func (rb *RingBuffer) getMaxProcessedLSN() uint64 {
	var maxLSN uint64
	
	// Start from the beginning of the buffer
	readPos := int(rb.readIdx.Load()) - rb.size // Go back to potentially find processed events
	if readPos < 0 {
		readPos = 0
	}
	
	writePos := int(rb.writeIdx.Load())
	
	// Find the last contiguous processed event
	for i := readPos; i < writePos; i++ {
		idx := i % rb.size
		if rb.buffer[idx] != nil && rb.buffer[idx].Event != nil {
			if rb.buffer[idx].GetStatus() == StatusProcessed {
				// Update max LSN if this event has a higher LSN
				if rb.buffer[idx].Event.LSN > maxLSN {
					maxLSN = rb.buffer[idx].Event.LSN
				}
			} else {
				// Found a non-processed event, stop here
				break
			}
		}
	}
	
	return maxLSN
}

// Start starts the ring buffer processing
func (rb *RingBuffer) Start(ctx context.Context, eventsCh <-chan *model.CDCEvent) error {
	// Start the worker pool
	g, ctx := errgroup.WithContext(ctx)
	
	// Start ticker handler
	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-rb.ticker.C:
				rb.createTickerSegment()
			}
		}
	})
	
	// Start LSN acknowledgment handler
	g.Go(func() error {
		ackTicker := time.NewTicker(time.Second)
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
			case event := <-eventsCh:
				// Try to add the event to the buffer
				for !rb.Add(event) {
					// If buffer is full, wait a bit and try again
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(10 * time.Millisecond):
						// Try again
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
			// Extract events for this segment
			events := make([]*model.CDCEvent, 0, rb.batchSize)
			
			// Calculate the actual number of events in this segment
			var count int
			if segment.EndIdx >= segment.StartIdx {
				count = segment.EndIdx - segment.StartIdx + 1
			} else {
				// Handle wrap-around case
				count = rb.size - segment.StartIdx + segment.EndIdx + 1
			}
			
			// Collect events
			for i := 0; i < count; i++ {
				idx := (segment.StartIdx + i) % rb.size
				if rb.buffer[idx] != nil && rb.buffer[idx].Event != nil {
					events = append(events, rb.buffer[idx].Event)
				}
			}
			
			// Process the batch
			if len(events) > 0 {
				log.Info().
					Int("workerID", workerID).
					Int("eventCount", len(events)).
					Int("startIdx", segment.StartIdx).
					Int("endIdx", segment.EndIdx).
					Msg("Processing segment")
				
				err := rb.processor.Process(ctx, events)
				
				// Mark events as processed or reset them based on the result
				for i := 0; i < count; i++ {
					idx := (segment.StartIdx + i) % rb.size
					if rb.buffer[idx] != nil {
						if err == nil {
							rb.buffer[idx].SetStatus(StatusProcessed)
						} else {
							// On error, reset to pending so it can be retried
							rb.buffer[idx].SetStatus(StatusPending)
							log.Error().Err(err).Int("workerID", workerID).Msg("Error processing segment")
						}
					}
				}
			}
		}
	}
}

// Stop stops the ring buffer processing
func (rb *RingBuffer) Stop() {
	if rb.ticker != nil {
		rb.ticker.Stop()
	}
}
