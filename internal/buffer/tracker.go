package buffer

import "sync"

// segmentTracker maps a segment's StartIdx to the segment. The receiver adds
// segments; the walker reads and deletes them. It holds only the segments
// between readIdx and lastSegIdx, and sees a few operations per segment, so a
// mutex costs nothing measurable.
//
// It replaced haxmap, whose Get can miss a present key while another
// goroutine calls Set. One miss stopped the walk for good: readIdx never
// moved again, the ring filled, and replication stalled.
type segmentTracker struct {
	mu sync.Mutex
	m  map[int64]*Segment
}

func newSegmentTracker() *segmentTracker {
	return &segmentTracker{m: make(map[int64]*Segment)}
}

func (t *segmentTracker) Set(start int64, s *Segment) {
	t.mu.Lock()
	t.m[start] = s
	t.mu.Unlock()
}

func (t *segmentTracker) Get(start int64) (*Segment, bool) {
	t.mu.Lock()
	s, ok := t.m[start]
	t.mu.Unlock()
	return s, ok
}

func (t *segmentTracker) Del(start int64) {
	t.mu.Lock()
	delete(t.m, start)
	t.mu.Unlock()
}

func (t *segmentTracker) Len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.m)
}
