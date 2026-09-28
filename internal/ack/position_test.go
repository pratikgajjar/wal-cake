package ack

import (
	"sync"
	"testing"
)

func TestPositionOnlyMovesForward(t *testing.T) {
	var p Position
	p.Advance(10)
	p.Advance(5)
	if got := p.Load(); got != 10 {
		t.Fatalf("Load = %d, want 10", got)
	}
	p.Advance(12)
	if got := p.Load(); got != 12 {
		t.Fatalf("Load = %d, want 12", got)
	}
}

func TestPositionConcurrentAdvance(t *testing.T) {
	var p Position
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				p.Advance(uint64(i*8 + g))
			}
		}(g)
	}
	wg.Wait()
	if got := p.Load(); got != 999*8+7 {
		t.Fatalf("Load = %d, want %d", got, 999*8+7)
	}
}
