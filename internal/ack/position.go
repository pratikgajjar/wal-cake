// Package ack holds the position that is safe to confirm to the replication slot.
package ack

import "sync/atomic"

// Position is the highest LSN whose events, and every event before them, are
// durable in S3. The ring buffer advances it; the replicator reads it when it
// sends a status update. It only moves forward, and a newer value is never
// dropped, unlike a bounded channel.
type Position struct {
	lsn atomic.Uint64
}

// Advance raises the position to lsn. Lower values are ignored.
func (p *Position) Advance(lsn uint64) {
	for {
		cur := p.lsn.Load()
		if lsn <= cur || p.lsn.CompareAndSwap(cur, lsn) {
			return
		}
	}
}

// Load returns the current position.
func (p *Position) Load() uint64 {
	return p.lsn.Load()
}
