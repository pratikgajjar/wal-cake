package transform

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"

	"git.famapp.in/fampay-inc/wal-cake/internal/model"
)

// fixture returns n events across 12 tables: a third each inserts, updates, and deletes.
func fixture(n int) []*model.CDCEvent {
	base := time.Date(2026, 5, 9, 10, 0, 0, 0, time.UTC)
	events := make([]*model.CDCEvent, 0, n)
	for i := range n {
		row := map[string]any{
			"id": int64(i), "user_id": int64(i * 7919 % 100000), "status": []string{"pending", "success", "failed"}[i%3],
			"note": fmt.Sprintf("payment ref %08d", i*104729%100000000), "created_at": base.Add(time.Duration(i) * time.Millisecond).Format(time.RFC3339Nano),
		}
		ev := &model.CDCEvent{Table: fmt.Sprintf("table_%02d", i%12), Timestamp: base.Add(time.Duration(i) * time.Millisecond), LSN: uint64(0x1000000 + i*120)}
		switch i % 3 {
		case 0:
			ev.Operation, ev.After = model.InsertOp, row
		case 1:
			ev.Operation, ev.Before, ev.After = model.UpdateOp, map[string]any{"id": int64(i), "status": "pending"}, row
		case 2:
			ev.Operation, ev.Before = model.DeleteOp, row
		}
		events = append(events, ev)
	}
	return events
}

// The pooled codec must write exactly the bytes arrow-go's own codec writes.
func TestPooledZstdMatchesDefaultCodec(t *testing.T) {
	events := fixture(1000)
	w := NewParquetWriter() // registers the pooled codec
	pooled, err := w.WriteToBuffer(events)
	if err != nil {
		t.Fatal(err)
	}

	current, _ := compress.GetCodec(compress.Codecs.Zstd)
	compress.RegisterCodec(compress.Codecs.Zstd, current.(*pooledZstd).Codec)
	defer compress.RegisterCodec(compress.Codecs.Zstd, current)
	original, err := w.WriteToBuffer(events)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(pooled, original) {
		t.Fatalf("pooled output (%d bytes) differs from default codec output (%d bytes)", len(pooled), len(original))
	}
	r, err := file.NewParquetReader(bytes.NewReader(pooled))
	if err != nil {
		t.Fatal(err)
	}
	if got := r.NumRows(); got != 1000 {
		t.Fatalf("file has %d rows, want 1000", got)
	}
}

func BenchmarkWriteToBuffer1000(b *testing.B) {
	w := NewParquetWriter()
	events := fixture(1000)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := w.WriteToBuffer(events); err != nil {
			b.Fatal(err)
		}
	}
}
