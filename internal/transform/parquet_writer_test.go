package transform

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/parquet"
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

// Missing row images are null, not empty bytes, and NUMERIC digits survive.
func TestRowImagesAreNullableJSON(t *testing.T) {
	ts := time.Date(2026, 5, 9, 10, 0, 0, 0, time.UTC)
	events := []*model.CDCEvent{
		{Table: "t", Operation: model.InsertOp, Timestamp: ts, LSN: 1, After: map[string]any{"amount": json.Number("9999999999999999.99")}},
		{Table: "t", Operation: model.DeleteOp, Timestamp: ts, LSN: 2, Before: map[string]any{"id": int64(1)}},
	}
	buf, err := NewParquetWriter().WriteToBuffer(events)
	if err != nil {
		t.Fatal(err)
	}
	r, err := file.NewParquetReader(bytes.NewReader(buf))
	if err != nil {
		t.Fatal(err)
	}
	read := func(col int) ([]parquet.ByteArray, []int16) {
		cr, err := r.RowGroup(0).Column(col)
		if err != nil {
			t.Fatal(err)
		}
		vals := make([]parquet.ByteArray, 2)
		defs := make([]int16, 2)
		_, n, err := cr.(*file.ByteArrayColumnChunkReader).ReadBatch(2, vals, defs, nil)
		if err != nil {
			t.Fatal(err)
		}
		return vals[:n], defs
	}

	before, beforeDefs := read(4)
	after, afterDefs := read(5)
	if beforeDefs[0] != 0 || afterDefs[1] != 0 {
		t.Errorf("insert before def=%d, delete after def=%d, want 0 (null)", beforeDefs[0], afterDefs[1])
	}
	if len(before) != 1 || string(before[0]) != `{"id":1}` {
		t.Errorf("before values = %q", before)
	}
	if len(after) != 1 || string(after[0]) != `{"amount":9999999999999999.99}` {
		t.Errorf("after values = %q", after)
	}
}
