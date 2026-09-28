package buffer

import (
	"context"
	"testing"
	"time"

	"pgregory.net/rapid"

	"git.famapp.in/fampay-inc/wal-cake/internal/model"
)

// For any events, Process uploads each event exactly once, in order, one file
// per run of equal UTC commit dates, and each file's folder is that date.
func TestProcessProperty(t *testing.T) {
	zones := []*time.Location{time.UTC, time.FixedZone("IST", 5*3600+1800), time.FixedZone("PST", -8*3600)}
	midnight := time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC)

	rapid.Check(t, func(t *rapid.T) {
		n := rapid.IntRange(1, 50).Draw(t, "n")
		events := make([]*model.CDCEvent, n)
		for i := range events {
			// Commit times within ±2 s of midnight, slightly out of order.
			commit := midnight.Add(time.Duration(rapid.IntRange(-2000, 2000).Draw(t, "commitMs")) * time.Millisecond)
			// Decoding takes about 0.5 µs per event, so neighbours often
			// share a microsecond.
			decoded := midnight.Add(time.Duration(i/rapid.IntRange(1, 3).Draw(t, "perMicro")) * time.Microsecond)
			events[i] = &model.CDCEvent{
				Table:      "t",
				Operation:  model.InsertOp,
				CommitTime: commit.In(rapid.SampledFrom(zones).Draw(t, "commitZone")),
				Timestamp:  decoded.In(rapid.SampledFrom(zones).Draw(t, "decodeZone")),
				LSN:        uint64(i + 1),
			}
		}

		w, u := &recordingWriter{}, &recordingUploader{}
		p := NewParquetBatchProcessor(w, u, &BatchProcessorConfig{Namespace: "ns"})
		if err := p.Process(context.Background(), events); err != nil {
			t.Fatal(err)
		}

		next := 0
		for i, file := range w.files {
			date := file[0].CommitTime.UTC().Truncate(24 * time.Hour)
			for _, ev := range file {
				if ev != events[next] {
					t.Fatalf("file %d: event %d out of order or duplicated", i, ev.LSN)
				}
				next++
				if !ev.CommitTime.UTC().Truncate(24 * time.Hour).Equal(date) {
					t.Fatalf("file %d mixes commit dates", i)
				}
			}
			if want := "ns/" + date.Format("2006/01/02") + "/"; len(u.keys[i]) < len(want) || u.keys[i][:len(want)] != want {
				t.Fatalf("file %d key %s, want folder %s", i, u.keys[i], want)
			}
			for j := range i {
				if u.keys[j] == u.keys[i] {
					t.Fatalf("files %d and %d share key %s: the second overwrites the first", j, i, u.keys[i])
				}
			}
			if i > 0 && file[0].Date().Equal(w.files[i-1][0].Date()) {
				t.Fatalf("files %d and %d have the same date but were not merged", i-1, i)
			}
		}
		if next != n {
			t.Fatalf("uploaded %d of %d events", next, n)
		}
	})
}
