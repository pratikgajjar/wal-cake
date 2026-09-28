package buffer

import (
	"context"
	"testing"
	"time"

	"git.famapp.in/fampay-inc/wal-cake/internal/model"
	"git.famapp.in/fampay-inc/wal-cake/internal/transform"
)

// recordingWriter records the events of each file instead of encoding them.
type recordingWriter struct{ files [][]*model.CDCEvent }

func (w *recordingWriter) WriteToBuffer(events []*model.CDCEvent) ([]byte, error) {
	w.files = append(w.files, events)
	return []byte{1}, nil
}
func (w *recordingWriter) GetCompressionCodec() string            { return "ZSTD" }
func (w *recordingWriter) AddFilter(filter transform.EventFilter) {}

type recordingUploader struct{ keys []string }

func (u *recordingUploader) UploadBytes(_ context.Context, key string, _ []byte) error {
	u.keys = append(u.keys, key)
	return nil
}

func TestProcessSplitsByDate(t *testing.T) {
	day1 := time.Date(2026, 5, 9, 23, 59, 59, 0, time.UTC)
	day2 := day1.Add(2 * time.Second)
	day3 := day2.Add(24 * time.Hour)

	tests := []struct {
		name  string
		days  []time.Time // one timestamp per event
		files []int       // expected events per uploaded file
	}{
		{"same day", []time.Time{day1, day1, day1, day1}, []int{4}},
		{"day changes after first event", []time.Time{day1, day2, day2, day2}, []int{1, 3}},
		{"day changes in the middle", []time.Time{day1, day1, day1, day2}, []int{3, 1}},
		{"three days", []time.Time{day1, day2, day2, day3, day3}, []int{1, 2, 2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			events := make([]*model.CDCEvent, len(tt.days))
			for i, ts := range tt.days {
				events[i] = &model.CDCEvent{Table: "t", Operation: model.InsertOp, Timestamp: ts, LSN: uint64(i + 1)}
			}
			w, u := &recordingWriter{}, &recordingUploader{}
			p := NewParquetBatchProcessor(w, u, &BatchProcessorConfig{Namespace: "ns"})

			if err := p.Process(context.Background(), events); err != nil {
				t.Fatalf("Process: %v", err)
			}

			if len(w.files) != len(tt.files) {
				t.Fatalf("got %d files, want %d", len(w.files), len(tt.files))
			}
			total := 0
			for i, f := range w.files {
				if len(f) != tt.files[i] {
					t.Errorf("file %d has %d events, want %d", i, len(f), tt.files[i])
				}
				for _, e := range f[1:] {
					if !e.Date().Equal(f[0].Date()) {
						t.Errorf("file %d mixes dates %v and %v", i, f[0].Date(), e.Date())
					}
				}
				total += len(f)
			}
			if total != len(events) {
				t.Errorf("uploaded %d events, want %d", total, len(events))
			}
		})
	}
}
