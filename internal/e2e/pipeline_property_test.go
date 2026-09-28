// Package e2e runs the whole pipeline against a real Postgres: replicator,
// ring, processor, and Parquet writer, with an in-memory S3 that injects
// faults. Set WALCAKE_TEST_PG, and keep the number of checks small:
//
//	WALCAKE_TEST_PG=postgres://postgres@127.0.0.1:5432/postgres \
//	  go test ./internal/e2e -rapid.checks=10 -v
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"
	"pgregory.net/rapid"

	"git.famapp.in/fampay-inc/wal-cake/internal/ack"
	"git.famapp.in/fampay-inc/wal-cake/internal/buffer"
	"git.famapp.in/fampay-inc/wal-cake/internal/config"
	"git.famapp.in/fampay-inc/wal-cake/internal/model"
	"git.famapp.in/fampay-inc/wal-cake/internal/replication"
	"git.famapp.in/fampay-inc/wal-cake/internal/transform"
)

func TestMain(m *testing.M) {
	zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	os.Exit(m.Run())
}

// ---------------------------------------------------------------- fake S3

// bucket is S3: objects survive node crashes.
type bucket struct {
	mu      sync.Mutex
	objects map[string][]byte
	failed  map[string]bool // content identities that already failed once
	rnd     *rand.Rand
}

func newBucket(seed int64) *bucket {
	return &bucket{objects: map[string][]byte{}, failed: map[string]bool{}, rnd: rand.New(rand.NewSource(seed))}
}

func (b *bucket) snapshot() map[string][]byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make(map[string][]byte, len(b.objects))
	for k, v := range b.objects {
		out[k] = v
	}
	return out
}

// nodeUploader is one node's S3 client. freeze() models a crash: uploads that
// have not finished never will.
type nodeUploader struct {
	b      *bucket
	frozen chan struct{}
	once   sync.Once
}

func (u *nodeUploader) freeze() { u.once.Do(func() { close(u.frozen) }) }

func (u *nodeUploader) UploadBytes(_ context.Context, key string, data []byte) error {
	u.b.mu.Lock()
	delay := time.Duration(u.b.rnd.Intn(30)) * time.Millisecond
	// Fail each distinct file body once, at random. The ring retries a
	// segment three times and then exits, so never fail the same body twice.
	id := fmt.Sprintf("%x", data[len(data)/2:])
	fail := !u.b.failed[id] && u.b.rnd.Intn(10) == 0
	if fail {
		u.b.failed[id] = true
	}
	u.b.mu.Unlock()

	select {
	case <-u.frozen:
		select {} // crashed: this upload never completes
	case <-time.After(delay):
	}
	if fail {
		return fmt.Errorf("injected S3 error")
	}
	select {
	case <-u.frozen:
		select {}
	default:
	}
	u.b.mu.Lock()
	u.b.objects[key] = data
	u.b.mu.Unlock()
	return nil
}

// ---------------------------------------------------------------- one node

type node struct {
	up         *nodeUploader
	stopRing   context.CancelFunc
	stopRepl   context.CancelFunc
	ringDone   chan struct{}
	replDone   chan struct{}
	crashed    bool
	stoppedAll bool
}

func startNode(cfg *config.Config, b *bucket) *node {
	n := &node{up: &nodeUploader{b: b, frozen: make(chan struct{})}, ringDone: make(chan struct{}), replDone: make(chan struct{})}
	acked := &ack.Position{}
	events := make(chan *model.CDCEvent, 64)
	w := transform.NewParquetWriter()
	w.AddFilter(func(e *model.CDCEvent) bool { return e.Operation != model.CommitOp }) // as in main
	proc := buffer.NewParquetBatchProcessor(w, n.up, &buffer.BatchProcessorConfig{Namespace: "ns"})
	rb := buffer.NewRingBuffer(7, 3, 200*time.Millisecond, proc, acked)

	ringCtx, stopRing := context.WithCancel(context.Background())
	replCtx, stopRepl := context.WithCancel(context.Background())
	n.stopRing, n.stopRepl = stopRing, stopRepl
	repl := replication.NewPGReplicator(cfg)
	go func() { defer close(n.replDone); repl.Start(replCtx, events, acked) }()
	go func() { defer close(n.ringDone); rb.Start(ringCtx, events) }()
	return n
}

// stop is a graceful shutdown: drain the ring, then send the final ACK.
func (n *node) stop(t *rapid.T) {
	n.stopRing()
	select {
	case <-n.ringDone:
	case <-time.After(30 * time.Second):
		t.Fatal("ring did not drain")
	}
	n.stopRepl()
	select {
	case <-n.replDone:
	case <-time.After(10 * time.Second):
		t.Fatal("replicator did not stop")
	}
	n.stoppedAll = true
}

// crash loses everything in memory: in-flight uploads never finish and no
// further status update reaches Postgres.
func (n *node) crash(t *rapid.T, c *pgx.Conn, slot string) {
	n.up.freeze()
	_, _ = c.Exec(context.Background(), "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = $1 AND active_pid IS NOT NULL", slot)
	n.stopRepl()
	select {
	case <-n.replDone:
	case <-time.After(10 * time.Second):
		t.Fatal("replicator did not stop after crash")
	}
	n.stopRing() // its workers stay blocked on frozen uploads; nothing else runs
	n.crashed = true
}

// ---------------------------------------------------------------- the model

type change struct {
	op  model.Operation
	id  int64
	v   string // after image value; empty for delete
	num string // NUMERIC as text; empty for delete
}

type machine struct {
	cfg     *config.Config
	conn    *pgx.Conn // workload
	conn2   *pgx.Conn // second session for interleaved transactions
	admin   *pgx.Conn
	bucket  *bucket
	node    *node
	nextID  int64
	live    []int64 // ids that currently exist
	want    []change
	rolled  map[int64]bool
	lastLSN string
}

func mustExec(t *rapid.T, c *pgx.Conn, sql string, args ...any) {
	if _, err := c.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

func (m *machine) newRow(t *rapid.T) (int64, string, string) {
	m.nextID++
	v := fmt.Sprintf("v%d-%d", m.nextID, rapid.IntRange(0, 999).Draw(t, "v"))
	// Values past float64 precision catch lossy NUMERIC decoding.
	num := fmt.Sprintf("%d.%02d", rapid.Int64Range(0, 99999999999999999).Draw(t, "int"), rapid.IntRange(0, 99).Draw(t, "frac"))
	return m.nextID, v, num
}

func (m *machine) InsertTx(t *rapid.T) {
	k := rapid.IntRange(1, 20).Draw(t, "rows")
	tx, err := m.conn.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var pending []change
	for range k {
		id, v, num := m.newRow(t)
		if _, err := tx.Exec(context.Background(), "INSERT INTO walcake_e2e VALUES ($1, $2, $3)", id, v, num); err != nil {
			t.Fatal(err)
		}
		pending = append(pending, change{model.InsertOp, id, v, num})
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, c := range pending {
		m.want = append(m.want, c)
		m.live = append(m.live, c.id)
	}
}

// BulkCopy uses COPY, which writes multi-insert WAL records: many rows share one LSN.
func (m *machine) BulkCopy(t *rapid.T) {
	k := rapid.IntRange(1, 300).Draw(t, "copyRows")
	rows := make([][]any, 0, k)
	var pending []change
	for range k {
		id, v, num := m.newRow(t)
		rows = append(rows, []any{id, v, num})
		pending = append(pending, change{model.InsertOp, id, v, num})
	}
	if _, err := m.conn.CopyFrom(context.Background(), pgx.Identifier{"walcake_e2e"}, []string{"id", "v", "n"}, pgx.CopyFromRows(rows)); err != nil {
		t.Fatal(err)
	}
	for _, c := range pending {
		m.want = append(m.want, c)
		m.live = append(m.live, c.id)
	}
}

// BackToBack commits two transactions so the second commit record starts
// where the first one ends.
func (m *machine) BackToBack(t *rapid.T) {
	ctx := context.Background()
	id2, v2, n2 := m.newRow(t)
	id1, v1, n1 := m.newRow(t)
	tx2, err := m.conn2.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx2.Exec(ctx, "INSERT INTO walcake_e2e VALUES ($1, $2, $3)", id2, v2, n2); err != nil {
		t.Fatal(err)
	}
	mustExec(t, m.conn, "INSERT INTO walcake_e2e VALUES ($1, $2, $3)", id1, v1, n1)
	if err := tx2.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	m.want = append(m.want, change{model.InsertOp, id1, v1, n1}, change{model.InsertOp, id2, v2, n2})
	m.live = append(m.live, id1, id2)
}

func (m *machine) Update(t *rapid.T) {
	if len(m.live) == 0 {
		t.Skip("no rows")
	}
	id := rapid.SampledFrom(m.live).Draw(t, "updateID")
	_, v, num := m.newRow(t)
	mustExec(t, m.conn, "UPDATE walcake_e2e SET v = $2, n = $3 WHERE id = $1", id, v, num)
	m.want = append(m.want, change{model.UpdateOp, id, v, num})
}

func (m *machine) Delete(t *rapid.T) {
	if len(m.live) == 0 {
		t.Skip("no rows")
	}
	i := rapid.IntRange(0, len(m.live)-1).Draw(t, "deleteIdx")
	id := m.live[i]
	mustExec(t, m.conn, "DELETE FROM walcake_e2e WHERE id = $1", id)
	m.live = append(m.live[:i], m.live[i+1:]...)
	m.want = append(m.want, change{op: model.DeleteOp, id: id})
}

func (m *machine) Rollback(t *rapid.T) {
	ctx := context.Background()
	tx, err := m.conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	id, v, num := m.newRow(t)
	if _, err := tx.Exec(ctx, "INSERT INTO walcake_e2e VALUES ($1, $2, $3)", id, v, num); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	m.rolled[id] = true
}

func (m *machine) KillWalsender(t *rapid.T) {
	mustExec(t, m.admin, "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = $1 AND active_pid IS NOT NULL", m.cfg.Slot)
}

func (m *machine) Crash(t *rapid.T) {
	m.node.crash(t, m.admin, m.cfg.Slot)
	m.node = startNode(m.cfg, m.bucket)
}

func (m *machine) Restart(t *rapid.T) {
	m.node.stop(t)
	m.node = startNode(m.cfg, m.bucket)
}

func (m *machine) Wait(t *rapid.T) {
	time.Sleep(time.Duration(rapid.IntRange(10, 300).Draw(t, "waitMs")) * time.Millisecond)
}

// Check runs after every action: the slot never moves backwards.
func (m *machine) Check(t *rapid.T) {
	var lsn string
	if err := m.admin.QueryRow(context.Background(),
		"SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1", m.cfg.Slot).Scan(&lsn); err != nil {
		return // slot not created yet
	}
	if m.lastLSN != "" && lsnValue(lsn) < lsnValue(m.lastLSN) {
		t.Fatalf("confirmed_flush_lsn moved backwards: %s -> %s", m.lastLSN, lsn)
	}
	m.lastLSN = lsn
}

func lsnValue(s string) uint64 {
	var hi, lo uint64
	fmt.Sscanf(s, "%X/%X", &hi, &lo)
	return hi<<32 | lo
}

// ---------------------------------------------------------------- decoding

type row struct {
	key        string
	op         string
	before     map[string]any
	after      map[string]any
	commitTime time.Time
}

func decodeObjects(t *rapid.T, objects map[string][]byte) []row {
	var out []row
	for key, data := range objects {
		r, err := file.NewParquetReader(bytes.NewReader(data))
		if err != nil {
			t.Fatalf("%s: %v", key, err)
		}
		n := int(r.NumRows())
		rg := r.RowGroup(0)
		bytesCol := func(i int) ([]parquet.ByteArray, []int16) {
			cr, err := rg.Column(i)
			if err != nil {
				t.Fatal(err)
			}
			vals, defs := make([]parquet.ByteArray, n), make([]int16, n)
			_, _, err = cr.(*file.ByteArrayColumnChunkReader).ReadBatch(int64(n), vals, defs, nil)
			if err != nil {
				t.Fatal(err)
			}
			return vals, defs
		}
		ops, _ := bytesCol(1)
		before, beforeDefs := bytesCol(4)
		after, afterDefs := bytesCol(5)
		cr, err := rg.Column(6)
		if err != nil {
			t.Fatal(err)
		}
		commit := make([]int64, n)
		if _, _, err := cr.(*file.Int64ColumnChunkReader).ReadBatch(int64(n), commit, nil, nil); err != nil {
			t.Fatal(err)
		}
		bi, ai := 0, 0
		for i := range n {
			rw := row{key: key, op: string(ops[i]), commitTime: time.UnixMicro(commit[i]).UTC()}
			if beforeDefs[i] == 1 {
				rw.before = decodeJSON(t, before[bi])
				bi++
			}
			if afterDefs[i] == 1 {
				rw.after = decodeJSON(t, after[ai])
				ai++
			}
			out = append(out, rw)
		}
	}
	return out
}

func decodeJSON(t *rapid.T, b []byte) map[string]any {
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	var m map[string]any
	if err := d.Decode(&m); err != nil {
		t.Fatalf("invalid JSON %q: %v", b, err)
	}
	return m
}

func (r row) id() int64 {
	img := r.after
	if img == nil {
		img = r.before
	}
	n, _ := img["id"].(json.Number).Int64()
	return n
}

// ---------------------------------------------------------------- final check

func (m *machine) verify(t *rapid.T) {
	// Recover: a fresh node streams from the slot until S3 has everything.
	m.node.stop(t)
	m.node = startNode(m.cfg, m.bucket)
	deadline := time.Now().Add(60 * time.Second)
	var missing []change
	var rows []row
	for {
		rows = decodeObjects(t, m.bucket.snapshot())
		missing = missingChanges(m.want, rows)
		if len(missing) == 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	m.node.stop(t)

	if len(missing) > 0 {
		t.Fatalf("%d of %d committed changes never reached S3, e.g. %+v", len(missing), len(m.want), missing[0])
	}
	for _, r := range rows {
		if m.rolled[r.id()] {
			t.Fatalf("rolled-back row %d is in S3 (%s)", r.id(), r.key)
		}
		folder := r.commitTime.Truncate(24 * time.Hour).Format("2006/01/02")
		if !strings.HasPrefix(r.key, "ns/"+folder+"/") {
			t.Fatalf("row committed on %s is in %s", folder, r.key)
		}
	}
}

func missingChanges(want []change, rows []row) []change {
	seen := map[string]bool{}
	for _, r := range rows {
		switch model.Operation(r.op) {
		case model.InsertOp, model.UpdateOp:
			seen[fmt.Sprintf("%s/%d/%v/%v", r.op, r.id(), r.after["v"], r.after["n"])] = true
		case model.DeleteOp:
			seen[fmt.Sprintf("%s/%d", r.op, r.id())] = true
		}
	}
	var missing []change
	for _, c := range want {
		k := fmt.Sprintf("%s/%d/%s/%s", c.op, c.id, c.v, c.num)
		if c.op == model.DeleteOp {
			k = fmt.Sprintf("%s/%d", c.op, c.id)
		}
		if !seen[k] {
			missing = append(missing, c)
		}
	}
	sort.Slice(missing, func(i, j int) bool { return missing[i].id < missing[j].id })
	return missing
}

// ---------------------------------------------------------------- test

func TestPipelineStateMachine(t *testing.T) {
	dsn := os.Getenv("WALCAKE_TEST_PG")
	if dsn == "" {
		t.Skip("WALCAKE_TEST_PG not set")
	}
	run := 0
	rapid.Check(t, func(t *rapid.T) {
		run++
		ctx := context.Background()
		connect := func() *pgx.Conn {
			c, err := pgx.Connect(ctx, dsn)
			if err != nil {
				t.Fatal(err)
			}
			return c
		}
		m := &machine{conn: connect(), conn2: connect(), admin: connect(), bucket: newBucket(int64(run)), rolled: map[int64]bool{}}
		defer m.conn.Close(ctx)
		defer m.conn2.Close(ctx)
		defer m.admin.Close(ctx)
		m.cfg = &config.Config{PGConn: dsn, Slot: "walcake_e2e_slot", Publication: "walcake_e2e_pub", OutputPlugin: "pgoutput"}

		mustExec(t, m.admin, "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = $1 AND active_pid IS NOT NULL", m.cfg.Slot)
		time.Sleep(100 * time.Millisecond)
		mustExec(t, m.admin, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1", m.cfg.Slot)
		mustExec(t, m.admin, "DROP PUBLICATION IF EXISTS walcake_e2e_pub")
		mustExec(t, m.admin, "DROP TABLE IF EXISTS walcake_e2e")
		mustExec(t, m.admin, "CREATE TABLE walcake_e2e (id bigint PRIMARY KEY, v text, n numeric)")
		mustExec(t, m.admin, "CREATE PUBLICATION walcake_e2e_pub FOR TABLE walcake_e2e")

		m.node = startNode(m.cfg, m.bucket)
		// Wait for the slot so the first writes are captured.
		deadline := time.Now().Add(10 * time.Second)
		for {
			var active bool
			err := m.admin.QueryRow(ctx, "SELECT active FROM pg_replication_slots WHERE slot_name = $1", m.cfg.Slot).Scan(&active)
			if err == nil && active {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("slot never became active")
			}
			time.Sleep(50 * time.Millisecond)
		}

		t.Repeat(rapid.StateMachineActions(m))
		m.verify(t)
	})
}
