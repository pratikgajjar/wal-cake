package replication

// Integration tests against a real Postgres with wal_level=logical.
// Set WALCAKE_TEST_PG to a connection URL without query parameters, e.g.
//
//	WALCAKE_TEST_PG=postgres://postgres@127.0.0.1:5432/postgres go test ./internal/replication

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"git.famapp.in/fampay-inc/wal-cake/internal/ack"
	"git.famapp.in/fampay-inc/wal-cake/internal/config"
	"git.famapp.in/fampay-inc/wal-cake/internal/model"
)

func testDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("WALCAKE_TEST_PG")
	if dsn == "" {
		t.Skip("WALCAKE_TEST_PG not set")
	}
	return dsn
}

func mustExec(t *testing.T, c *pgx.Conn, sql string) {
	t.Helper()
	if _, err := c.Exec(context.Background(), sql); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

// setup creates a fresh table, publication, and no slot. Start creates the slot.
func setup(t *testing.T, dsn, name string) (*pgx.Conn, *config.Config) {
	t.Helper()
	ctx := context.Background()
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close(ctx) })
	mustExec(t, c, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s_pub", name))
	mustExec(t, c, fmt.Sprintf("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '%s_slot'", name))
	mustExec(t, c, fmt.Sprintf("DROP TABLE IF EXISTS %s", name))
	mustExec(t, c, fmt.Sprintf("CREATE TABLE %s (v text)", name))
	mustExec(t, c, fmt.Sprintf("CREATE PUBLICATION %s_pub FOR TABLE %s", name, name))
	t.Cleanup(func() {
		c.Exec(ctx, fmt.Sprintf("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '%s_slot' AND NOT active", name))
	})
	return c, &config.Config{PGConn: dsn, Slot: name + "_slot", Publication: name + "_pub", OutputPlugin: "pgoutput"}
}

type running struct {
	repl   PGReplicator
	events chan *model.CDCEvent
	acked  *ack.Position
	cancel context.CancelFunc
	done   chan error
	once   sync.Once
}

func start(t *testing.T, cfg *config.Config) *running {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	r := &running{repl: NewPGReplicator(cfg), events: make(chan *model.CDCEvent, 100), acked: &ack.Position{}, cancel: cancel, done: make(chan error, 1)}
	go func() { r.done <- r.repl.Start(ctx, r.events, r.acked) }()
	t.Cleanup(r.stop)
	return r
}

func (r *running) stop() {
	r.once.Do(func() {
		r.cancel()
		select {
		case <-r.done:
		case <-time.After(10 * time.Second):
		}
	})
}

func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func slotField(t *testing.T, c *pgx.Conn, slot, field string) string {
	var v string
	_ = c.QueryRow(context.Background(), fmt.Sprintf("SELECT coalesce(%s::text, '') FROM pg_replication_slots WHERE slot_name = $1", field), slot).Scan(&v)
	return v
}

// next returns the next non-commit event, or fails after timeout.
func next(t *testing.T, ch <-chan *model.CDCEvent, timeout time.Duration) *model.CDCEvent {
	t.Helper()
	select {
	case ev := <-ch:
		return ev
	case <-time.After(timeout):
		return nil
	}
}

// TestACKAfterCommitDoesNotSkipNextTransaction commits T1 and T2 back to back, so
// T2's commit record starts where T1's ends. Confirming T1's commit event must
// not confirm T2: after a restart, Postgres must send T2 again.
func TestACKAfterCommitDoesNotSkipNextTransaction(t *testing.T) {
	dsn := testDSN(t)
	ctx := context.Background()
	c, cfg := setup(t, dsn, "walcake_ack")
	c2, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close(ctx)

	r1 := start(t, cfg)
	waitFor(t, "slot active", 10*time.Second, func() bool { return slotField(t, c, cfg.Slot, "active") == "true" })

	tx2, err := c2.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx2.Exec(ctx, "INSERT INTO walcake_ack VALUES ('T2')"); err != nil {
		t.Fatal(err)
	}
	mustExec(t, c, "INSERT INTO walcake_ack VALUES ('T1')") // T1 commits first
	if err := tx2.Commit(ctx); err != nil {                 // T2 commits right after
		t.Fatal(err)
	}

	// Stream order: T1 insert, T1 commit, T2 insert, T2 commit.
	var t1Commit *model.CDCEvent
	for i := 0; i < 4; i++ {
		ev := next(t, r1.events, 10*time.Second)
		if ev == nil {
			t.Fatal("missing events in the first run")
		}
		if i == 1 {
			if ev.Operation != model.CommitOp {
				t.Fatalf("event 1 is %s, want the T1 commit", ev.Operation)
			}
			t1Commit = ev
		}
	}

	// Confirm T1 only, as the ring does when T1 is uploaded and T2 is not.
	r1.acked.Advance(t1Commit.LSN)
	want := model.LSNStr(t1Commit.LSN)
	waitFor(t, "slot to confirm T1", 15*time.Second, func() bool { return slotField(t, c, cfg.Slot, "confirmed_flush_lsn") == want })
	r1.stop() // crash before T2 is uploaded

	r2 := start(t, cfg)
	for {
		ev := next(t, r2.events, 10*time.Second)
		if ev == nil {
			t.Fatal("T2 was not sent again after restart: the ACK for T1 skipped it")
		}
		if ev.Operation == model.InsertOp && ev.After["v"] == "T2" {
			return
		}
	}
}

// TestReconnectsAfterConnectionLoss terminates the walsender and expects the
// replicator to reconnect from the slot and deliver rows written after the loss.
func TestReconnectsAfterConnectionLoss(t *testing.T) {
	dsn := testDSN(t)
	c, cfg := setup(t, dsn, "walcake_reconnect")
	r := start(t, cfg)
	waitFor(t, "slot active", 10*time.Second, func() bool { return slotField(t, c, cfg.Slot, "active") == "true" })
	waitFor(t, "ready", 10*time.Second, func() bool { return r.repl.HealthCheck(context.Background()) == nil })

	mustExec(t, c, "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = 'walcake_reconnect_slot'")
	waitFor(t, "not ready after the connection is lost", 10*time.Second, func() bool { return r.repl.HealthCheck(context.Background()) != nil })

	mustExec(t, c, "INSERT INTO walcake_reconnect VALUES ('after-loss')")
	for {
		ev := next(t, r.events, 20*time.Second)
		if ev == nil {
			t.Fatal("no event after the connection was lost: the replicator did not reconnect")
		}
		if ev.Operation == model.InsertOp && ev.After["v"] == "after-loss" {
			break
		}
	}
	if err := r.repl.HealthCheck(context.Background()); err != nil {
		t.Fatalf("not ready after reconnect: %v", err)
	}
}

// TestStartLSNFailsClosed checks that a failed slot query is an error, not a
// silent start from the current WAL position.
func TestStartLSNFailsClosed(t *testing.T) {
	dsn := testDSN(t)
	ctx := context.Background()
	_, cfg := setup(t, dsn, "walcake_startlsn")
	r := NewPGReplicator(cfg).(*pgReplicator)
	var err error
	if r.queryConn, err = pgx.Connect(ctx, dsn); err != nil {
		t.Fatal(err)
	}
	r.queryConn.Close(ctx) // the slot query will fail
	if lsn, err := r.getStartLSN(ctx); err == nil {
		t.Fatalf("getStartLSN returned %s and no error", lsn)
	}
}

// TestShutdownConfirmsFinalPosition checks that stopping the replicator
// confirms the latest acked position before it closes the connection.
func TestShutdownConfirmsFinalPosition(t *testing.T) {
	dsn := testDSN(t)
	c, cfg := setup(t, dsn, "walcake_shutdown")
	r := start(t, cfg)
	waitFor(t, "slot active", 10*time.Second, func() bool { return slotField(t, c, cfg.Slot, "active") == "true" })

	mustExec(t, c, "INSERT INTO walcake_shutdown VALUES ('x')")
	var commit *model.CDCEvent
	for commit == nil {
		ev := next(t, r.events, 10*time.Second)
		if ev == nil {
			t.Fatal("no commit event")
		}
		if ev.CommitTime.IsZero() {
			t.Fatalf("%s event has no commit time", ev.Operation)
		}
		if ev.Operation == model.CommitOp {
			commit = ev
		}
	}
	r.acked.Advance(commit.LSN)
	r.stop() // immediately, before any periodic update

	if got, want := slotField(t, c, cfg.Slot, "confirmed_flush_lsn"), model.LSNStr(commit.LSN); got != want {
		t.Fatalf("confirmed_flush_lsn = %s after shutdown, want %s", got, want)
	}
}
