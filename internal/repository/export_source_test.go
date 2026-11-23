package repository

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

// Requiere TEST_POSTGRES_DSN. Usa un tenant único y restaura export_state al terminar.
func TestPostgresExportSource(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN no definido")
	}
	ctx := context.Background()
	repo, err := NewPostgresRepository(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()
	src := repo.ExportSource()

	var savedLast int64
	var savedPending *int64
	if err := repo.pool.QueryRow(ctx, `SELECT last_id, pending_to FROM export_state WHERE name='parquet'`).Scan(&savedLast, &savedPending); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = repo.pool.Exec(ctx, `UPDATE export_state SET last_id=$1, pending_to=$2 WHERE name='parquet'`, savedLast, savedPending)
	})

	tenant := fmt.Sprintf("exp-%d", time.Now().UnixNano())
	for i := 0; i < 5; i++ {
		if _, err := repo.pool.Exec(ctx,
			`INSERT INTO events (event_id, tenant_id, event_type, user_id, payload) VALUES ($1,$2,'click','u',$3)`,
			fmt.Sprintf("x%d", i), tenant, map[string]any{"i": i}); err != nil {
			t.Fatal(err)
		}
	}
	// Una fila sin payload (NULL) debe volver como cadena vacía
	if _, err := repo.pool.Exec(ctx, `INSERT INTO events (event_id, tenant_id) VALUES ('nullpayload', $1)`, tenant); err != nil {
		t.Fatal(err)
	}
	var maxID int64
	_ = repo.pool.QueryRow(ctx, `SELECT max(id) FROM events WHERE tenant_id=$1`, tenant).Scan(&maxID)
	base := maxID - 6 // las 6 filas son consecutivas salvo concurrencia ajena: se filtra por tenant abajo

	// Estado inicial controlado
	if _, err := repo.pool.Exec(ctx, `UPDATE export_state SET last_id=$1, pending_to=NULL WHERE name='parquet'`, base); err != nil {
		t.Fatal(err)
	}

	// Lag: con before en el pasado no hay nada elegible
	if _, n, err := src.Plan(ctx, base, time.Now().Add(-time.Hour), 100); err != nil || n != 0 {
		t.Fatalf("Plan con lag: n=%d err=%v", n, err)
	}
	upTo, n, err := src.Plan(ctx, base, time.Now().Add(time.Minute), 100)
	if err != nil || n < 6 || upTo < maxID {
		t.Fatalf("Plan: upTo=%d n=%d err=%v", upTo, n, err)
	}
	if _, n, _ := src.Plan(ctx, base, time.Now().Add(time.Minute), 2); n != 2 {
		t.Fatalf("Plan respeta limit: n=%d", n)
	}

	if err := src.MarkPending(ctx, upTo); err != nil {
		t.Fatal(err)
	}
	if err := src.MarkPending(ctx, upTo); err == nil {
		t.Fatal("un segundo MarkPending con lote en curso debe fallar")
	}
	st, _ := src.State(ctx)
	if st.LastID != base || st.PendingTo != upTo {
		t.Fatalf("State = %+v", st)
	}

	recs, err := src.Read(ctx, base, upTo)
	if err != nil {
		t.Fatal(err)
	}
	mine, nulls := 0, 0
	for i, r := range recs {
		if i > 0 && r.ID <= recs[i-1].ID {
			t.Fatal("Read debe ordenar por id")
		}
		if r.TenantID == tenant {
			mine++
			if r.Payload == "" {
				nulls++
			}
		}
	}
	if mine != 6 || nulls != 1 {
		t.Fatalf("filas propias=%d (want 6), sin payload=%d (want 1)", mine, nulls)
	}

	if err := src.Commit(ctx, upTo+1); err == nil {
		t.Fatal("Commit con un rango distinto al pendiente debe fallar")
	}
	if err := src.Commit(ctx, upTo); err != nil {
		t.Fatal(err)
	}
	if st, _ := src.State(ctx); st.LastID != upTo || st.PendingTo != 0 {
		t.Fatalf("tras Commit: %+v", st)
	}
}

func TestPostgresExportLockIsExclusive(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN no definido")
	}
	ctx := context.Background()
	repo, err := NewPostgresRepository(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()
	other, err := NewPostgresRepository(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()

	release, ok, err := repo.ExportSource().TryLock(ctx)
	if err != nil || !ok {
		t.Fatalf("primer lock: ok=%v err=%v", ok, err)
	}
	if _, ok, _ := other.ExportSource().TryLock(ctx); ok {
		t.Fatal("otra réplica no debe obtener el lock")
	}
	release()
	rel2, ok, err := other.ExportSource().TryLock(ctx)
	if err != nil || !ok {
		t.Fatalf("tras liberar debe poder tomarse: ok=%v err=%v", ok, err)
	}
	rel2()
}

// Una transacción abierta con un id menor no debe quedar atrás cuando otra posterior confirma.
func TestPostgresExportPlanWaitsForInFlightWriter(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN no definido")
	}
	ctx := context.Background()
	repo, err := NewPostgresRepository(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()
	src := repo.ExportSource()
	tenant := fmt.Sprintf("inflight-%d", time.Now().UnixNano())

	var base int64
	_ = repo.pool.QueryRow(ctx, `SELECT COALESCE(max(id), 0) FROM events`).Scan(&base)

	open, err := repo.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer open.Rollback(ctx)
	if _, err := open.Exec(ctx, `INSERT INTO events (event_id, tenant_id, event_type, user_id) VALUES ('slow', $1, 'click', 'u')`, tenant); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.pool.Exec(ctx, `INSERT INTO events (event_id, tenant_id, event_type, user_id) VALUES ('fast', $1, 'click', 'u')`, tenant); err != nil {
		t.Fatal(err)
	}

	upTo, n, err := src.Plan(ctx, base, time.Now().Add(time.Minute), 100)
	if err != nil {
		t.Fatal(err)
	}
	var fastID int64
	_ = repo.pool.QueryRow(ctx, `SELECT id FROM events WHERE event_id='fast' AND tenant_id=$1`, tenant).Scan(&fastID)
	if n != 0 && upTo >= fastID {
		t.Fatalf("Plan exportó id %d con una transacción anterior sin confirmar (upTo=%d)", fastID, upTo)
	}

	if err := open.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, n, err := src.Plan(ctx, base, time.Now().Add(time.Minute), 100); err != nil || n < 2 {
		t.Fatalf("tras confirmar deben ser elegibles ambas filas: n=%d err=%v", n, err)
	}
}
