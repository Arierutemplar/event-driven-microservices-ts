package export

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/Arierutemplar/event-driven-microservices-ts/internal/domain"
)

type fakeSource struct {
	mu       sync.Mutex
	rows     []domain.ExportRecord
	st       domain.ExportState
	lockBusy bool
	commits  int
}

func (f *fakeSource) TryLock(context.Context) (func(), bool, error) {
	if f.lockBusy {
		return nil, false, nil
	}
	return func() {}, true, nil
}
func (f *fakeSource) State(context.Context) (domain.ExportState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.st, nil
}
func (f *fakeSource) Plan(_ context.Context, after int64, before time.Time, limit int) (int64, int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var up int64
	n := 0
	for _, r := range f.rows {
		if r.ID > after && r.CreatedAt.Before(before) && n < limit {
			up, n = r.ID, n+1
		}
	}
	return up, n, nil
}
func (f *fakeSource) MarkPending(_ context.Context, upTo int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.st.PendingTo = upTo
	return nil
}
func (f *fakeSource) Read(_ context.Context, after, upTo int64) ([]domain.ExportRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []domain.ExportRecord
	for _, r := range f.rows {
		if r.ID > after && r.ID <= upTo {
			out = append(out, r)
		}
	}
	return out, nil
}
func (f *fakeSource) Commit(_ context.Context, upTo int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.st = domain.ExportState{LastID: upTo}
	f.commits++
	return nil
}
func (f *fakeSource) add(rs ...domain.ExportRecord) {
	f.mu.Lock()
	f.rows = append(f.rows, rs...)
	f.mu.Unlock()
}

type fakeStore struct {
	mu        sync.Mutex
	objects   map[string][]byte
	puts      int
	failPuts  int // los primeros N Put fallan con failErr
	failErr   error
	ensured   int
	ensureErr error
}

func newStore() *fakeStore { return &fakeStore{objects: map[string][]byte{}} }

func (s *fakeStore) EnsureBucket(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensured++
	return s.ensureErr
}
func (s *fakeStore) Put(_ context.Context, key string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.puts++
	if s.failPuts > 0 {
		s.failPuts--
		return s.failErr
	}
	s.objects[key] = append([]byte(nil), data...)
	return nil
}
func (s *fakeStore) keys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var k []string
	for key := range s.objects {
		k = append(k, key)
	}
	sort.Strings(k)
	return k
}

func rec(id int64, tenant string, at time.Time) domain.ExportRecord {
	return domain.ExportRecord{
		ID: id, EventID: fmt.Sprintf("e%d", id), TenantID: tenant, EventType: "click", UserID: "u",
		Payload: `{"n":1}`, CreatedAt: at,
	}
}

var old = time.Date(2026, 11, 2, 23, 59, 0, 0, time.UTC)

func testExporter(src *fakeSource, st *fakeStore, mutate func(*Config)) *Exporter {
	cfg := Config{
		Interval: time.Hour, Poll: time.Hour, Lag: time.Second, BatchSize: 100,
		Retry: RetryPolicy{MaxAttempts: 4, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond},
	}
	if mutate != nil {
		mutate(&cfg)
	}
	return New(src, st, cfg)
}

func readBack(t *testing.T, data []byte) []Row {
	t.Helper()
	rows, err := parquet.Read[Row](bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("parquet.Read: %v", err)
	}
	return rows
}

func TestEncodeRoundTrip(t *testing.T) {
	in := []domain.ExportRecord{
		rec(1, "acme", old),
		{ID: 2, EventID: "e2", TenantID: "acme", EventType: "view", UserID: "u2", CreatedAt: old.Add(time.Minute)}, // sin payload
	}
	data, err := Encode(in)
	if err != nil {
		t.Fatal(err)
	}
	out := readBack(t, data)
	if len(out) != 2 || out[0].ID != 1 || out[0].Payload != `{"n":1}` || out[1].Payload != "" || out[1].EventType != "view" {
		t.Fatalf("round trip: %+v", out)
	}
	if !out[0].CreatedAt.Equal(old) {
		t.Fatalf("created_at = %v, want %v", out[0].CreatedAt, old)
	}
}

func TestPartitionsByDayAndTenant(t *testing.T) {
	src, st := &fakeSource{}, newStore()
	src.add(
		rec(1, "acme", old),                       // 2026-11-02 acme
		rec(2, "acme", old.Add(2*time.Minute)),    // 2026-11-03 acme (cruza medianoche UTC)
		rec(3, "globex", old.Add(2*time.Minute)),  // 2026-11-03 globex
		rec(4, "../evil", old.Add(3*time.Minute)), // tenant hostil
	)
	n, err := testExporter(src, st, func(c *Config) { c.Prefix = "/events/" }).RunOnce(context.Background())
	if err != nil || n != 4 {
		t.Fatalf("RunOnce = %d, %v", n, err)
	}
	want := []string{
		"events/year=2026/month=11/day=02/tenant=acme/events-1-4.parquet",
		"events/year=2026/month=11/day=03/tenant=.._evil/events-1-4.parquet",
		"events/year=2026/month=11/day=03/tenant=acme/events-1-4.parquet",
		"events/year=2026/month=11/day=03/tenant=globex/events-1-4.parquet",
	}
	got := st.keys()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("claves:\n got  %v\n want %v", got, want)
	}
	// "../evil" -> ".._evil": la barra se neutraliza y no se puede escapar de la jerarquía
	if src.st.LastID != 4 || src.st.PendingTo != 0 {
		t.Fatalf("estado tras commit: %+v", src.st)
	}
	if rows := readBack(t, st.objects[want[3]]); len(rows) != 1 || rows[0].TenantID != "globex" {
		t.Fatalf("contenido globex: %+v", rows)
	}
}

func TestTransientUploadFailuresAreRetried(t *testing.T) {
	src, st := &fakeSource{}, newStore()
	src.add(rec(1, "acme", old))
	st.failPuts, st.failErr = 2, errors.New("connection refused")

	n, err := testExporter(src, st, nil).RunOnce(context.Background())
	if err != nil || n != 1 {
		t.Fatalf("RunOnce = %d, %v", n, err)
	}
	if st.puts != 3 || len(st.keys()) != 1 || src.commits != 1 {
		t.Fatalf("puts=%d objetos=%d commits=%d", st.puts, len(st.keys()), src.commits)
	}
}

func TestExhaustedRetriesKeepWatermarkAndResumeSameRange(t *testing.T) {
	src, st := &fakeSource{}, newStore()
	src.add(rec(1, "acme", old), rec(2, "acme", old))
	st.failPuts, st.failErr = 100, errors.New("i/o timeout")

	e := testExporter(src, st, nil)
	if _, err := e.RunOnce(context.Background()); err == nil {
		t.Fatal("se esperaba error con el storage caído")
	}
	if st.puts != 4 {
		t.Fatalf("intentos = %d, want 4 (MaxAttempts)", st.puts)
	}
	if src.st.LastID != 0 || src.st.PendingTo != 2 {
		t.Fatalf("el watermark no debe avanzar y el lote debe quedar fijado: %+v", src.st)
	}

	// Llegan filas nuevas mientras el storage estaba caído, y el storage se recupera
	src.add(rec(3, "acme", old), rec(4, "acme", old))
	st.failPuts = 0
	n, err := e.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// Reintento: mismo rango (1-2) => misma clave; las filas nuevas van en un lote posterior (3-4)
	want := []string{
		"year=2026/month=11/day=02/tenant=acme/events-1-2.parquet",
		"year=2026/month=11/day=02/tenant=acme/events-3-4.parquet",
	}
	if fmt.Sprint(st.keys()) != fmt.Sprint(want) || n != 4 || src.st.LastID != 4 {
		t.Fatalf("n=%d keys=%v state=%+v", n, st.keys(), src.st)
	}
}

func TestPermanentErrorDoesNotRetry(t *testing.T) {
	src, st := &fakeSource{}, newStore()
	src.add(rec(1, "acme", old))
	st.failPuts, st.failErr = 100, fmt.Errorf("%w: AccessDenied", ErrPermanent)

	if _, err := testExporter(src, st, nil).RunOnce(context.Background()); !IsPermanent(err) {
		t.Fatalf("error = %v, se esperaba permanente", err)
	}
	if st.puts != 1 {
		t.Fatalf("un error permanente no se reintenta: puts=%d", st.puts)
	}
}

func TestThresholdsAndLag(t *testing.T) {
	src, st := &fakeSource{}, newStore()
	e := testExporter(src, st, func(c *Config) { c.BatchSize = 3; c.Lag = time.Hour })

	src.add(rec(1, "a", old), rec(2, "a", old))
	if n, _ := e.Drain(context.Background(), 3); n != 0 || st.puts != 0 {
		t.Fatalf("por debajo del tamaño de lote no debe volcar: n=%d", n)
	}
	// Filas recientes (dentro del desfase) nunca se exportan, aunque se fuerce
	src.add(rec(3, "a", time.Now()))
	if n, _ := e.Drain(context.Background(), 1); n != 2 {
		t.Fatalf("forzado: n=%d, want 2 (la reciente queda fuera)", n)
	}
	if src.st.LastID != 2 {
		t.Fatalf("LastID = %d", src.st.LastID)
	}
}

func TestDrainSplitsFullBatches(t *testing.T) {
	src, st := &fakeSource{}, newStore()
	for i := int64(1); i <= 7; i++ {
		src.add(rec(i, "a", old))
	}
	n, err := testExporter(src, st, func(c *Config) { c.BatchSize = 3 }).RunOnce(context.Background())
	if err != nil || n != 7 || src.commits != 3 || len(st.keys()) != 3 {
		t.Fatalf("n=%d err=%v commits=%d objetos=%v", n, err, src.commits, st.keys())
	}
}

func TestLockHeldByAnotherReplicaSkips(t *testing.T) {
	src, st := &fakeSource{lockBusy: true}, newStore()
	src.add(rec(1, "a", old))
	if n, err := testExporter(src, st, nil).RunOnce(context.Background()); n != 0 || err != nil || st.puts != 0 {
		t.Fatalf("n=%d err=%v puts=%d", n, err, st.puts)
	}
}

func TestBucketVerificationFailureIsNotFatalWhenPermanent(t *testing.T) {
	src, st := &fakeSource{}, newStore()
	src.add(rec(1, "a", old))
	st.ensureErr = fmt.Errorf("%w: AccessDenied", ErrPermanent)
	if n, err := testExporter(src, st, nil).RunOnce(context.Background()); err != nil || n != 1 {
		t.Fatalf("n=%d err=%v", n, err)
	}
}

func TestRetryStopsPromptlyOnCancel(t *testing.T) {
	src, st := &fakeSource{}, newStore()
	src.add(rec(1, "a", old))
	st.failPuts, st.failErr = 1000, errors.New("down")
	e := testExporter(src, st, func(c *Config) {
		c.Retry = RetryPolicy{MaxAttempts: 50, BaseDelay: time.Minute, MaxDelay: time.Minute}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := e.RunOnce(ctx); err == nil {
		t.Fatal("se esperaba error")
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("no respetó la cancelación: %s", time.Since(start))
	}
	if src.st.PendingTo != 1 || src.st.LastID != 0 {
		t.Fatalf("el lote debe quedar pendiente: %+v", src.st)
	}
}

func TestBackoffGrowsAndIsBounded(t *testing.T) {
	p := RetryPolicy{BaseDelay: 100 * time.Millisecond, MaxDelay: time.Second}
	for attempt := 1; attempt <= 30; attempt++ {
		d := p.backoff(attempt)
		if d <= 0 || d > time.Second {
			t.Fatalf("backoff(%d) = %s fuera de rango", attempt, d)
		}
	}
	if d := p.backoff(1); d < 50*time.Millisecond || d > 100*time.Millisecond {
		t.Fatalf("backoff(1) = %s", d)
	}
}
