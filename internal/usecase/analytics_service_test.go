package usecase

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Arierutemplar/event-driven-microservices-ts/internal/dedup"
	"github.com/Arierutemplar/event-driven-microservices-ts/internal/domain"
)

// fakeRepo es un EventRepository thread-safe para las pruebas.
type fakeRepo struct {
	mu      sync.Mutex
	saved   []domain.Event
	failing bool
}

func (r *fakeRepo) Save(_ context.Context, e *domain.Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failing {
		return errors.New("fallo simulado")
	}
	r.saved = append(r.saved, *e)
	return nil
}

func (r *fakeRepo) GetHistory(_ context.Context, limit int) ([]*domain.Event, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*domain.Event, 0, len(r.saved))
	for i := len(r.saved) - 1; i >= 0 && (limit <= 0 || len(out) < limit); i-- {
		e := r.saved[i]
		out = append(out, &e)
	}
	return out, nil
}

func (r *fakeRepo) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.saved)
}

func TestConcurrentIngestAllProcessed(t *testing.T) {
	const (
		producers   = 50
		perProducer = 200
		total       = producers * perProducer
	)
	repo := &fakeRepo{}
	svc := NewAnalyticsService(total, repo) // buffer holgado: ninguna ingesta se rechaza
	svc.StartWorkerPool(8)

	types := []string{"click", "page_view", "purchase"}
	var rejected atomic.Int64
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				e := domain.Event{
					ID:        fmt.Sprintf("p%d-%d", p, i),
					EventType: types[(p+i)%len(types)],
					Timestamp: time.Now(),
				}
				if err := svc.IngestEvent(e); err != nil {
					rejected.Add(1)
				}
			}
		}(p)
	}
	wg.Wait()
	svc.Stop() // drena la cola y espera a los workers

	if rejected.Load() != 0 {
		t.Fatalf("%d eventos rechazados con buffer suficiente", rejected.Load())
	}

	sum := svc.GetSummary()
	if sum.TotalEvents != total {
		t.Fatalf("TotalEvents = %d, quería %d", sum.TotalEvents, total)
	}
	var byTypeSum int64
	for _, n := range sum.EventsByType {
		byTypeSum += n
	}
	if byTypeSum != total {
		t.Fatalf("suma por tipo = %d, quería %d", byTypeSum, total)
	}
	if repo.count() != total {
		t.Fatalf("persistidos = %d, quería %d", repo.count(), total)
	}
	if sum.LastEvent == nil {
		t.Fatal("LastEvent no debería ser nil")
	}
}

func TestReadersConcurrentWithWriters(t *testing.T) {
	repo := &fakeRepo{}
	svc := NewAnalyticsService(10000, repo)
	svc.StartWorkerPool(4)

	stopReaders := make(chan struct{})
	var readers sync.WaitGroup
	for i := 0; i < 4; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			var prev int64
			for {
				select {
				case <-stopReaders:
					return
				default:
				}
				s := svc.GetSummary()
				if s.TotalEvents < prev {
					t.Errorf("TotalEvents retrocedió: %d -> %d", prev, s.TotalEvents)
					return
				}
				prev = s.TotalEvents
				// Mutar la copia no debe afectar al estado interno
				s.EventsByType["mutado"] = 99
			}
		}()
	}

	var producers sync.WaitGroup
	for p := 0; p < 10; p++ {
		producers.Add(1)
		go func() {
			defer producers.Done()
			for i := 0; i < 500; i++ {
				_ = svc.IngestEvent(domain.Event{EventType: "click"})
			}
		}()
	}
	producers.Wait()
	svc.Stop()
	close(stopReaders)
	readers.Wait()

	if _, ok := svc.GetSummary().EventsByType["mutado"]; ok {
		t.Fatal("GetSummary expuso el mapa interno")
	}
}

func TestIngestNonBlockingWhenQueueFull(t *testing.T) {
	svc := NewAnalyticsService(2, &fakeRepo{}) // sin workers: la cola se llena

	for i := 0; i < 2; i++ {
		if err := svc.IngestEvent(domain.Event{ID: "x"}); err != nil {
			t.Fatalf("ingesta %d falló: %v", i, err)
		}
	}

	done := make(chan error, 1)
	go func() { done <- svc.IngestEvent(domain.Event{ID: "overflow"}) }()

	select {
	case err := <-done:
		if !errors.Is(err, ErrQueueFull) {
			t.Fatalf("error = %v, quería ErrQueueFull", err)
		}
	case <-time.After(time.Second):
		t.Fatal("IngestEvent se bloqueó con la cola llena")
	}
}

func TestStopDrainsQueuedEvents(t *testing.T) {
	repo := &fakeRepo{}
	svc := NewAnalyticsService(100, repo)
	for i := 0; i < 100; i++ {
		if err := svc.IngestEvent(domain.Event{EventType: "click"}); err != nil {
			t.Fatal(err)
		}
	}
	// Los workers arrancan con la cola ya llena y Stop se pide de inmediato.
	svc.StartWorkerPool(4)
	svc.Stop()

	if got := svc.GetSummary().TotalEvents; got != 100 {
		t.Fatalf("TotalEvents = %d, quería 100", got)
	}
}

func TestStopIsIdempotentAndRejectsIngest(t *testing.T) {
	svc := NewAnalyticsService(10, &fakeRepo{})
	svc.StartWorkerPool(2)
	svc.Stop()
	svc.Stop() // no debe provocar panic por doble close

	if err := svc.IngestEvent(domain.Event{}); err == nil {
		t.Fatal("se esperaba error al ingerir tras Stop")
	}
}

func TestPersistFailureStillUpdatesSummary(t *testing.T) {
	repo := &fakeRepo{failing: true}
	svc := NewAnalyticsService(10, repo)
	svc.StartWorkerPool(2)
	for i := 0; i < 5; i++ {
		_ = svc.IngestEvent(domain.Event{EventType: "click"})
	}
	svc.Stop()

	if got := svc.GetSummary().TotalEvents; got != 5 {
		t.Fatalf("TotalEvents = %d, quería 5", got)
	}
	if repo.count() != 0 {
		t.Fatal("no debería haber eventos persistidos")
	}
}

func TestDuplicateIDsProcessedOnce(t *testing.T) {
	repo := &fakeRepo{}
	svc := NewAnalyticsService(1000, repo).WithDedup(dedup.New(time.Minute, 1000))
	svc.StartWorkerPool(8)

	// 20 productores concurrentes reenvían los mismos 50 ids
	var wg sync.WaitGroup
	for p := 0; p < 20; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				_ = svc.IngestEvent(domain.Event{ID: fmt.Sprint("evt-", i), EventType: "click"})
			}
		}()
	}
	wg.Wait()
	svc.Stop()

	if got := svc.GetSummary().TotalEvents; got != 50 {
		t.Fatalf("TotalEvents = %d, quería 50 (uno por id)", got)
	}
	if repo.count() != 50 {
		t.Fatalf("persistidos = %d, quería 50", repo.count())
	}
}

func TestMissingIDGetsAssigned(t *testing.T) {
	repo := &fakeRepo{}
	svc := NewAnalyticsService(10, repo).WithDedup(dedup.New(time.Minute, 100))
	svc.StartWorkerPool(1)
	for i := 0; i < 3; i++ {
		if err := svc.IngestEvent(domain.Event{EventType: "click"}); err != nil {
			t.Fatal(err)
		}
	}
	svc.Stop()

	seen := map[string]bool{}
	for _, e := range repo.saved {
		if e.ID == "" {
			t.Fatal("el evento sin id debió recibir uno")
		}
		seen[e.ID] = true
	}
	if len(seen) != 3 || svc.GetSummary().TotalEvents != 3 {
		t.Fatalf("ids únicos = %d, total = %d; quería 3 y 3", len(seen), svc.GetSummary().TotalEvents)
	}
}

func TestGetHistoryDelegatesToRepository(t *testing.T) {
	repo := &fakeRepo{}
	svc := NewAnalyticsService(10, repo)
	svc.StartWorkerPool(1) // un solo worker mantiene el orden
	for _, id := range []string{"a", "b", "c"} {
		_ = svc.IngestEvent(domain.Event{ID: id})
	}
	svc.Stop()

	got, err := svc.GetHistory(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ID != "c" || got[1].ID != "b" {
		t.Fatalf("historial inesperado: %+v", got)
	}
}

func BenchmarkConcurrentIngest(b *testing.B) {
	svc := NewAnalyticsService(b.N+1, &fakeRepo{})
	svc.StartWorkerPool(8)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = svc.IngestEvent(domain.Event{EventType: "click"})
		}
	})
	svc.Stop()
}

func TestDedupIsScopedPerTenant(t *testing.T) {
	repo := &fakeRepo{}
	svc := NewAnalyticsService(100, repo).WithDedup(dedup.New(time.Minute, 1000))
	svc.StartWorkerPool(2)

	// Mismo id en dos tenants: ambos cuentan. Repetido dentro del mismo tenant: solo uno.
	_ = svc.IngestEvent(domain.Event{ID: "x", TenantID: "a", EventType: "click"})
	_ = svc.IngestEvent(domain.Event{ID: "x", TenantID: "b", EventType: "click"})
	_ = svc.IngestEvent(domain.Event{ID: "x", TenantID: "a", EventType: "click"})
	_ = svc.IngestEvent(domain.Event{ID: "y", EventType: "click"}) // sin tenant => default
	svc.Stop()

	if got := svc.GetSummary().TotalEvents; got != 3 {
		t.Fatalf("TotalEvents = %d, quería 3", got)
	}
	if got := svc.GetSummary().LastEvent; got == nil {
		t.Fatal("sin último evento")
	}
	for _, e := range svc.GetSummary().RecentEvents {
		if e.TenantID == "" {
			t.Errorf("evento %q sin tenant", e.ID)
		}
	}
}
