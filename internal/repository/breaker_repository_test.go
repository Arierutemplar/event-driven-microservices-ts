package repository

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Arierutemplar/event-driven-microservices-ts/internal/domain"
	"github.com/Arierutemplar/event-driven-microservices-ts/internal/resilience"
)

type stubRepo struct {
	err   atomic.Pointer[error]
	calls atomic.Int32
	pings atomic.Int32
}

func (s *stubRepo) cur() error {
	if p := s.err.Load(); p != nil {
		return *p
	}
	return nil
}
func (s *stubRepo) set(err error) { s.err.Store(&err) }
func (s *stubRepo) clear()        { s.err.Store(nil) }

func (s *stubRepo) Save(context.Context, *domain.Event) error {
	s.calls.Add(1)
	return s.cur()
}
func (s *stubRepo) SaveBatch(context.Context, []*domain.Event) error {
	s.calls.Add(1)
	return s.cur()
}
func (s *stubRepo) GetHistory(context.Context, int) ([]*domain.Event, error) { return nil, nil }
func (s *stubRepo) Ping(context.Context) error {
	s.pings.Add(1)
	return s.cur()
}

func newBreakerRepo(s *stubRepo) *BreakerRepository {
	return NewBreakerRepository(s, resilience.Config{Name: "t-" + fmt.Sprint(time.Now().UnixNano()), ConsecutiveFailures: 3, OpenTimeout: 100 * time.Millisecond})
}

func TestBreakerRepositoryPermanentErrorsDoNotTrip(t *testing.T) {
	s := &stubRepo{}
	s.set(domain.ErrPermanent)
	r := newBreakerRepo(s)
	for i := 0; i < 10; i++ {
		if err := r.SaveBatch(context.Background(), nil); !errors.Is(err, domain.ErrPermanent) {
			t.Fatalf("err = %v", err)
		}
	}
	if r.State() != resilience.Closed {
		t.Fatalf("estado = %v, quería Closed", r.State())
	}
}

func TestBreakerRepositoryOpensRejectsAndRecovers(t *testing.T) {
	s := &stubRepo{}
	s.set(errors.New("db caída"))
	r := newBreakerRepo(s)
	for i := 0; i < 3; i++ {
		_ = r.SaveBatch(context.Background(), nil)
	}
	if r.State() != resilience.Open {
		t.Fatalf("estado = %v, quería Open", r.State())
	}
	before := s.calls.Load()
	if err := r.SaveBatch(context.Background(), nil); !errors.Is(err, resilience.ErrOpen) {
		t.Fatalf("err = %v, quería ErrOpen", err)
	}
	if s.calls.Load() != before {
		t.Fatal("con el circuito abierto no debe llamarse a la base")
	}

	// Con la base aún caída WaitReady no debe devolver hasta que el ctx expire
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()
	if err := r.WaitReady(ctx); err == nil {
		t.Fatal("WaitReady debía fallar con la base caída")
	}
	if s.pings.Load() == 0 {
		t.Fatal("la recuperación debe sondear con Ping")
	}

	s.clear()
	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()
	if err := r.WaitReady(ctx2); err != nil {
		t.Fatalf("WaitReady tras recuperarse: %v", err)
	}
	if err := r.SaveBatch(context.Background(), nil); err != nil {
		t.Fatalf("SaveBatch tras recuperarse: %v", err)
	}
	if r.State() != resilience.Closed {
		t.Fatalf("estado = %v, quería Closed", r.State())
	}
}
