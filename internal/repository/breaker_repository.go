package repository

import (
	"context"
	"errors"

	"github.com/Arierutemplar/event-driven-microservices-ts/internal/domain"
	"github.com/Arierutemplar/event-driven-microservices-ts/internal/resilience"
)

var _ domain.BatchEventRepository = (*BreakerRepository)(nil)

// Pinger lo implementan los repositorios que pueden comprobar su conectividad barata.
type Pinger interface {
	Ping(ctx context.Context) error
}

// BreakerRepository protege las escrituras de otro repositorio con un circuit breaker.
// Los errores permanentes (datos inválidos) no cuentan: indican un evento venenoso, no
// una base de datos enferma. Las lecturas no pasan por el circuito.
type BreakerRepository struct {
	next    domain.BatchEventRepository
	breaker *resilience.Breaker
}

// NewBreakerRepository envuelve next. cfg.IsFailure se fija según la semántica de errores del dominio.
func NewBreakerRepository(next domain.BatchEventRepository, cfg resilience.Config) *BreakerRepository {
	cfg.IsFailure = isStorageFailure
	return &BreakerRepository{next: next, breaker: resilience.New(cfg)}
}

func isStorageFailure(err error) bool {
	if err == nil {
		return false
	}
	// Un apagado (contexto cancelado) o un dato inválido no indican una base caída
	return !errors.Is(err, domain.ErrPermanent) && !errors.Is(err, context.Canceled)
}

func (r *BreakerRepository) Save(ctx context.Context, e *domain.Event) error {
	return r.breaker.Do(func() error { return r.next.Save(ctx, e) })
}

func (r *BreakerRepository) SaveBatch(ctx context.Context, events []*domain.Event) error {
	return r.breaker.Do(func() error { return r.next.SaveBatch(ctx, events) })
}

func (r *BreakerRepository) GetHistory(ctx context.Context, limit int) ([]*domain.Event, error) {
	return r.next.GetHistory(ctx, limit)
}

func (r *BreakerRepository) GetHistoryForTenant(ctx context.Context, tenantID string, limit int) ([]*domain.Event, error) {
	if scoped, ok := r.next.(domain.TenantHistoryRepository); ok {
		return scoped.GetHistoryForTenant(ctx, tenantID, limit)
	}
	return nil, errors.New("repository: tenant history not supported")
}

// State expone el estado del circuito.
func (r *BreakerRepository) State() resilience.State { return r.breaker.State() }

// WaitReady bloquea mientras el circuito esté abierto. Cuando pasa a semiabierto sondea la
// base con Ping (si next lo soporta) para que la recuperación no gaste entregas de mensajes reales.
func (r *BreakerRepository) WaitReady(ctx context.Context) error {
	var probe func(context.Context) error
	if p, ok := r.next.(Pinger); ok {
		probe = p.Ping
	}
	return r.breaker.WaitReady(ctx, probe)
}
