package domain

import (
	"context"
	"errors"
)

// ErrPermanent marca fallos de almacenamiento que no se resuelven reintentando
// (p. ej. datos que la base rechaza). El consumer descarta esos mensajes en
// lugar de reentregarlos indefinidamente.
var ErrPermanent = errors.New("permanent storage error")

// EventRepository abstrae la persistencia de eventos.
type EventRepository interface {
	Save(ctx context.Context, event *Event) error
	// GetHistory devuelve hasta `limit` eventos, del más reciente al más antiguo.
	GetHistory(ctx context.Context, limit int) ([]*Event, error)
}

// TenantHistoryRepository permite consultas acotadas a un tenant.
// Las implementaciones deben tratar tenant vacío como DefaultTenant.
type TenantHistoryRepository interface {
	GetHistoryForTenant(ctx context.Context, tenantID string, limit int) ([]*Event, error)
}

// BatchEventRepository añade la inserción masiva. Si el evento trae MessageID,
// la inserción es idempotente.
type BatchEventRepository interface {
	EventRepository
	SaveBatch(ctx context.Context, events []*Event) error
}

// EventPublisher encola eventos en un broker de forma durable: Publish solo
// retorna nil cuando el broker confirmó el mensaje.
type EventPublisher interface {
	Publish(ctx context.Context, event *Event) error
}
