package usecase

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/Arierutemplar/event-driven-microservices-ts/internal/dedup"
	"github.com/Arierutemplar/event-driven-microservices-ts/internal/domain"
	"github.com/Arierutemplar/event-driven-microservices-ts/internal/observability"
)

const (
	persistTimeout = 2 * time.Second
	publishTimeout = 3 * time.Second
)

// ErrQueueFull se retorna cuando el canal de eventos está lleno.
var ErrQueueFull = errors.New("analytics: event queue is full")

// AnalyticsService ingesta eventos y mantiene un resumen de métricas.
//
// Tiene dos modos de ingesta:
//   - directo (por defecto): canal en memoria + worker pool que persiste cada evento.
//   - publisher (WithPublisher): IngestEvent publica de forma durable en un broker y
//     un consumer externo persiste por lotes y llama a RecordProcessed.
type AnalyticsService struct {
	events    chan domain.Event
	repo      domain.EventRepository
	publisher domain.EventPublisher

	summaryStore SummaryStore

	mu      sync.RWMutex
	summary domain.MetricSummary

	dedup *dedup.Cache

	tenantSummaries map[string]*domain.MetricSummary

	wg       sync.WaitGroup
	stopOnce sync.Once
	done     chan struct{}
}

type SummaryStore interface {
	RecordEvent(ctx context.Context, event *domain.Event) error
	RecordDLQ(ctx context.Context) error
	GetSummary(ctx context.Context, tenantID string) (domain.MetricSummary, error)
	GetGlobalSummary(ctx context.Context) (domain.MetricSummary, error)
}

func (s *AnalyticsService) WithSummaryStore(store SummaryStore) *AnalyticsService {
	s.summaryStore = store
	return s
}

// WithDedup activa la deduplicación en memoria del modo directo: un evento con un id ya
// procesado recientemente no se persiste ni se cuenta de nuevo.
func (s *AnalyticsService) WithDedup(c *dedup.Cache) *AnalyticsService {
	s.dedup = c
	return s
}

func newEventID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand solo falla si el SO no tiene entropía; el id sigue siendo único por tiempo
		return fmt.Sprintf("evt-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

// WithPublisher activa el modo publisher. Debe llamarse antes de usar el servicio.
func (s *AnalyticsService) WithPublisher(p domain.EventPublisher) *AnalyticsService {
	s.publisher = p
	return s
}

// NewAnalyticsService crea el servicio con un canal con buffer de tamaño bufferSize
// y el repositorio donde se persistirá cada evento.
func NewAnalyticsService(bufferSize int, repo domain.EventRepository) *AnalyticsService {
	if bufferSize < 0 {
		bufferSize = 0
	}
	return &AnalyticsService{
		events:          make(chan domain.Event, bufferSize),
		repo:            repo,
		done:            make(chan struct{}),
		tenantSummaries: make(map[string]*domain.MetricSummary),
		summary: domain.MetricSummary{
			EventsByType: make(map[string]int64),
		},
	}
}

// GetHistory devuelve los eventos persistidos más recientes.
func (s *AnalyticsService) GetHistory(ctx context.Context, limit int) ([]*domain.Event, error) {
	return s.repo.GetHistory(ctx, limit)
}

// GetHistoryForTenant devuelve historial aislado por tenant.
func (s *AnalyticsService) GetHistoryForTenant(ctx context.Context, tenantID string, limit int) ([]*domain.Event, error) {
	if scoped, ok := s.repo.(domain.TenantHistoryRepository); ok {
		return scoped.GetHistoryForTenant(ctx, tenantID, limit)
	}
	events, err := s.repo.GetHistory(ctx, limit)
	if err != nil {
		return nil, err
	}
	if tenantID == "" {
		tenantID = domain.DefaultTenant
	}
	filtered := make([]*domain.Event, 0, len(events))
	for _, event := range events {
		if event.TenantID == tenantID {
			filtered = append(filtered, event)
		}
	}
	return filtered, nil
}

// IngestEvent acepta el evento. En modo directo lo encola sin bloquear (ErrQueueFull si
// no hay espacio); en modo publisher lo publica y espera la confirmación del broker.
func (s *AnalyticsService) IngestEvent(e domain.Event) error {
	return s.IngestEventContext(context.Background(), e)
}

func (s *AnalyticsService) IngestEventContext(parent context.Context, e domain.Event) error {
	select {
	case <-s.done:
		return errors.New("analytics: service stopped")
	default:
	}

	// Sin id el evento no tendría identidad para deduplicar ni para trazarlo
	if e.ID == "" {
		e.ID = newEventID()
	}
	if e.TenantID == "" {
		e.TenantID = domain.DefaultTenant
	}

	if s.publisher != nil {
		ctx, cancel := context.WithTimeout(parent, publishTimeout)
		defer cancel()
		if err := s.publisher.Publish(ctx, &e); err != nil {
			return fmt.Errorf("analytics: publicar evento: %w", err)
		}
		return nil
	}

	select {
	case s.events <- e:
		return nil
	default:
		return ErrQueueFull
	}
}

// RecordProcessed actualiza el resumen local y el distribuido con eventos ya persistidos (modo publisher).
func (s *AnalyticsService) RecordProcessed(events []*domain.Event) {
	s.ApplyLocal(events)
	if err := s.AggregateDistributed(context.Background(), events); err != nil {
		slog.Error("analytics: actualizar resumen distribuido", "error", err)
	}
}

// ApplyLocal actualiza solo el resumen en memoria.
func (s *AnalyticsService) ApplyLocal(events []*domain.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range events {
		s.apply(*e)
	}
}

// AggregateDistributed registra los eventos en el almacén distribuido. Es idempotente por id de
// evento, así que puede reintentarse tras un fallo sin duplicar contadores.
func (s *AnalyticsService) AggregateDistributed(ctx context.Context, events []*domain.Event) error {
	if s.summaryStore == nil {
		return nil
	}
	var errs []error
	for _, e := range events {
		if err := s.summaryStore.RecordEvent(ctx, e); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// apply requiere s.mu tomado para escritura.
func (s *AnalyticsService) apply(e domain.Event) {
	s.summary.TotalEvents++
	s.summary.EventsByType[e.EventType]++
	s.summary.LastEvent = &e

	recent := s.summary.RecentEvents
	if len(recent) >= domain.RecentEventsLimit {
		recent = recent[:domain.RecentEventsLimit-1]
	}
	s.summary.RecentEvents = append([]domain.Event{e}, recent...)

	tenantID := e.TenantID
	if tenantID == "" {
		tenantID = domain.DefaultTenant
	}
	tenantSummary, ok := s.tenantSummaries[tenantID]
	if !ok {
		tenantSummary = &domain.MetricSummary{EventsByType: make(map[string]int64)}
		s.tenantSummaries[tenantID] = tenantSummary
	}
	tenantSummary.TotalEvents++
	tenantSummary.EventsByType[e.EventType]++
	tenantEvent := e
	tenantSummary.LastEvent = &tenantEvent
	recentTenant := tenantSummary.RecentEvents
	if len(recentTenant) >= domain.RecentEventsLimit {
		recentTenant = recentTenant[:domain.RecentEventsLimit-1]
	}
	tenantSummary.RecentEvents = append([]domain.Event{tenantEvent}, recentTenant...)
}

// RecordDLQ registra un evento enviado a la Dead Letter Queue.
func (s *AnalyticsService) RecordDLQ() {
	s.mu.Lock()
	s.summary.DLQEvents++
	s.mu.Unlock()
	if s.summaryStore != nil {
		if err := s.summaryStore.RecordDLQ(context.Background()); err != nil {
			slog.Error("analytics: actualizar DLQ distribuida", "error", err)
		}
	}
}

// StartWorkerPool lanza `workers` goroutines que procesan eventos hasta Stop().
func (s *AnalyticsService) StartWorkerPool(workers int) {
	if workers < 1 {
		workers = 1
	}
	for i := 0; i < workers; i++ {
		s.wg.Add(1)
		go s.worker()
	}
}

func (s *AnalyticsService) worker() {
	defer s.wg.Done()
	for {
		select {
		case e := <-s.events:
			s.process(e)
		case <-s.done:
			// Drena lo que quede encolado antes de salir.
			for {
				select {
				case e := <-s.events:
					s.process(e)
				default:
					return
				}
			}
		}
	}
}

func (s *AnalyticsService) process(e domain.Event) {
	// Un id ya procesado (reenvío del cliente) no se persiste ni se cuenta otra vez
	if s.dedup != nil {
		if !s.dedup.TryAdd(e.ScopedID()) {
			observability.DedupDropped("worker", 1)
			return
		}
	}

	// Persistir primero; un fallo se registra pero no pierde la métrica en memoria.
	ctx, cancel := context.WithTimeout(context.Background(), persistTimeout)
	err := s.repo.Save(ctx, &e)
	cancel()
	if err != nil {
		slog.Error("analytics: error persistiendo evento", "event_id", e.ID, "error", err)
		if s.dedup != nil {
			s.dedup.Forget(e.ScopedID())
		}
	}

	s.mu.Lock()
	s.apply(e)
	s.mu.Unlock()
	if err == nil && s.summaryStore != nil {
		if err := s.summaryStore.RecordEvent(context.Background(), &e); err != nil {
			slog.Error("analytics: actualizar resumen distribuido", "error", err)
		}
	}
}

// Stop detiene los workers tras drenar la cola y espera a que terminen.
func (s *AnalyticsService) Stop() {
	s.stopOnce.Do(func() { close(s.done) })
	s.wg.Wait()
}

// GetSummary retorna una copia del resumen actual, segura para uso concurrente.
func (s *AnalyticsService) GetSummary() domain.MetricSummary {
	if s.summaryStore != nil {
		if summary, err := s.summaryStore.GetGlobalSummary(context.Background()); err == nil {
			return summary
		} else {
			slog.Error("analytics: leer resumen distribuido", "error", err)
		}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return copyMetricSummary(s.summary)
}

// GetSummaryForTenant devuelve el resumen aislado para el tenant indicado.
func (s *AnalyticsService) GetSummaryForTenant(tenantID string) domain.MetricSummary {
	if tenantID == "" {
		tenantID = domain.DefaultTenant
	}
	if s.summaryStore != nil {
		if summary, err := s.summaryStore.GetSummary(context.Background(), tenantID); err == nil {
			return summary
		} else {
			slog.Error("analytics: leer resumen distribuido", "error", err)
		}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	source := s.summary
	if tenantSummary, ok := s.tenantSummaries[tenantID]; ok {
		source = *tenantSummary
	} else if tenantID != domain.DefaultTenant {
		source = domain.MetricSummary{EventsByType: make(map[string]int64)}
	}
	return copyMetricSummary(source)
}

func copyMetricSummary(source domain.MetricSummary) domain.MetricSummary {
	byType := make(map[string]int64, len(source.EventsByType))
	for k, v := range source.EventsByType {
		byType[k] = v
	}

	var last *domain.Event
	if source.LastEvent != nil {
		l := *source.LastEvent
		last = &l
	}
	return domain.MetricSummary{
		TotalEvents:  source.TotalEvents,
		EventsByType: byType,
		LastEvent:    last,
		RecentEvents: append([]domain.Event{}, source.RecentEvents...),
		DLQEvents:    source.DLQEvents,
	}
}
