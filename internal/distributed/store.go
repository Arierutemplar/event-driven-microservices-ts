package distributed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Arierutemplar/event-driven-microservices-ts/internal/domain"
	"github.com/Arierutemplar/event-driven-microservices-ts/internal/transport"
)

type SummaryStore interface {
	RecordEvent(ctx context.Context, event *domain.Event) error
	RecordDLQ(ctx context.Context) error
	GetSummary(ctx context.Context, tenantID string) (domain.MetricSummary, error)
	GetGlobalSummary(ctx context.Context) (domain.MetricSummary, error)
}

type RedisStore struct {
	client *redis.Client
	prefix string
}

func NewRedisStore(ctx context.Context, url, prefix string) (*RedisStore, error) {
	opts, err := transport.RedisOptions(url)
	if err != nil {
		return nil, fmt.Errorf("redis: parse URL: %w", err)
	}
	if prefix == "" {
		prefix = "analytics"
	}
	client := redis.NewClient(opts)
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redis: ping: %w", err)
	}
	return &RedisStore{client: client, prefix: prefix}, nil
}

func (s *RedisStore) Close() error { return s.client.Close() }

func (s *RedisStore) Ping(ctx context.Context) error { return s.client.Ping(ctx).Err() }

func (s *RedisStore) tenantBase(tenantID string) string {
	if tenantID == "" {
		tenantID = domain.DefaultTenant
	}
	// El prefijo "t:" separa los tenants del agregado global: ningún nombre de tenant puede colisionar con él
	return s.prefix + ":summary:t:" + tenantID
}

func (s *RedisStore) globalBase() string { return s.prefix + ":summary:g" }

// recordScript aplica un evento a los agregados global y del tenant solo si su id no se había
// contado antes, de forma atómica: una reentrega tras un fallo no duplica los contadores.
var recordScript = redis.NewScript(`
if redis.call('SET', KEYS[1], '1', 'NX', 'EX', ARGV[3]) == false then
  return 0
end
for i = 2, 3 do
  local base = KEYS[i]
  redis.call('INCR', base .. ':total')
  redis.call('HINCRBY', base .. ':types', ARGV[1], 1)
  redis.call('SET', base .. ':last', ARGV[2])
  redis.call('LPUSH', base .. ':recent', ARGV[2])
  redis.call('LTRIM', base .. ':recent', 0, tonumber(ARGV[4]) - 1)
  redis.call('EXPIRE', base .. ':recent', 2592000)
end
return 1
`)

const seenTTLSeconds = 7 * 24 * 3600 // mayor que la retención del stream (72 h)

func (s *RedisStore) RecordEvent(ctx context.Context, event *domain.Event) error {
	if event == nil {
		return errors.New("redis: nil event")
	}
	tenantID := event.TenantID
	if tenantID == "" {
		tenantID = domain.DefaultTenant
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("redis: serialize event: %w", err)
	}

	keys := []string{
		s.prefix + ":seen:" + tenantID + ":" + event.ID,
		s.globalBase(),
		s.tenantBase(tenantID),
	}
	if event.ID == "" {
		// Sin id no hay identidad que deduplicar: se cuenta siempre
		keys[0] = s.prefix + ":seen:" + tenantID + ":anon:" + strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	if err := recordScript.Run(ctx, s.client, keys, event.EventType, payload, seenTTLSeconds, domain.RecentEventsLimit).Err(); err != nil {
		return fmt.Errorf("redis: record event: %w", err)
	}
	return nil
}

// RecordDLQ cuenta un mensaje muerto en el agregado global; el mensaje puede no ser decodificable,
// así que no se atribuye a ningún tenant.
func (s *RedisStore) RecordDLQ(ctx context.Context) error {
	if err := s.client.Incr(ctx, s.globalBase()+":dlq").Err(); err != nil {
		return fmt.Errorf("redis: record dlq: %w", err)
	}
	return nil
}

// GetGlobalSummary devuelve el agregado de todos los tenants (solo uso operativo).
func (s *RedisStore) GetGlobalSummary(ctx context.Context) (domain.MetricSummary, error) {
	return s.read(ctx, s.globalBase())
}

// GetSummary devuelve solo los datos del tenant indicado.
func (s *RedisStore) GetSummary(ctx context.Context, tenantID string) (domain.MetricSummary, error) {
	return s.read(ctx, s.tenantBase(tenantID))
}

func (s *RedisStore) read(ctx context.Context, base string) (domain.MetricSummary, error) {
	totalRaw, err := s.client.Get(ctx, base+":total").Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return domain.MetricSummary{}, fmt.Errorf("redis: read total: %w", err)
	}
	total, _ := strconv.ParseInt(totalRaw, 10, 64)

	dlqRaw, err := s.client.Get(ctx, base+":dlq").Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return domain.MetricSummary{}, fmt.Errorf("redis: read dlq: %w", err)
	}
	dlq, _ := strconv.ParseInt(dlqRaw, 10, 64)

	types, err := s.client.HGetAll(ctx, base+":types").Result()
	if err != nil {
		return domain.MetricSummary{}, fmt.Errorf("redis: read types: %w", err)
	}
	byType := make(map[string]int64, len(types))
	for name, raw := range types {
		value, _ := strconv.ParseInt(raw, 10, 64)
		byType[name] = value
	}

	recentRaw, err := s.client.LRange(ctx, base+":recent", 0, domain.RecentEventsLimit-1).Result()
	if err != nil {
		return domain.MetricSummary{}, fmt.Errorf("redis: read recent: %w", err)
	}
	recent := make([]domain.Event, 0, len(recentRaw))
	for _, raw := range recentRaw {
		var event domain.Event
		if json.Unmarshal([]byte(raw), &event) == nil {
			recent = append(recent, event)
		}
	}

	var last *domain.Event
	lastRaw, err := s.client.Get(ctx, base+":last").Result()
	if err == nil {
		var event domain.Event
		if json.Unmarshal([]byte(lastRaw), &event) == nil {
			last = &event
		}
	} else if !errors.Is(err, redis.Nil) {
		return domain.MetricSummary{}, fmt.Errorf("redis: read last: %w", err)
	}

	return domain.MetricSummary{
		TotalEvents:  total,
		EventsByType: byType,
		LastEvent:    last,
		RecentEvents: recent,
		DLQEvents:    dlq,
	}, nil
}
