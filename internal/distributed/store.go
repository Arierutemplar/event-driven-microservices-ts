package distributed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Arierutemplar/REPO/analytics-service/internal/domain"
)

type SummaryStore interface {
	RecordEvent(ctx context.Context, event *domain.Event) error
	RecordDLQ(ctx context.Context) error
	GetSummary(ctx context.Context, tenantID string) (domain.MetricSummary, error)
}

type RedisStore struct {
	client *redis.Client
	prefix string
}

func NewRedisStore(ctx context.Context, url, prefix string) (*RedisStore, error) {
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("redis: parse URL: %w", err)
	}
	if prefix == "" {
		prefix = "analytics"
	}
	client := redis.NewClient(opts)
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("redis: ping: %w", err)
	}
	return &RedisStore{client: client, prefix: prefix}, nil
}

func (s *RedisStore) Close() error { return s.client.Close() }

func (s *RedisStore) base(tenantID string) string {
	if tenantID == "" {
		tenantID = domain.DefaultTenant
	}
	return s.prefix + ":summary:" + tenantID
}

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

	for _, base := range []string{s.prefix + ":summary:global", s.base(tenantID)} {
		pipe := s.client.TxPipeline()
		pipe.Incr(ctx, base+":total")
		pipe.HIncrBy(ctx, base+":types", event.EventType, 1)
		pipe.Set(ctx, base+":last", payload, 0)
		pipe.LPush(ctx, base+":recent", payload)
		pipe.LTrim(ctx, base+":recent", 0, domain.RecentEventsLimit-1)
		pipe.Expire(ctx, base+":recent", 30*24*time.Hour)
		if _, err := pipe.Exec(ctx); err != nil {
			return fmt.Errorf("redis: record event: %w", err)
		}
	}
	return nil
}

func (s *RedisStore) RecordDLQ(ctx context.Context) error {
	for _, base := range []string{s.prefix + ":summary:global", s.prefix + ":summary:default"} {
		if err := s.client.Incr(ctx, base+":dlq").Err(); err != nil {
			return fmt.Errorf("redis: record dlq: %w", err)
		}
	}
	return nil
}

func (s *RedisStore) GetSummary(ctx context.Context, tenantID string) (domain.MetricSummary, error) {
	if tenantID == "" {
		tenantID = domain.DefaultTenant
	}
	base := s.base(tenantID)
	if tenantID == domain.DefaultTenant {
		base = s.prefix + ":summary:global"
	}

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
