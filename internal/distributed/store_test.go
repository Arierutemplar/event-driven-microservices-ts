package distributed

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Arierutemplar/event-driven-microservices-ts/internal/domain"
)

// Requiere TEST_REDIS_URL. Usa un prefijo único por ejecución.
func newTestStore(t *testing.T) *RedisStore {
	t.Helper()
	url := os.Getenv("TEST_REDIS_URL")
	if url == "" {
		t.Skip("TEST_REDIS_URL no definido")
	}
	store, err := NewRedisStore(context.Background(), url, fmt.Sprintf("test-%d", time.Now().UnixNano()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func TestRedisStoreIsolatesTenants(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	for _, e := range []*domain.Event{
		{ID: "1", TenantID: "alpha", EventType: "click"},
		{ID: "2", TenantID: "global", EventType: "view"},
		{ID: "3", EventType: "view"},
	} {
		if err := store.RecordEvent(ctx, e); err != nil {
			t.Fatal(err)
		}
	}

	want := map[string]int64{"alpha": 1, "global": 1, domain.DefaultTenant: 1, "unknown": 0}
	for tenant, total := range want {
		got, err := store.GetSummary(ctx, tenant)
		if err != nil {
			t.Fatal(err)
		}
		if got.TotalEvents != total {
			t.Errorf("tenant %q: total = %d, want %d", tenant, got.TotalEvents, total)
		}
	}
	global, err := store.GetGlobalSummary(ctx)
	if err != nil || global.TotalEvents != 3 {
		t.Fatalf("global = %+v, %v; want 3 events", global, err)
	}
}

func TestRedisStoreRecordEventIsIdempotent(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	event := &domain.Event{ID: "dup", TenantID: "alpha", EventType: "click"}

	for i := 0; i < 3; i++ {
		if err := store.RecordEvent(ctx, event); err != nil {
			t.Fatal(err)
		}
	}
	got, err := store.GetSummary(ctx, "alpha")
	if err != nil {
		t.Fatal(err)
	}
	if got.TotalEvents != 1 || got.EventsByType["click"] != 1 || len(got.RecentEvents) != 1 {
		t.Fatalf("redelivery double-counted: %+v", got)
	}
}
