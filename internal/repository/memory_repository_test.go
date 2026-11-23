package repository

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/Arierutemplar/REPO/analytics-service/internal/domain"
)

func TestHistoryOrderAndCapacity(t *testing.T) {
	r, _ := NewMemoryRepository(3, "")
	for i := 1; i <= 5; i++ {
		_ = r.Save(context.Background(), &domain.Event{ID: fmt.Sprint(i)})
	}
	got, _ := r.GetHistory(context.Background(), 10)
	if len(got) != 3 || got[0].ID != "5" || got[2].ID != "3" {
		t.Fatalf("historial inesperado: %v", got)
	}
}

func TestDiskPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ev.jsonl")
	r, err := NewMemoryRepository(10, path)
	if err != nil {
		t.Fatal(err)
	}
	_ = r.Save(context.Background(), &domain.Event{ID: "a"})
	_ = r.Save(context.Background(), &domain.Event{ID: "b"})
	r.Close()

	r2, err := NewMemoryRepository(10, path)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	got, _ := r2.GetHistory(context.Background(), 0)
	if len(got) != 2 || got[0].ID != "b" {
		t.Fatalf("no se rehidrató: %v", got)
	}
}

func TestConcurrentSave(t *testing.T) {
	r, _ := NewMemoryRepository(100, "")
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = r.Save(context.Background(), &domain.Event{ID: "x"})
			_, _ = r.GetHistory(context.Background(), 10)
		}()
	}
	wg.Wait()
}

func TestHistoryIsScopedByTenant(t *testing.T) {
	r, _ := NewMemoryRepository(20, "")
	_ = r.Save(context.Background(), &domain.Event{ID: "a1", TenantID: "acme"})
	_ = r.Save(context.Background(), &domain.Event{ID: "b1", TenantID: "globex"})
	_ = r.Save(context.Background(), &domain.Event{ID: "a2", TenantID: "acme"})

	got, err := r.GetHistoryForTenant(context.Background(), "acme", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ID != "a2" || got[1].ID != "a1" {
		t.Fatalf("historial filtrado inesperado: %+v", got)
	}
}
