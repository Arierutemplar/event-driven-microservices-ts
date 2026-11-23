package repository

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/Arierutemplar/event-driven-microservices-ts/internal/domain"
)

var _ domain.EventRepository = (*MemoryRepository)(nil)

// MemoryRepository guarda los últimos `capacity` eventos en un buffer circular.
// Si se indica filePath, cada evento se anexa a un archivo JSON Lines y el
// buffer se rehidrata con él al arrancar.
type MemoryRepository struct {
	mu       sync.RWMutex
	buf      []*domain.Event
	capacity int
	next     int // posición de escritura
	size     int

	file *os.File
	enc  *json.Encoder
}

// NewMemoryRepository crea el repositorio. filePath vacío desactiva el disco.
func NewMemoryRepository(capacity int, filePath string) (*MemoryRepository, error) {
	if capacity < 1 {
		capacity = 1
	}
	r := &MemoryRepository{
		buf:      make([]*domain.Event, capacity),
		capacity: capacity,
	}
	if filePath == "" {
		return r, nil
	}

	if err := r.load(filePath); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("repository: abrir %s: %w", filePath, err)
	}
	r.file = f
	r.enc = json.NewEncoder(f)
	return r, nil
}

func (r *MemoryRepository) load(path string) error {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("repository: leer %s: %w", path, err)
	}
	defer func() { _ = f.Close() }() // solo lectura: un fallo al cerrar no pierde datos

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		var e domain.Event
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			continue // línea corrupta (p. ej. escritura truncada): se omite
		}
		r.push(&e)
	}
	return sc.Err()
}

// push requiere que el llamador tenga el lock de escritura (o uso exclusivo).
func (r *MemoryRepository) push(e *domain.Event) {
	r.buf[r.next] = e
	r.next = (r.next + 1) % r.capacity
	if r.size < r.capacity {
		r.size++
	}
}

func (r *MemoryRepository) Save(ctx context.Context, event *domain.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if event == nil {
		return fmt.Errorf("repository: evento nulo")
	}
	cp := *event

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.enc != nil {
		if err := r.enc.Encode(&cp); err != nil {
			return fmt.Errorf("repository: persistir en disco: %w", err)
		}
	}
	r.push(&cp)
	return nil
}

// SaveBatch inserta los eventos en orden; implementa domain.BatchEventRepository.
func (r *MemoryRepository) SaveBatch(ctx context.Context, events []*domain.Event) error {
	for _, e := range events {
		if err := r.Save(ctx, e); err != nil {
			return err
		}
	}
	return nil
}

func (r *MemoryRepository) GetHistory(ctx context.Context, limit int) ([]*domain.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	if limit <= 0 || limit > r.size {
		limit = r.size
	}
	out := make([]*domain.Event, 0, limit)
	for i := 1; i <= limit; i++ {
		idx := (r.next - i + r.capacity) % r.capacity
		cp := *r.buf[idx]
		out = append(out, &cp)
	}
	return out, nil
}

// GetHistoryForTenant devuelve los eventos recientes pertenecientes exclusivamente al tenant.
func (r *MemoryRepository) GetHistoryForTenant(ctx context.Context, tenantID string, limit int) ([]*domain.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if tenantID == "" {
		tenantID = domain.DefaultTenant
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]*domain.Event, 0)
	if limit <= 0 {
		limit = r.size
	}
	for i := 1; i <= r.size && len(out) < limit; i++ {
		idx := (r.next - i + r.capacity) % r.capacity
		e := r.buf[idx]
		if e.TenantID != tenantID {
			continue
		}
		cp := *e
		out = append(out, &cp)
	}
	return out, nil
}

// Close libera el archivo de persistencia, si existe.
func (r *MemoryRepository) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.file == nil {
		return nil
	}
	err := r.file.Close()
	r.file, r.enc = nil, nil
	return err
}
