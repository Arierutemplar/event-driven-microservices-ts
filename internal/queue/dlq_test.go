package queue

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Arierutemplar/event-driven-microservices-ts/internal/domain"
)

// fakeRepo implementa domain.BatchEventRepository con un comportamiento configurable.
type fakeRepo struct {
	mu    sync.Mutex
	saved []*domain.Event
	calls atomic.Int64
	fail  func(batch []*domain.Event) error
}

func (f *fakeRepo) Save(ctx context.Context, e *domain.Event) error {
	return f.SaveBatch(ctx, []*domain.Event{e})
}

func (f *fakeRepo) GetHistory(context.Context, int) ([]*domain.Event, error) { return nil, nil }

func (f *fakeRepo) SaveBatch(_ context.Context, batch []*domain.Event) error {
	f.calls.Add(1)
	if f.fail != nil {
		if err := f.fail(batch); err != nil {
			return err
		}
	}
	f.mu.Lock()
	f.saved = append(f.saved, batch...)
	f.mu.Unlock()
	return nil
}

func (f *fakeRepo) savedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.saved)
}

// startClient levanta un Client aislado (stream, DLQ y consumer con nombres únicos)
// y su consumer en segundo plano.
func startClient(t *testing.T, repo domain.BatchEventRepository, mutate func(*Config)) *Client {
	t.Helper()
	url := os.Getenv("TEST_NATS_URL")
	if url == "" {
		t.Skip("TEST_NATS_URL no definido")
	}

	id := fmt.Sprintf("T%d", rand.Int63())
	cfg := Config{
		URL:           url,
		Stream:        id,
		Subject:       id + ".ingest",
		DLQSubject:    id + ".dlq",
		Durable:       id + "-w",
		BatchSize:     10,
		FlushInterval: 200 * time.Millisecond,
		MaxDeliver:    3,
		RetryDelay:    50 * time.Millisecond,
	}
	if mutate != nil {
		mutate(&cfg)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := Connect(ctx, cfg)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}

	runCtx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.RunConsumer(runCtx, repo, nil)
	}()

	t.Cleanup(func() {
		stop()
		<-done
		dctx, dcancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer dcancel()
		_ = c.js.DeleteStream(dctx, cfg.Stream)
		_ = c.js.DeleteStream(dctx, cfg.Stream+"_DLQ")
		_ = c.Close()
	})
	return c
}

func publish(t *testing.T, c *Client, user string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Publish(ctx, &domain.Event{ID: user, EventType: "click", UserID: user}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timeout esperando: %s", what)
}

func streamMsgs(t *testing.T, c *Client, name string) uint64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	s, err := c.js.Stream(ctx, name)
	if err != nil {
		t.Fatalf("Stream %s: %v", name, err)
	}
	info, err := s.Info(ctx)
	if err != nil {
		t.Fatalf("Info %s: %v", name, err)
	}
	return info.State.Msgs
}

func firstDLQ(t *testing.T, c *Client) *jetstream.RawStreamMsg {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	s, err := c.js.Stream(ctx, c.cfg.Stream+"_DLQ")
	if err != nil {
		t.Fatalf("stream DLQ: %v", err)
	}
	raw, err := s.GetMsg(ctx, 1)
	if err != nil {
		t.Fatalf("GetMsg DLQ: %v", err)
	}
	return raw
}

func TestDLQPermanentErrorIsolatedAndAcked(t *testing.T) {
	repo := &fakeRepo{fail: func(batch []*domain.Event) error {
		for _, e := range batch {
			if e.UserID == "poison" {
				return fmt.Errorf("boom: %w", domain.ErrPermanent)
			}
		}
		return nil
	}}
	c := startClient(t, repo, nil)

	publish(t, c, "ok-1")
	publish(t, c, "poison")
	publish(t, c, "ok-2")

	eventually(t, "2 eventos buenos guardados", func() bool { return repo.savedCount() == 2 })
	eventually(t, "1 mensaje en la DLQ", func() bool { return streamMsgs(t, c, c.cfg.Stream+"_DLQ") == 1 })
	eventually(t, "stream principal vacío (original confirmado)", func() bool { return streamMsgs(t, c, c.cfg.Stream) == 0 })

	raw := firstDLQ(t, c)
	if got := raw.Header.Get(HdrDLQReason); got != ReasonPermanent {
		t.Errorf("reason = %q, want %q", got, ReasonPermanent)
	}
	if raw.Header.Get(HdrDLQError) == "" || raw.Header.Get(HdrDLQTimestamp) == "" {
		t.Errorf("faltan metadatos: %v", raw.Header)
	}
	if _, err := time.Parse(time.RFC3339Nano, raw.Header.Get(HdrDLQTimestamp)); err != nil {
		t.Errorf("timestamp inválido: %v", err)
	}
	if raw.Header.Get(nats.MsgIdHdr) == "" {
		t.Error("la DLQ debe conservar Nats-Msg-Id")
	}
}

func TestDLQAfterMaxDeliveries(t *testing.T) {
	repo := &fakeRepo{fail: func([]*domain.Event) error { return errors.New("db caída") }}
	c := startClient(t, repo, nil)

	publish(t, c, "x")

	eventually(t, "mensaje en la DLQ", func() bool { return streamMsgs(t, c, c.cfg.Stream+"_DLQ") == 1 })
	eventually(t, "stream principal vacío", func() bool { return streamMsgs(t, c, c.cfg.Stream) == 0 })

	if got := repo.calls.Load(); got != 3 {
		t.Errorf("intentos de inserción = %d, want 3 (MaxDeliver)", got)
	}
	raw := firstDLQ(t, c)
	if got := raw.Header.Get(HdrDLQReason); got != ReasonMaxDeliveries {
		t.Errorf("reason = %q, want %q", got, ReasonMaxDeliveries)
	}
	if got := raw.Header.Get(HdrDLQDeliveries); got != "3" {
		t.Errorf("entregas = %q, want 3", got)
	}
}

func TestDLQUndecodableMessage(t *testing.T) {
	repo := &fakeRepo{}
	c := startClient(t, repo, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := c.js.Publish(ctx, c.cfg.Subject, []byte("{no es json")); err != nil {
		t.Fatal(err)
	}

	eventually(t, "mensaje ilegible en la DLQ", func() bool { return streamMsgs(t, c, c.cfg.Stream+"_DLQ") == 1 })
	if got := firstDLQ(t, c).Header.Get(HdrDLQReason); got != ReasonDecode {
		t.Errorf("reason = %q, want %q", got, ReasonDecode)
	}
	if repo.savedCount() != 0 {
		t.Error("no debe guardar nada")
	}
}

func TestMaxDeliveriesAdvisoryMovesStuckMessage(t *testing.T) {
	// Un consumer externo (sin worker) agota las entregas sin ack: el advisory
	// debe mover el mensaje a la DLQ y borrarlo del stream principal.
	url := os.Getenv("TEST_NATS_URL")
	if url == "" {
		t.Skip("TEST_NATS_URL no definido")
	}
	repo := &fakeRepo{}
	c := startClient(t, repo, func(cfg *Config) { cfg.AckWait = 200 * time.Millisecond })

	// Sustituimos al worker: detenemos el consumo real borrando su consumer y
	// creando otro que nunca confirma.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stuck, err := c.stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable: c.cfg.Durable, FilterSubject: c.cfg.Subject,
		AckPolicy: jetstream.AckExplicitPolicy, AckWait: 200 * time.Millisecond, MaxDeliver: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = stuck
	// El worker de RunConsumer sigue activo sobre el mismo durable; para aislar el
	// caso, hacemos que su inserción tarde más que AckWait*MaxDeliver.
	repo.fail = func([]*domain.Event) error { time.Sleep(time.Second); return errors.New("lento") }

	publish(t, c, "stuck")
	eventually(t, "mensaje en la DLQ", func() bool { return streamMsgs(t, c, c.cfg.Stream+"_DLQ") >= 1 })
	eventually(t, "stream principal vacío", func() bool { return streamMsgs(t, c, c.cfg.Stream) == 0 })
}
