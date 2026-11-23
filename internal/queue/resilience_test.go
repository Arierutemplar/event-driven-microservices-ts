package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/Arierutemplar/event-driven-microservices-ts/internal/dedup"
	"github.com/Arierutemplar/event-driven-microservices-ts/internal/domain"
	"github.com/Arierutemplar/event-driven-microservices-ts/internal/repository"
	"github.com/Arierutemplar/event-driven-microservices-ts/internal/resilience"
)

// pingRepo añade un Ping controlable a fakeRepo para ejercitar el breaker.
type pingRepo struct {
	*fakeRepo
	down atomic.Bool
}

func (p *pingRepo) Ping(context.Context) error {
	if p.down.Load() {
		return errors.New("db caída")
	}
	return nil
}

// startManual levanta un Client aislado como startClient, pero deja que el test configure
// Gate/Dedup y controle cuándo se detiene el consumer.
func startManual(t *testing.T, repo domain.BatchEventRepository, mutate func(*Config), setup func(*Client)) (c *Client, stop context.CancelFunc, done <-chan struct{}) {
	t.Helper()
	url := os.Getenv("TEST_NATS_URL")
	if url == "" {
		t.Skip("TEST_NATS_URL no definido")
	}

	id := fmt.Sprintf("T%d", rand.Int63())
	cfg := Config{
		URL: url, Stream: id, Subject: id + ".ingest", DLQSubject: id + ".dlq", Durable: id + "-w",
		BatchSize: 10, FlushInterval: 200 * time.Millisecond, MaxDeliver: 3, RetryDelay: 50 * time.Millisecond,
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
	if setup != nil {
		setup(c)
	}

	runCtx, stopFn := context.WithCancel(context.Background())
	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		c.RunConsumer(runCtx, repo, nil)
	}()

	t.Cleanup(func() {
		stopFn()
		<-doneCh
		dctx, dcancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer dcancel()
		_ = c.js.DeleteStream(dctx, cfg.Stream)
		_ = c.js.DeleteStream(dctx, cfg.Stream+"_DLQ")
		_ = c.Close()
	})
	return c, stopFn, doneCh
}

// rawPublish publica saltándose la deduplicación del broker (Nats-Msg-Id distinto),
// como ocurriría con un reenvío fuera de la ventana de JetStream.
func rawPublish(t *testing.T, c *Client, eventID string) {
	t.Helper()
	data, _ := json.Marshal(&domain.Event{ID: eventID, EventType: "click", UserID: eventID})
	msg := &nats.Msg{Subject: c.cfg.Subject, Data: data, Header: nats.Header{}}
	msg.Header.Set(nats.MsgIdHdr, newMsgID())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := c.js.PublishMsg(ctx, msg); err != nil {
		t.Fatalf("rawPublish: %v", err)
	}
}

func (f *fakeRepo) savedIDs() map[string]int {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]int, len(f.saved))
	for _, e := range f.saved {
		out[e.ID]++
	}
	return out
}

func TestBreakerPausesConsumptionWithoutBurningDeliveries(t *testing.T) {
	pr := &pingRepo{fakeRepo: &fakeRepo{}}
	pr.fail = func([]*domain.Event) error {
		if pr.down.Load() {
			return errors.New("db caída")
		}
		return nil
	}
	guarded := repository.NewBreakerRepository(pr, resilience.Config{
		Name: "test-consumer", ConsecutiveFailures: 2, OpenTimeout: 300 * time.Millisecond,
	})

	c, _, _ := startManual(t, guarded,
		func(cfg *Config) { cfg.BatchSize = 1 }, // cada mensaje es su propio lote
		func(c *Client) { c.Gate = guarded.WaitReady })

	pr.down.Store(true)
	for i := 0; i < 8; i++ {
		publish(t, c, fmt.Sprint("ev-", i))
	}

	eventually(t, "circuito abierto", func() bool { return guarded.State() != resilience.Closed })
	time.Sleep(300 * time.Millisecond) // deja asentarse cualquier lectura en vuelo
	before := pr.calls.Load()
	time.Sleep(1500 * time.Millisecond) // varios ciclos de OpenTimeout con la base caída
	if after := pr.calls.Load(); after != before {
		t.Fatalf("el consumo debía estar pausado: inserciones %d -> %d", before, after)
	}
	if before > 3 {
		t.Fatalf("solo debieron intentarse los lotes que abrieron el circuito, hubo %d", before)
	}
	if n := streamMsgs(t, c, c.cfg.Stream+"_DLQ"); n != 0 {
		t.Fatalf("la pausa no debe enviar mensajes a la DLQ (hay %d)", n)
	}

	pr.down.Store(false)
	eventually(t, "los 8 eventos persistidos tras recuperarse", func() bool { return pr.savedCount() == 8 })
	eventually(t, "stream principal vacío", func() bool { return streamMsgs(t, c, c.cfg.Stream) == 0 })
	if n := streamMsgs(t, c, c.cfg.Stream+"_DLQ"); n != 0 {
		t.Fatalf("DLQ = %d, quería 0", n)
	}
	for id, n := range pr.savedIDs() {
		if n != 1 {
			t.Errorf("evento %s persistido %d veces", id, n)
		}
	}
}

func TestConsumerDropsDuplicateEventIDs(t *testing.T) {
	repo := &fakeRepo{}
	c, _, _ := startManual(t, repo, nil, func(c *Client) { c.Dedup = dedup.New(time.Minute, 1000) })

	// a y b repetidos dentro del mismo lote
	for _, id := range []string{"a", "b", "a", "c", "b"} {
		rawPublish(t, c, id)
	}
	eventually(t, "3 eventos únicos", func() bool { return repo.savedCount() >= 3 })
	eventually(t, "stream vacío", func() bool { return streamMsgs(t, c, c.cfg.Stream) == 0 })

	// y reenvíos posteriores, ya en otro lote
	rawPublish(t, c, "a")
	rawPublish(t, c, "d")
	eventually(t, "d persistido", func() bool { return repo.savedIDs()["d"] == 1 })
	eventually(t, "stream vacío", func() bool { return streamMsgs(t, c, c.cfg.Stream) == 0 })

	ids := repo.savedIDs()
	if len(ids) != 4 || repo.savedCount() != 4 {
		t.Fatalf("persistidos = %v (total %d), quería a,b,c,d una vez cada uno", ids, repo.savedCount())
	}
}

func TestBrokerDedupsRepeatedPublish(t *testing.T) {
	repo := &fakeRepo{}
	c, _, _ := startManual(t, repo, nil, nil)

	for i := 0; i < 5; i++ {
		publish(t, c, "same-id") // mismo event.ID => mismo Nats-Msg-Id
	}
	eventually(t, "1 evento persistido", func() bool { return repo.savedCount() >= 1 })
	time.Sleep(500 * time.Millisecond)
	if repo.savedCount() != 1 {
		t.Fatalf("persistidos = %d, quería 1", repo.savedCount())
	}
}

func TestFailedBatchReleasesIDForRedelivery(t *testing.T) {
	var attempts atomic.Int32
	repo := &fakeRepo{fail: func([]*domain.Event) error {
		if attempts.Add(1) == 1 {
			return errors.New("fallo transitorio")
		}
		return nil
	}}
	c, _, _ := startManual(t, repo, nil, func(c *Client) { c.Dedup = dedup.New(time.Minute, 1000) })

	publish(t, c, "retry-me")

	// Si el id siguiera reservado tras el fallo, la reentrega se descartaría como duplicado y se perdería
	eventually(t, "evento persistido en la reentrega", func() bool { return repo.savedIDs()["retry-me"] == 1 })
	eventually(t, "stream vacío", func() bool { return streamMsgs(t, c, c.cfg.Stream) == 0 })
	if n := streamMsgs(t, c, c.cfg.Stream+"_DLQ"); n != 0 {
		t.Fatalf("DLQ = %d, quería 0", n)
	}
}

func TestShutdownFlushesInFlightBatch(t *testing.T) {
	repo := &fakeRepo{}

	// Ventana de lote larga: al pedir el apagado el lote sigue abierto y debe volcarse igual
	c, stop, done := startManual(t, repo,
		func(cfg *Config) { cfg.BatchSize = 100; cfg.FlushInterval = 1500 * time.Millisecond }, nil)

	for i := 0; i < 7; i++ {
		publish(t, c, fmt.Sprint("sd-", i))
	}
	time.Sleep(100 * time.Millisecond) // el Fetch ya tiene los mensajes en su lote
	stop()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("el consumer no terminó tras la cancelación")
	}
	if repo.savedCount() != 7 {
		t.Fatalf("persistidos = %d, quería 7: el lote en curso debe volcarse antes de salir", repo.savedCount())
	}
	if n := streamMsgs(t, c, c.cfg.Stream); n != 0 {
		t.Fatalf("stream principal = %d, quería 0 (todo confirmado)", n)
	}
}

func TestTenantPropagatesThroughJetStreamAndIsolatesIDs(t *testing.T) {
	repo := &fakeRepo{}
	c, _, _ := startManual(t, repo, nil, func(c *Client) { c.Dedup = dedup.New(time.Minute, 1000) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pub := func(tenant, id string) {
		if err := c.Publish(ctx, &domain.Event{ID: id, TenantID: tenant, EventType: "click"}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	pub("tenant-a", "same")
	pub("tenant-b", "same") // mismo id, otro tenant: no debe tratarse como duplicado
	pub("tenant-a", "same") // duplicado real: lo descarta el broker

	eventually(t, "2 eventos (uno por tenant)", func() bool { return repo.savedCount() >= 2 })
	time.Sleep(300 * time.Millisecond)

	repo.mu.Lock()
	defer repo.mu.Unlock()
	got := map[string]int{}
	for _, e := range repo.saved {
		got[e.TenantID]++
	}
	if len(repo.saved) != 2 || got["tenant-a"] != 1 || got["tenant-b"] != 1 {
		t.Fatalf("persistidos por tenant = %v (total %d), quería a:1 b:1", got, len(repo.saved))
	}
}

// Si el agregado falla, el lote vuelve a la cola y se reintenta hasta contarse (sin ack previo).
func TestAggregateFailureRedeliversBatch(t *testing.T) {
	var calls atomic.Int32
	counted := make(chan struct{}, 1)
	c, _, _ := startManual(t, &fakeRepo{}, nil, func(c *Client) {
		c.Aggregate = func(context.Context, []*domain.Event) error {
			if calls.Add(1) == 1 {
				return errors.New("redis caído")
			}
			select {
			case counted <- struct{}{}:
			default:
			}
			return nil
		}
	})
	rawPublish(t, c, "agg-1")

	select {
	case <-counted:
	case <-time.After(10 * time.Second):
		t.Fatalf("el agregado no se reintentó tras el fallo (llamadas=%d)", calls.Load())
	}
}
