// Package integration contiene pruebas de extremo a extremo del microservicio.
//
//   - TestConcurrentDirectMode no necesita infraestructura (se ejecuta siempre).
//   - Los tests de NATS/PostgreSQL requieren TEST_NATS_URL y TEST_POSTGRES_DSN y se
//     omiten si no están definidos.
//
// Ejemplo (con `docker compose up -d nats postgres`):
//
//	TEST_NATS_URL=nats://localhost:4222 \
//	TEST_POSTGRES_DSN=postgres://postgres:postgres@localhost:5433/analytics?sslmode=disable \
//	go test -race -count=1 ./internal/integration/...
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Arierutemplar/event-driven-microservices-ts/internal/domain"
	"github.com/Arierutemplar/event-driven-microservices-ts/internal/handler"
	"github.com/Arierutemplar/event-driven-microservices-ts/internal/queue"
	"github.com/Arierutemplar/event-driven-microservices-ts/internal/repository"
	"github.com/Arierutemplar/event-driven-microservices-ts/internal/usecase"
)

// ---------------------------------------------------------------------------
// Infraestructura de prueba
// ---------------------------------------------------------------------------

// countingRepo registra el tamaño de cada lote insertado.
type countingRepo struct {
	domain.BatchEventRepository
	mu      sync.Mutex
	batches []int
}

func (c *countingRepo) SaveBatch(ctx context.Context, evs []*domain.Event) error {
	if err := c.BatchEventRepository.SaveBatch(ctx, evs); err != nil {
		return err
	}
	c.mu.Lock()
	c.batches = append(c.batches, len(evs))
	c.mu.Unlock()
	return nil
}

func (c *countingRepo) stats() (n, sum, max int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, b := range c.batches {
		sum += b
		if b > max {
			max = b
		}
	}
	return len(c.batches), sum, max
}

// app replica la composición de cmd/api/main.go sobre un httptest.Server.
type app struct {
	svc *usecase.AnalyticsService
	srv *httptest.Server
	ws  *handler.WSHandler
}

func newApp(t *testing.T, svc *usecase.AnalyticsService) *app {
	t.Helper()
	h := handler.NewAnalyticsHandler(svc)
	ws := handler.NewWSHandler()
	go ws.HandleMessages()

	mux := http.NewServeMux()
	mux.HandleFunc("/api/events", h.HandleIngest)
	mux.HandleFunc("/api/metrics", h.HandleSummary)
	mux.HandleFunc("/api/events/history", h.HandleHistory)
	mux.HandleFunc("/ws", ws.HandleConnections)

	a := &app{svc: svc, srv: httptest.NewServer(mux), ws: ws}
	t.Cleanup(a.srv.Close)
	return a
}

func (a *app) broadcastSummary() {
	data, _ := json.Marshal(a.svc.GetSummary())
	a.ws.Broadcast(data)
}

func (a *app) post(client *http.Client, ev map[string]any) (int, error) {
	body, _ := json.Marshal(ev)
	resp, err := client.Post(a.srv.URL+"/api/events", "application/json", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	resp.Body.Close()
	return resp.StatusCode, nil
}

func (a *app) wsURL() string { return "ws" + strings.TrimPrefix(a.srv.URL, "http") + "/ws" }

func (a *app) dialWS(t *testing.T) *websocket.Conn {
	t.Helper()
	c, _, err := websocket.DefaultDialer.Dial(a.wsURL(), nil)
	if err != nil {
		t.Fatalf("dial ws: %v", err)
	}
	return c
}

// watchWS lee mensajes del WebSocket y reporta el mayor total_events visto.
func watchWS(c *websocket.Conn) *atomic.Int64 {
	maxTotal := new(atomic.Int64)
	go func() {
		for {
			_, data, err := c.ReadMessage()
			if err != nil {
				return
			}
			var s domain.MetricSummary
			if json.Unmarshal(data, &s) == nil {
				for {
					cur := maxTotal.Load()
					if s.TotalEvents <= cur || maxTotal.CompareAndSwap(cur, s.TotalEvents) {
						break
					}
				}
			}
		}
	}()
	return maxTotal
}

func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timeout esperando: %s", what)
}

// event genera un evento alternando payload de texto, objeto anidado y ausente.
func event(runID string, i int) map[string]any {
	ev := map[string]any{
		"id":         fmt.Sprintf("%s-%d", runID, i),
		"event_type": []string{"click", "page_view", "purchase"}[i%3],
		"user_id":    fmt.Sprintf("%s-u%d", runID, i),
	}
	switch i % 3 {
	case 0:
		ev["payload"] = fmt.Sprintf("texto-%d", i)
	case 1:
		ev["payload"] = map[string]any{"n": i, "nested": map[string]any{"ok": true, "tags": []string{"a", "b"}}}
	}
	return ev
}

// churnWS conecta y desconecta clientes WebSocket y lee el resumen hasta que stop se cierre.
func churnWS(a *app, stop <-chan struct{}, workers int, bg *sync.WaitGroup) {
	for i := 0; i < workers; i++ {
		bg.Add(1)
		go func() {
			defer bg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if c, _, err := websocket.DefaultDialer.Dial(a.wsURL(), nil); err == nil {
					time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)
					c.Close()
				}
				s := a.svc.GetSummary()
				s.EventsByType["mutar-copia"]++ // GetSummary debe devolver una copia
			}
		}()
	}
}

// ---------------------------------------------------------------------------
// Modo directo (sin infraestructura): worker pool + WebSocket bajo carga
// ---------------------------------------------------------------------------

type slowRepo struct{ saved atomic.Int64 }

func (r *slowRepo) Save(context.Context, *domain.Event) error {
	time.Sleep(time.Millisecond)
	r.saved.Add(1)
	return nil
}
func (r *slowRepo) GetHistory(context.Context, int) ([]*domain.Event, error) { return nil, nil }

// Cientos de peticiones simultáneas contra el worker pool mientras clientes WebSocket
// se conectan/desconectan y otras goroutines leen el resumen. Ejecutar con -race.
func TestConcurrentDirectMode(t *testing.T) {
	const (
		producers   = 100
		perProducer = 10
	)
	repo := &slowRepo{}
	// Buffer pequeño: fuerza 503 (ErrQueueFull) y comprueba que no hay bloqueos.
	svc := usecase.NewAnalyticsService(64, repo)
	svc.StartWorkerPool(8)
	a := newApp(t, svc)

	stable := make([]*atomic.Int64, 3)
	for i := range stable {
		c := a.dialWS(t)
		stable[i] = watchWS(c)
		t.Cleanup(func() { c.Close() })
	}

	stop := make(chan struct{})
	var bg sync.WaitGroup
	churnWS(a, stop, 5, &bg)

	var accepted, rejected, failed atomic.Int64
	var wg sync.WaitGroup
	client := &http.Client{Timeout: 10 * time.Second}
	start := make(chan struct{})
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			<-start
			for i := 0; i < perProducer; i++ {
				code, err := a.post(client, event("direct", p*perProducer+i))
				switch {
				case err != nil:
					failed.Add(1)
				case code == http.StatusAccepted:
					accepted.Add(1)
					a.broadcastSummary()
				case code == http.StatusServiceUnavailable:
					rejected.Add(1)
				default:
					failed.Add(1)
				}
			}
		}(p)
	}
	close(start)
	wg.Wait()

	if failed.Load() != 0 {
		t.Fatalf("%d peticiones fallaron con error o estado inesperado", failed.Load())
	}
	if accepted.Load()+rejected.Load() != producers*perProducer {
		t.Fatalf("accepted+rejected = %d, want %d", accepted.Load()+rejected.Load(), producers*perProducer)
	}
	if accepted.Load() == 0 {
		t.Fatal("ningún evento aceptado")
	}

	// Stop drena la cola: todo lo aceptado debe quedar procesado y persistido
	done := make(chan struct{})
	go func() { svc.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("Stop() no retornó: posible bloqueo en el worker pool")
	}
	close(stop)
	bg.Wait()

	sum := svc.GetSummary()
	if sum.TotalEvents != accepted.Load() {
		t.Errorf("TotalEvents = %d, want %d (aceptados)", sum.TotalEvents, accepted.Load())
	}
	if got := repo.saved.Load(); got != accepted.Load() {
		t.Errorf("persistidos = %d, want %d", got, accepted.Load())
	}
	var byType int64
	for typ, n := range sum.EventsByType {
		if typ == "mutar-copia" {
			t.Error("GetSummary devolvió el mapa interno en lugar de una copia")
		}
		byType += n
	}
	if byType != accepted.Load() {
		t.Errorf("suma de events_by_type = %d, want %d", byType, accepted.Load())
	}

	a.broadcastSummary()
	eventually(t, 5*time.Second, "clientes WS estables reciben el total final", func() bool {
		for _, m := range stable {
			if m.Load() != accepted.Load() {
				return false
			}
		}
		return true
	})
	t.Logf("aceptados=%d rechazados(503)=%d", accepted.Load(), rejected.Load())
}

// ---------------------------------------------------------------------------
// NATS + PostgreSQL reales
// ---------------------------------------------------------------------------

type stack struct {
	*app
	repo  *countingRepo
	pool  *pgxpool.Pool
	cfg   queue.Config
	runID string
}

func newStack(t *testing.T, batchSize int, flush time.Duration) *stack {
	t.Helper()
	natsURL, dsn := os.Getenv("TEST_NATS_URL"), os.Getenv("TEST_POSTGRES_DSN")
	if natsURL == "" || dsn == "" {
		t.Skip("TEST_NATS_URL y TEST_POSTGRES_DSN no definidos")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pg, err := repository.NewPostgresRepository(ctx, dsn)
	if err != nil {
		t.Fatalf("postgres: %v", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}

	runID := fmt.Sprintf("it%d", rand.Int63())
	cfg := queue.Config{
		URL:           natsURL,
		Stream:        strings.ToUpper(runID),
		Subject:       runID + ".ingest",
		DLQSubject:    runID + ".dlq",
		Durable:       runID + "-writer",
		BatchSize:     batchSize,
		FlushInterval: flush,
		RetryDelay:    100 * time.Millisecond,
	}
	qc, err := queue.Connect(ctx, cfg)
	if err != nil {
		t.Fatalf("nats: %v", err)
	}

	repo := &countingRepo{BatchEventRepository: pg}
	svc := usecase.NewAnalyticsService(16, repo).WithPublisher(qc)
	a := newApp(t, svc)

	runCtx, stop := context.WithCancel(context.Background())
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		qc.RunConsumer(runCtx, repo, func(evs []*domain.Event) {
			svc.RecordProcessed(evs)
			a.broadcastSummary()
		})
	}()

	t.Cleanup(func() {
		stop()
		<-consumerDone
		cctx, ccancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer ccancel()
		_, _ = pool.Exec(cctx, "DELETE FROM events WHERE user_id LIKE $1", runID+"%")
		if nc, err := nats.Connect(natsURL); err == nil {
			if js, err := jetstream.New(nc); err == nil {
				_ = js.DeleteStream(cctx, cfg.Stream)
				_ = js.DeleteStream(cctx, cfg.Stream+"_DLQ")
			}
			nc.Close()
		}
		_ = qc.Close()
		pool.Close()
		_ = pg.Close()
	})
	return &stack{app: a, repo: repo, pool: pool, cfg: cfg, runID: runID}
}

func (s *stack) rowCount(t *testing.T) (rows, distinctMsg int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := s.pool.QueryRow(ctx,
		"SELECT count(*), count(DISTINCT msg_id) FROM events WHERE user_id LIKE $1", s.runID+"%").
		Scan(&rows, &distinctMsg)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	return
}

// Ciclo completo: HTTP → publish (JetStream) → consumer → INSERT por lotes → resumen → WS.
func TestFullCycleHTTPToPostgresInBatches(t *testing.T) {
	const total = 240
	s := newStack(t, 50, 300*time.Millisecond)

	ws := s.dialWS(t)
	defer ws.Close()
	wsMax := watchWS(ws)

	client := &http.Client{Timeout: 10 * time.Second}
	for i := 0; i < total; i++ {
		code, err := s.post(client, event(s.runID, i))
		if err != nil || code != http.StatusAccepted {
			t.Fatalf("POST %d: code=%d err=%v", i, code, err)
		}
	}

	eventually(t, 20*time.Second, "filas en PostgreSQL", func() bool {
		rows, _ := s.rowCount(t)
		return rows == total
	})
	rows, distinct := s.rowCount(t)
	if rows != total || distinct != total {
		t.Fatalf("filas=%d msg_id distintos=%d, want %d", rows, distinct, total)
	}

	// Inserción por lotes: pocas llamadas, ninguna por encima de BatchSize
	n, sum, max := s.repo.stats()
	if sum != total {
		t.Errorf("suma de lotes = %d, want %d", sum, total)
	}
	if max > s.cfg.BatchSize {
		t.Errorf("lote de %d > BatchSize %d", max, s.cfg.BatchSize)
	}
	if n >= total {
		t.Errorf("%d lotes para %d eventos: no hubo agrupación", n, total)
	}
	t.Logf("%d eventos en %d lotes (máx %d)", total, n, max)

	// Payloads: texto, objeto anidado y ausente sobreviven el viaje completo
	ctx := context.Background()
	var p0, p1 []byte
	if err := s.pool.QueryRow(ctx, "SELECT payload FROM events WHERE event_id=$1", fmt.Sprintf("%s-0", s.runID)).Scan(&p0); err != nil {
		t.Fatal(err)
	}
	if string(p0) != `"texto-0"` {
		t.Errorf("payload texto = %s", p0)
	}
	if err := s.pool.QueryRow(ctx, "SELECT payload FROM events WHERE event_id=$1", fmt.Sprintf("%s-1", s.runID)).Scan(&p1); err != nil {
		t.Fatal(err)
	}
	var obj struct {
		N      int `json:"n"`
		Nested struct {
			OK   bool     `json:"ok"`
			Tags []string `json:"tags"`
		} `json:"nested"`
	}
	if err := json.Unmarshal(p1, &obj); err != nil || obj.N != 1 || !obj.Nested.OK || len(obj.Nested.Tags) != 2 {
		t.Errorf("payload objeto = %s (%v)", p1, err)
	}
	var nullPayload bool
	if err := s.pool.QueryRow(ctx, "SELECT payload IS NULL OR payload = 'null'::jsonb FROM events WHERE event_id=$1",
		fmt.Sprintf("%s-2", s.runID)).Scan(&nullPayload); err != nil || !nullPayload {
		t.Errorf("payload ausente debería ser NULL/null (err=%v)", err)
	}

	// Resumen y WebSocket
	eventually(t, 5*time.Second, "resumen con el total", func() bool {
		return s.svc.GetSummary().TotalEvents == total
	})
	summary := s.svc.GetSummary()
	for _, typ := range []string{"click", "page_view", "purchase"} {
		if summary.EventsByType[typ] != total/3 {
			t.Errorf("events_by_type[%s] = %d, want %d", typ, summary.EventsByType[typ], total/3)
		}
	}
	eventually(t, 5*time.Second, "difusión WS del total final", func() bool { return wsMax.Load() == total })

	// El historial HTTP lee de PostgreSQL
	resp, err := client.Get(s.srv.URL + "/api/events/history?limit=1000")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var hist []domain.Event
	if err := json.NewDecoder(resp.Body).Decode(&hist); err != nil {
		t.Fatalf("history: %v", err)
	}
	found := 0
	for _, e := range hist {
		if strings.HasPrefix(e.UserID, s.runID) {
			found++
		}
	}
	if found == 0 {
		t.Error("el historial no contiene los eventos de la prueba")
	}
}

// Ventana de tiempo: menos eventos que BatchSize se vuelcan igualmente al vencer el flush.
func TestFlushWindowWritesPartialBatch(t *testing.T) {
	s := newStack(t, 1000, 200*time.Millisecond)
	client := &http.Client{Timeout: 10 * time.Second}
	for i := 0; i < 7; i++ {
		if code, err := s.post(client, event(s.runID, i)); err != nil || code != http.StatusAccepted {
			t.Fatalf("POST: %d %v", code, err)
		}
	}
	eventually(t, 10*time.Second, "7 filas por ventana de tiempo", func() bool {
		rows, _ := s.rowCount(t)
		return rows == 7
	})
}

// Carga concurrente sobre el camino completo: sin pérdidas, sin duplicados, sin bloqueos.
func TestConcurrentLoadThroughNATSAndPostgres(t *testing.T) {
	const (
		producers   = 50
		perProducer = 10
		total       = producers * perProducer
	)
	s := newStack(t, 100, 250*time.Millisecond)

	wsClients := make([]*atomic.Int64, 3)
	for i := range wsClients {
		c := s.dialWS(t)
		wsClients[i] = watchWS(c)
		t.Cleanup(func() { c.Close() })
	}
	stop := make(chan struct{})
	var bg sync.WaitGroup
	churnWS(s.app, stop, 4, &bg)

	var ok, bad atomic.Int64
	var wg sync.WaitGroup
	client := &http.Client{Timeout: 15 * time.Second}
	start := make(chan struct{})
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			<-start
			for i := 0; i < perProducer; i++ {
				code, err := s.post(client, event(s.runID, p*perProducer+i))
				if err == nil && code == http.StatusAccepted {
					ok.Add(1)
				} else {
					bad.Add(1)
				}
			}
		}(p)
	}
	close(start)
	wg.Wait()

	if bad.Load() != 0 || ok.Load() != total {
		t.Fatalf("aceptados=%d fallidos=%d, want %d/0", ok.Load(), bad.Load(), total)
	}

	eventually(t, 30*time.Second, "todas las filas persistidas", func() bool {
		rows, _ := s.rowCount(t)
		return rows == total
	})
	rows, distinct := s.rowCount(t)
	if rows != total || distinct != total {
		t.Errorf("filas=%d distintos=%d, want %d (pérdida o duplicados)", rows, distinct, total)
	}
	eventually(t, 10*time.Second, "resumen y WS convergen", func() bool {
		if s.svc.GetSummary().TotalEvents != total {
			return false
		}
		for _, m := range wsClients {
			if m.Load() != total {
				return false
			}
		}
		return true
	})
	close(stop)
	bg.Wait()

	n, sum, _ := s.repo.stats()
	t.Logf("%d eventos en %d lotes", sum, n)
}
