package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Arierutemplar/REPO/analytics-service/internal/dedup"
	"github.com/Arierutemplar/REPO/analytics-service/internal/distributed"
	"github.com/Arierutemplar/REPO/analytics-service/internal/domain"
	"github.com/Arierutemplar/REPO/analytics-service/internal/handler"
	"github.com/Arierutemplar/REPO/analytics-service/internal/middleware"
	"github.com/Arierutemplar/REPO/analytics-service/internal/observability"
	"github.com/Arierutemplar/REPO/analytics-service/internal/queue"
	"github.com/Arierutemplar/REPO/analytics-service/internal/repository"
	"github.com/Arierutemplar/REPO/analytics-service/internal/resilience"
	"github.com/Arierutemplar/REPO/analytics-service/internal/usecase"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// statusRecorder captura el código HTTP escrito por el handler envuelto.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// Los helpers env* devuelven def cuando la variable falta; si existe pero no es válida
// (o no es positiva) avisan y también usan def.
func envFloat(name string, def float64) float64 {
	raw := os.Getenv(name)
	if raw == "" {
		return def
	}
	if v, err := strconv.ParseFloat(raw, 64); err == nil && v > 0 {
		return v
	}
	slog.Warn("variable de entorno inválida, usando valor por defecto", "var", name, "valor", raw, "defecto", def)
	return def
}

func envInt(name string, def int) int {
	raw := os.Getenv(name)
	if raw == "" {
		return def
	}
	if v, err := strconv.Atoi(raw); err == nil && v > 0 {
		return v
	}
	slog.Warn("variable de entorno inválida, usando valor por defecto", "var", name, "valor", raw, "defecto", def)
	return def
}

func envString(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func envDuration(name string, def time.Duration) time.Duration {
	raw := os.Getenv(name)
	if raw == "" {
		return def
	}
	if v, err := time.ParseDuration(raw); err == nil && v > 0 {
		return v
	}
	slog.Warn("variable de entorno inválida, usando valor por defecto", "var", name, "valor", raw, "defecto", def)
	return def
}

// newEventRepository usa PostgreSQL (protegido por un circuit breaker) cuando POSTGRES_HOST
// está definido; si no, cae a un repositorio solo en memoria (útil para desarrollo y pruebas).
// El gate bloquea mientras el circuito esté abierto; es nil si no hay breaker.
func newEventRepository() (domain.BatchEventRepository, io.Closer, func(context.Context) error, func(context.Context) error, error) {
	host := os.Getenv("POSTGRES_HOST")
	if host == "" {
		slog.Warn("POSTGRES_HOST no definido: usando repositorio en memoria (sin persistencia)")
		r, err := repository.NewMemoryRepository(10000, "")
		return r, r, nil, nil, err
	}

	cfg := repository.PostgresConfig{
		Host:     host,
		Port:     envString("POSTGRES_PORT", "5432"),
		User:     envString("POSTGRES_USER", "postgres"),
		Password: os.Getenv("POSTGRES_PASSWORD"),
		DBName:   envString("POSTGRES_DB", "analytics"),
		SSLMode:  envString("POSTGRES_SSLMODE", "disable"),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	r, err := repository.NewPostgresRepository(ctx, cfg.DSN())
	if err != nil {
		return nil, nil, nil, nil, err
	}
	slog.Info("PostgreSQL conectado", "host", cfg.Host, "port", cfg.Port, "db", cfg.DBName)

	// El circuito debe abrirse antes de que un mismo lote agote MaxDeliver; si no, los mensajes
	// llegarían a la DLQ por una caída de la base en vez de esperar en el stream.
	failures := envInt("DB_BREAKER_FAILURES", 2)
	if maxDeliver := envInt("NATS_MAX_DELIVER", 3); failures > maxDeliver-1 {
		failures = maxDeliver - 1
		slog.Warn("DB_BREAKER_FAILURES ajustado a NATS_MAX_DELIVER-1", "valor", failures)
	}
	if failures < 1 {
		failures = 1
	}
	guarded := repository.NewBreakerRepository(r, resilience.Config{
		Name:                "postgres",
		ConsecutiveFailures: uint32(failures),
		OpenTimeout:         envDuration("DB_BREAKER_OPEN_TIMEOUT", 15*time.Second),
		SlowThreshold:       envDuration("DB_BREAKER_SLOW_THRESHOLD", 5*time.Second),
	})
	return guarded, r, guarded.WaitReady, r.Ping, nil
}

// waitDone espera a done o a que venza ctx; retorna true si terminó a tiempo.
func waitDone(ctx context.Context, done <-chan struct{}, what string) bool {
	select {
	case <-done:
		return true
	case <-ctx.Done():
		slog.Warn("apagado: tiempo agotado esperando", "componente", what)
		return false
	}
}

func main() {
	setupLogger()
	if err := run(); err != nil {
		slog.Error("el servicio terminó con error", "error", err)
		os.Exit(1)
	}
}

// run compone las dependencias, sirve HTTP y ejecuta el apagado ordenado. Devuelve error en
// lugar de abortar para que los defer de limpieza se ejecuten.
func run() error {
	slog.Info("iniciando microservicio de analíticas")
	if strings.EqualFold(os.Getenv("ENVIRONMENT"), "production") {
		if os.Getenv("API_KEY") == "" && os.Getenv("TENANT_API_KEYS") == "" {
			return errors.New("producción requiere API_KEY o TENANT_API_KEYS")
		}
		if os.Getenv("POSTGRES_HOST") == "" {
			return errors.New("producción requiere POSTGRES_HOST")
		}
		if os.Getenv("NATS_URL") == "" {
			return errors.New("producción requiere NATS_URL")
		}
		if os.Getenv("REDIS_URL") == "" {
			return errors.New("producción requiere REDIS_URL")
		}
		if os.Getenv("CORS_ALLOWED_ORIGINS") == "" {
			return errors.New("producción requiere CORS_ALLOWED_ORIGINS")
		}
		if os.Getenv("CORS_ALLOWED_ORIGINS") == "*" {
			return errors.New("producción no permite CORS_ALLOWED_ORIGINS=*")
		}
		if os.Getenv("WS_ALLOWED_ORIGINS") == "" {
			return errors.New("producción requiere WS_ALLOWED_ORIGINS")
		}
		if os.Getenv("WS_ALLOWED_ORIGINS") == "*" {
			return errors.New("producción no permite WS_ALLOWED_ORIGINS=*")
		}
	}

	eventRepo, closer, dbGate, dbPing, err := newEventRepository()
	if err != nil {
		return fmt.Errorf("inicializar repositorio: %w", err)
	}

	// Memoria temporal de ids ya persistidos (primera barrera; el índice único de
	// event_id en PostgreSQL es la garantía definitiva).
	dedupCache := dedup.New(envDuration("DEDUP_TTL", 10*time.Minute), envInt("DEDUP_MAX_ENTRIES", 100000))

	analyticsService := usecase.NewAnalyticsService(1000, eventRepo).WithDedup(dedupCache)
	analyticsHandler := handler.NewAnalyticsHandler(analyticsService)

	// Handler WebSocket y difusor en segundo plano
	wsHandler := handler.NewWSHandlerWithOrigins(os.Getenv("WS_ALLOWED_ORIGINS"))
	go wsHandler.HandleMessages()

	broadcastSummary := func() {
		data, err := json.Marshal(analyticsService.GetSummary())
		if err != nil {
			slog.Error("serializar resumen", "error", err)
			return
		}
		wsHandler.Broadcast(data)
	}

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var redisStore *distributed.RedisStore
	if redisURL := os.Getenv("REDIS_URL"); redisURL != "" {
		ctx, cancel := context.WithTimeout(rootCtx, 10*time.Second)
		redisStore, err = distributed.NewRedisStore(ctx, redisURL, envString("REDIS_PREFIX", "analytics"))
		cancel()
		if err != nil {
			return fmt.Errorf("conectar Redis: %w", err)
		}
		analyticsService.WithSummaryStore(redisStore)
		slog.Info("estado distribuido conectado", "backend", "Redis")
	}

	// El consumer tiene su propio contexto: se cancela después de cerrar HTTP, para que
	// no quede nada publicándose mientras se detiene la lectura de JetStream.
	consumerCtx, stopConsumer := context.WithCancel(context.Background())
	defer stopConsumer()

	var (
		qc           *queue.Client
		consumerDone chan struct{}
		ingest       http.HandlerFunc
	)
	var natsReady atomic.Bool

	if natsURL := os.Getenv("NATS_URL"); natsURL != "" {
		// Modo cola: el POST publica en JetStream (202 = mensaje durable) y un consumer
		// inserta por lotes en la base de datos y actualiza el resumen.
		connCtx, cancel := context.WithTimeout(rootCtx, 45*time.Second)
		qc, err = queue.Connect(connCtx, queue.Config{
			URL:           natsURL,
			Stream:        envString("NATS_STREAM", "EVENTS"),
			Subject:       envString("NATS_SUBJECT", "events.ingest"),
			Durable:       envString("NATS_DURABLE", "analytics-batch-writer"),
			DLQSubject:    envString("NATS_DLQ_SUBJECT", "analytics.dlq"),
			MaxDeliver:    envInt("NATS_MAX_DELIVER", 3),
			BatchSize:     envInt("BATCH_SIZE", 500),
			FlushInterval: envDuration("BATCH_FLUSH_INTERVAL", time.Second),
		})
		cancel()
		if err != nil {
			return fmt.Errorf("conectar a NATS: %w", err)
		}
		natsReady.Store(true)
		slog.Info("NATS JetStream conectado", "url", natsURL)

		analyticsService.WithPublisher(qc)
		qc.Gate = dbGate
		qc.Dedup = dedupCache
		qc.OnDeadLetter = func(string) {
			analyticsService.RecordDLQ()
			broadcastSummary()
		}
		consumerDone = make(chan struct{})
		go func() {
			defer close(consumerDone)
			qc.RunConsumer(consumerCtx, eventRepo, func(batch []*domain.Event) {
				analyticsService.RecordProcessed(batch)
				broadcastSummary()
			})
		}()
		ingest = analyticsHandler.HandleIngest
	} else {
		// Modo directo: canal en memoria + worker pool que persiste cada evento.
		slog.Info("NATS_URL no definido: ingesta directa (canal en memoria + worker pool)")
		analyticsService.StartWorkerPool(4)

		// Tras aceptar un evento, emite el resumen actual a los clientes WebSocket
		ingest = func(w http.ResponseWriter, r *http.Request) {
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			analyticsHandler.HandleIngest(rec, r)
			if rec.status == http.StatusAccepted {
				broadcastSummary()
			}
		}
	}

	// Exportador a Parquet / data lake (opcional: solo con S3_ENDPOINT y PostgreSQL)
	exportCtx, stopExport := context.WithCancel(context.Background())
	defer stopExport()
	exportDone := startExporter(exportCtx, closer)

	// Seguridad: API_KEY vacío desactiva la verificación (modo opcional)
	if os.Getenv("API_KEY") == "" {
		slog.Warn("API_KEY no definida: rutas de lectura sin autenticación")
	}

	// Multi-tenancy: API_KEY (administración) y TENANT_API_KEYS ("clave:tenant,...") autentican la ingesta
	tenantKeys, err := middleware.ParseTenantKeys(os.Getenv("TENANT_API_KEYS"))
	if err != nil {
		return fmt.Errorf("TENANT_API_KEYS inválida: %w", err)
	}
	tenantAuth, err := middleware.NewTenantAuth(os.Getenv("API_KEY"), tenantKeys)
	if err != nil {
		return fmt.Errorf("configuración de API keys: %w", err)
	}
	if !tenantAuth.Enabled() {
		slog.Warn("sin API keys: ingesta sin autenticación; X-Tenant-ID es informativo (no verificable)")
	}
	// WebSocket: acepta WS_TOKEN (solo lectura, apto para el navegador) o la clave de administración
	wsAuth := middleware.WSToken(os.Getenv("WS_TOKEN"), os.Getenv("API_KEY"))
	if os.Getenv("WS_TOKEN") == "" && os.Getenv("API_KEY") == "" {
		slog.Warn("WS_TOKEN/API_KEY no definidos: /ws sin autenticación")
	}
	stopLimiter := make(chan struct{})
	defer close(stopLimiter)
	rps := envFloat("RATE_LIMIT_RPS", 20)
	burst := envInt("RATE_LIMIT_BURST", 40)
	var limiter middleware.RequestLimiter
	var redisLimiter *middleware.RedisRateLimiter
	if redisURL := os.Getenv("REDIS_URL"); redisURL != "" {
		ctx, cancel := context.WithTimeout(rootCtx, 10*time.Second)
		redisLimiter, err = middleware.NewRedisRateLimiter(ctx, redisURL, rps, burst, envString("REDIS_PREFIX", "analytics"))
		cancel()
		if err != nil {
			return fmt.Errorf("conectar Redis para rate limiting: %w", err)
		}
		limiter = redisLimiter
	} else {
		limiter = middleware.NewRateLimiter(rps, burst, stopLimiter)
	}

	// Rutas HTTP. Instrument va por fuera para contar también 401/429.
	var shuttingDown atomic.Bool
	http.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("Analytics Go Service is running successfully")); err != nil {
			slog.Debug("escribir respuesta /health", "error", err)
		}
	})
	// Readiness: pasa a 503 en cuanto empieza el apagado para que el balanceador deje de enviar tráfico
	http.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) {
		if shuttingDown.Load() {
			http.Error(w, "apagando", http.StatusServiceUnavailable)
			return
		}
		if dbPing != nil {
			pingCtx, pingCancel := context.WithTimeout(context.Background(), 750*time.Millisecond)
			defer pingCancel()
			if err := dbPing(pingCtx); err != nil {
				http.Error(w, "base de datos no disponible", http.StatusServiceUnavailable)
				return
			}
		}
		if os.Getenv("NATS_URL") != "" && !natsReady.Load() {
			http.Error(w, "NATS no disponible", http.StatusServiceUnavailable)
			return
		}
		if os.Getenv("NATS_URL") != "" && qc != nil {
			pingCtx, pingCancel := context.WithTimeout(context.Background(), 750*time.Millisecond)
			defer pingCancel()
			if err := qc.Ping(pingCtx); err != nil {
				http.Error(w, "NATS no disponible", http.StatusServiceUnavailable)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
	})

	// El limitador va por fuera de la autenticación: frena también la fuerza bruta de claves
	http.HandleFunc("/api/events", observability.Instrument("/api/events", limiter.Limit(tenantAuth.Ingest(ingest))))
	http.HandleFunc("/api/events/history", observability.Instrument("/api/events/history", tenantAuth.Read(analyticsHandler.HandleHistory)))
	http.HandleFunc("/api/metrics", observability.Instrument("/api/metrics", tenantAuth.Read(analyticsHandler.HandleSummary)))
	http.Handle("/metrics", middleware.APIKey(os.Getenv("API_KEY"))(promhttp.Handler()))
	// Operaciones de DLQ: solo administrador (falla cerrado sin API_KEY) y con limitador propio y estricto
	var dlqMgr domain.DLQManager
	if qc != nil {
		dlqMgr = qc.DLQ()
	}
	dlqH := handler.NewDLQHandler(dlqMgr)
	adminLimiter := middleware.NewRateLimiter(2, 5, stopLimiter)
	admin := middleware.AdminOnly(os.Getenv("API_KEY"))
	http.HandleFunc("/api/dlq", observability.Instrument("/api/dlq", adminLimiter.Limit(admin(http.HandlerFunc(dlqH.List)).ServeHTTP)))
	http.HandleFunc("/api/dlq/redrive", observability.Instrument("/api/dlq/redrive", adminLimiter.Limit(admin(http.HandlerFunc(dlqH.Redrive)).ServeHTTP)))
	http.HandleFunc("/ws", wsAuth(wsHandler.HandleConnections))

	const addr = ":8080"
	slog.Info("servidor escuchando", "addr", addr)

	// CORS envuelve todo el mux (DefaultServeMux) de forma global. Los timeouts de cabeceras y
	// de inactividad protegen frente a clientes lentos sin cortar las conexiones WebSocket.
	srv := &http.Server{
		Addr: addr,
		Handler: middleware.RequestContext(
			middleware.CORSAllowed(os.Getenv("CORS_ALLOWED_ORIGINS"), http.DefaultServeMux),
		),
		ReadTimeout:       15 * time.Second,
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("servidor HTTP", "error", err)
			stop()
		}
	}()

	<-rootCtx.Done()
	stop() // restaura el comportamiento por defecto: una segunda señal termina el proceso al instante
	shuttingDown.Store(true)

	timeout := envDuration("SHUTDOWN_TIMEOUT", 25*time.Second)
	slog.Info("señal recibida: apagado ordenado", "limite", timeout)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// 1. HTTP: deja de aceptar conexiones y espera a las peticiones en vuelo
	httpCtx, httpCancel := context.WithTimeout(ctx, timeout/2)
	if err := srv.Shutdown(httpCtx); err != nil {
		slog.Error("apagado: HTTP no cerró limpiamente", "error", err)
	}
	httpCancel()

	// 2. JetStream: deja de leer; el lote que se está armando se termina y se vuelca a PostgreSQL.
	// Lo que siga en el stream es durable y lo retomará el siguiente arranque.
	stopConsumer()
	if consumerDone != nil && waitDone(ctx, consumerDone, "el consumer de JetStream") {
		slog.Info("apagado: consumer detenido y lote final volcado")
	}

	// 3. Worker pool (modo directo): drena la cola en memoria hacia el repositorio
	poolDone := make(chan struct{})
	go func() {
		analyticsService.Stop()
		close(poolDone)
	}()
	if waitDone(ctx, poolDone, "el worker pool") {
		slog.Info("apagado: workers drenados")
	}

	// 4. Recursos: exportador, clientes WebSocket, NATS (Drain) y pool de PostgreSQL
	stopExport()
	if exportDone != nil && waitDone(ctx, exportDone, "el exportador a Parquet") {
		slog.Info("apagado: exportador detenido")
	}
	wsHandler.Close()
	if qc != nil {
		natsReady.Store(false)
		if err := qc.Close(); err != nil {
			slog.Error("apagado: cierre de NATS", "error", err)
		}
	}
	if redisLimiter != nil {
		if err := redisLimiter.Close(); err != nil {
			slog.Error("apagado: cierre de Redis rate limiter", "error", err)
		}
	}
	if redisStore != nil {
		if err := redisStore.Close(); err != nil {
			slog.Error("apagado: cierre de Redis store", "error", err)
		}
	}
	if err := closer.Close(); err != nil {
		slog.Error("apagado: cierre del repositorio", "error", err)
	}
	slog.Info("apagado completo")
	return nil
}
