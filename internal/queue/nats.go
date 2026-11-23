package queue

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/Arierutemplar/event-driven-microservices-ts/internal/dedup"
	"github.com/Arierutemplar/event-driven-microservices-ts/internal/domain"
	"github.com/Arierutemplar/event-driven-microservices-ts/internal/observability"
	"github.com/Arierutemplar/event-driven-microservices-ts/internal/transport"
)

var _ domain.EventPublisher = (*Client)(nil)

// Config parametriza el stream, el consumer y el batching.
type Config struct {
	URL                                                 string
	Stream                                              string
	Subject                                             string
	Durable                                             string
	BatchSize                                           int           // máximo de eventos por inserción
	FlushInterval                                       time.Duration // ventana máxima de espera para completar un lote
	MaxDeliver                                          int           // intentos antes de enviar el mensaje a la DLQ (defecto 3)
	DLQSubject                                          string        // subject de mensajes muertos (defecto analytics.dlq)
	AckWait                                             time.Duration
	RetryDelay                                          time.Duration // espera base antes de reentregar tras un Nak (crece con cada intento)
	TLSCAFile, TLSCertFile, TLSKeyFile, CredentialsFile string
	Replicas                                            int           // réplicas del stream (1 = sin HA; usar 3 en clúster)
	MaxAge                                              time.Duration // retención máxima de mensajes sin procesar
}

func (c *Config) applyDefaults() {
	if c.Stream == "" {
		c.Stream = "EVENTS"
	}
	if c.Subject == "" {
		c.Subject = "events.ingest"
	}
	if c.DLQSubject == "" {
		c.DLQSubject = "analytics.dlq"
	}
	if c.Durable == "" {
		c.Durable = "analytics-batch-writer"
	}
	if c.BatchSize < 1 {
		c.BatchSize = 500
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = time.Second
	}
	if c.MaxDeliver == 0 {
		c.MaxDeliver = 3
	}
	if c.AckWait <= 0 {
		c.AckWait = 30 * time.Second
	}
	if c.RetryDelay <= 0 {
		c.RetryDelay = 5 * time.Second
	}
}

// Client agrupa la conexión, el stream y el consumer durable.
type Client struct {
	nc     *nats.Conn
	js     jetstream.JetStream
	stream jetstream.Stream
	cons   jetstream.Consumer
	cfg    Config
	admin  *DLQAdmin

	// OnDeadLetter, si no es nil, se invoca tras publicar con éxito un mensaje en la DLQ.
	// Debe asignarse antes de RunConsumer.
	OnDeadLetter func(reason string)

	// Aggregate, si no es nil, se invoca tras persistir cada lote y antes del ack. Si devuelve
	// error el lote se devuelve a la cola; debe ser idempotente por id de evento.
	Aggregate func(ctx context.Context, events []*domain.Event) error

	// Gate, si no es nil, se consulta antes de cada lectura: bloquea mientras el destino
	// (p. ej. un circuit breaker abierto) no admita escrituras, pausando el consumo sin
	// gastar entregas de los mensajes. Retorna error solo si ctx se cancela.
	Gate func(ctx context.Context) error

	// Dedup, si no es nil, descarta eventos con un id ya persistido recientemente.
	Dedup *dedup.Cache
}

// Connect conecta con reintentos hasta que NATS esté disponible o venza ctx,
// y crea (o actualiza) el stream y el consumer durable.
//
// El stream usa retención WorkQueue: cada mensaje se elimina al confirmarse (ack),
// de modo que el almacenamiento solo contiene el backlog pendiente.
func Connect(ctx context.Context, cfg Config) (*Client, error) {
	cfg.applyDefaults()

	var (
		nc  *nats.Conn
		err error
	)
	opts := []nats.Option{
		nats.Name("event-driven-microservices-ts"),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			slog.Warn("nats: desconectado", "error", err)
		}),
		nats.ReconnectHandler(func(c *nats.Conn) {
			slog.Info("nats: reconectado")
		}),
	}
	if cfg.TLSCAFile != "" || cfg.TLSCertFile != "" || cfg.TLSKeyFile != "" {
		tlsConfig, err := transport.ClientTLS(cfg.TLSCAFile, cfg.TLSCertFile, cfg.TLSKeyFile)
		if err != nil {
			return nil, fmt.Errorf("queue: TLS: %w", err)
		}
		opts = append(opts, nats.Secure(tlsConfig))
	}
	if cfg.CredentialsFile != "" {
		opts = append(opts, nats.UserCredentials(cfg.CredentialsFile))
	}
	for {
		nc, err = nats.Connect(cfg.URL, opts...)
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("queue: esperando NATS: %w", err)
		case <-time.After(time.Second):
		}
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("queue: jetstream: %w", err)
	}

	replicas, maxAge := cfg.Replicas, cfg.MaxAge
	if replicas < 1 {
		replicas = 1
	}
	if maxAge <= 0 {
		maxAge = 72 * time.Hour
	}
	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      cfg.Stream,
		Subjects:  []string{cfg.Subject},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.WorkQueuePolicy,
		MaxAge:    maxAge,
		Replicas:  replicas,
		// Ventana de deduplicación por Nats-Msg-Id (reintentos de publicación)
		Duplicates: 2 * time.Minute,
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("queue: crear stream %s: %w", cfg.Stream, err)
	}

	// Stream de mensajes muertos: retención por límites (no se borra al leer) para
	// poder inspeccionarlos y reprocesarlos.
	dlqStream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       cfg.Stream + "_DLQ",
		Subjects:   []string{cfg.DLQSubject},
		Storage:    jetstream.FileStorage,
		Retention:  jetstream.LimitsPolicy,
		MaxAge:     30 * 24 * time.Hour,
		Duplicates: 2 * time.Minute,
		Replicas:   replicas,
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("queue: crear stream DLQ: %w", err)
	}

	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       cfg.Durable,
		FilterSubject: cfg.Subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       cfg.AckWait,
		MaxDeliver:    cfg.MaxDeliver,
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("queue: crear consumer %s: %w", cfg.Durable, err)
	}

	c := &Client{nc: nc, js: js, stream: stream, cons: cons, cfg: cfg}
	c.admin = &DLQAdmin{js: js, dlq: dlqStream, cfg: cfg}
	if err := c.watchMaxDeliveries(); err != nil {
		nc.Close()
		return nil, fmt.Errorf("queue: suscribir advisory MAX_DELIVERIES: %w", err)
	}
	return c, nil
}

// Publish serializa el evento y lo publica en el stream. Retorna nil solo tras
// el ack de JetStream, así que un 202 al cliente implica mensaje durable.
func (c *Client) Publish(ctx context.Context, e *domain.Event) error {
	ctx, span := otel.Tracer("analytics/queue").Start(ctx, "nats publish", trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()
	data, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("queue: serializar evento: %w", err)
	}

	msg := &nats.Msg{Subject: c.cfg.Subject, Data: data, Header: nats.Header{}}
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(http.Header(msg.Header)))
	// Con id de evento, JetStream descarta reenvíos del mismo id dentro de la ventana
	// de deduplicación (Duplicates). El id va acotado al tenant: así un tenant no puede
	// suprimir eventos de otro reutilizando sus ids. Sin id se usa uno aleatorio.
	msgID := e.ScopedID()
	if msgID == "" {
		msgID = newMsgID()
	}
	msg.Header.Set(nats.MsgIdHdr, msgID)

	ack, err := c.js.PublishMsg(ctx, msg)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "publish failed")
		observability.NATSPublishError()
		return fmt.Errorf("queue: publicar: %w", err)
	}
	if ack.Duplicate {
		observability.DedupDropped("publish", 1)
		return nil
	}
	observability.NATSPublished()
	return nil
}

// Close drena la conexión (entrega lo pendiente) y la cierra.
func (c *Client) Close() error {
	return c.nc.Drain()
}

// Ping comprueba que la conexión NATS esté abierta y que pueda enviar un flush.
func (c *Client) Ping(ctx context.Context) error {
	if c == nil || c.nc == nil {
		return fmt.Errorf("queue: conexión no inicializada")
	}
	if c.nc.Status() != nats.CONNECTED && c.nc.Status() != nats.RECONNECTING {
		return fmt.Errorf("queue: estado NATS %s", c.nc.Status())
	}
	if err := c.nc.FlushWithContext(ctx); err != nil {
		return fmt.Errorf("queue: flush NATS: %w", err)
	}
	return nil
}

func newMsgID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("msg-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}
