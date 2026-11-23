package queue

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Arierutemplar/REPO/analytics-service/internal/dedup"
	"github.com/Arierutemplar/REPO/analytics-service/internal/domain"
	"github.com/Arierutemplar/REPO/analytics-service/internal/observability"
)

var _ domain.EventPublisher = (*Client)(nil)

// Config parametriza el stream, el consumer y el batching.
type Config struct {
	URL           string
	Stream        string
	Subject       string
	Durable       string
	BatchSize     int           // máximo de eventos por inserción
	FlushInterval time.Duration // ventana máxima de espera para completar un lote
	MaxDeliver    int           // intentos antes de enviar el mensaje a la DLQ (defecto 3)
	DLQSubject    string        // subject de mensajes muertos (defecto analytics.dlq)
	AckWait       time.Duration
	RetryDelay    time.Duration // espera base antes de reentregar tras un Nak (crece con cada intento)
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
	for {
		nc, err = nats.Connect(cfg.URL,
			nats.Name("analytics-service"),
			nats.MaxReconnects(-1),
			nats.ReconnectWait(time.Second),
			nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
				slog.Warn("nats: desconectado", "error", err)
			}),
			nats.ReconnectHandler(func(c *nats.Conn) {
				slog.Info("nats: reconectado", "url", c.ConnectedUrl())
			}),
		)
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("queue: esperando NATS (%s): %w", cfg.URL, err)
		case <-time.After(time.Second):
		}
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("queue: jetstream: %w", err)
	}

	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      cfg.Stream,
		Subjects:  []string{cfg.Subject},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.WorkQueuePolicy,
		MaxAge:    72 * time.Hour,
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
	data, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("queue: serializar evento: %w", err)
	}

	msg := &nats.Msg{Subject: c.cfg.Subject, Data: data, Header: nats.Header{}}
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
