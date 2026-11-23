package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Arierutemplar/event-driven-microservices-ts/internal/observability"
)

// Motivos por los que un mensaje termina en la DLQ (valores del label `reason`).
const (
	ReasonDecode        = "decode_error"
	ReasonPermanent     = "permanent_error"
	ReasonMaxDeliveries = "max_deliveries_exceeded"
)

// Cabeceras de metadatos añadidas a cada mensaje de la DLQ. El cuerpo y el resto de
// cabeceras (incluido Nats-Msg-Id) son los del mensaje original, listo para reprocesarse.
const (
	HdrDLQReason          = "X-DLQ-Reason"
	HdrDLQError           = "X-DLQ-Error"
	HdrDLQTimestamp       = "X-DLQ-Timestamp"
	HdrDLQOriginalSubject = "X-DLQ-Original-Subject"
	HdrDLQDeliveries      = "X-DLQ-Deliveries"
	HdrDLQStreamSeq       = "X-DLQ-Stream-Seq"
)

const (
	dlqPublishTimeout = 5 * time.Second
	maxErrHeaderLen   = 512
)

var headerSanitizer = strings.NewReplacer("\r", " ", "\n", " ")

// publishDLQ publica el mensaje en el subject de mensajes muertos y espera el ack del stream.
func (c *Client) publishDLQ(data []byte, orig nats.Header, subject string, seq, deliveries uint64, reason string, cause error) error {
	hdr := nats.Header{}
	for k, v := range orig {
		hdr[k] = append([]string(nil), v...)
	}
	causeText := "desconocido"
	if cause != nil {
		causeText = headerSanitizer.Replace(cause.Error())
		if len(causeText) > maxErrHeaderLen {
			causeText = causeText[:maxErrHeaderLen]
		}
	}
	hdr.Set(HdrDLQReason, reason)
	hdr.Set(HdrDLQError, causeText)
	hdr.Set(HdrDLQTimestamp, time.Now().UTC().Format(time.RFC3339Nano))
	hdr.Set(HdrDLQOriginalSubject, subject)
	hdr.Set(HdrDLQDeliveries, strconv.FormatUint(deliveries, 10))
	hdr.Set(HdrDLQStreamSeq, strconv.FormatUint(seq, 10))

	ctx, cancel := context.WithTimeout(context.Background(), dlqPublishTimeout)
	defer cancel()
	if _, err := c.js.PublishMsg(ctx, &nats.Msg{Subject: c.cfg.DLQSubject, Data: data, Header: hdr}); err != nil {
		observability.DLQPublishError()
		return fmt.Errorf("queue: publicar en DLQ: %w", err)
	}
	observability.DLQMessage(reason)
	if c.OnDeadLetter != nil {
		c.OnDeadLetter(reason)
	}
	return nil
}

// deadLetter envía el mensaje a la DLQ y, solo si se confirmó, hace Ack del original
// para que salga del stream principal. Si la DLQ falla se reintenta con Nak.
func (c *Client) deadLetter(msg jetstream.Msg, reason string, cause error) bool {
	var seq, deliveries uint64
	if meta, err := msg.Metadata(); err == nil {
		seq, deliveries = meta.Sequence.Stream, meta.NumDelivered
	}
	if err := c.publishDLQ(msg.Data(), msg.Headers(), msg.Subject(), seq, deliveries, reason, cause); err != nil {
		slog.Error("queue: DLQ no disponible; el mensaje se reintentará", "error", err)
		nak(msg, c.cfg.RetryDelay)
		return false
	}
	if err := msg.Ack(); err != nil {
		slog.Warn("queue: ack tras DLQ falló (el evento ya está en la DLQ)", "error", err)
	}
	slog.Warn("queue: mensaje enviado a la DLQ", "motivo", reason, "seq", seq, "entregas", deliveries, "causa", cause)
	return true
}

// exhausted indica si esta entrega fue la última permitida por MaxDeliver.
func (c *Client) exhausted(msg jetstream.Msg) bool {
	if c.cfg.MaxDeliver < 1 {
		return false
	}
	meta, err := msg.Metadata()
	return err == nil && meta.NumDelivered >= uint64(c.cfg.MaxDeliver)
}

// retryDelay crece linealmente con el número de entregas para dar margen a la base de datos.
func (c *Client) retryDelay(msg jetstream.Msg) time.Duration {
	n := uint64(1)
	if meta, err := msg.Metadata(); err == nil && meta.NumDelivered > 0 {
		n = meta.NumDelivered
	}
	return c.cfg.RetryDelay * time.Duration(n)
}

// watchMaxDeliveries escucha el advisory que JetStream emite cuando un mensaje agota
// MaxDeliver sin ack (p. ej. el worker murió a mitad de lote). JetStream no vuelve a
// entregarlo y en un stream WorkQueue se quedaría para siempre, así que lo movemos a la
// DLQ y lo borramos del stream principal.
func (c *Client) watchMaxDeliveries() error {
	subj := fmt.Sprintf("$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.%s.%s", c.cfg.Stream, c.cfg.Durable)
	_, err := c.nc.Subscribe(subj, func(m *nats.Msg) { c.handleMaxDeliveries(m.Data) })
	return err
}

func (c *Client) handleMaxDeliveries(payload []byte) {
	var adv struct {
		StreamSeq  uint64 `json:"stream_seq"`
		Deliveries uint64 `json:"deliveries"`
	}
	if err := json.Unmarshal(payload, &adv); err != nil || adv.StreamSeq == 0 {
		slog.Warn("queue: advisory MAX_DELIVERIES ilegible", "error", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	raw, err := c.stream.GetMsg(ctx, adv.StreamSeq)
	if err != nil {
		if !errors.Is(err, jetstream.ErrMsgNotFound) { // ya gestionado por el worker
			slog.Error("queue: leer mensaje para DLQ", "seq", adv.StreamSeq, "error", err)
		}
		return
	}
	cause := errors.New("entregas agotadas sin ack")
	if err := c.publishDLQ(raw.Data, raw.Header, raw.Subject, adv.StreamSeq, adv.Deliveries, ReasonMaxDeliveries, cause); err != nil {
		slog.Error("queue: publicar advisory en DLQ", "seq", adv.StreamSeq, "error", err)
		return
	}
	if err := c.stream.DeleteMsg(ctx, adv.StreamSeq); err != nil {
		slog.Error("queue: borrar mensaje tras DLQ", "seq", adv.StreamSeq, "error", err)
	}
}
