package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Arierutemplar/event-driven-microservices-ts/internal/domain"
	"github.com/Arierutemplar/event-driven-microservices-ts/internal/observability"
)

var _ domain.DLQManager = (*DLQAdmin)(nil)

// Cabeceras que añade el re-drive al mensaje reinyectado. Si vuelve a fallar, publishDLQ las
// conserva, de modo que X-Redrive-Count refleja cuántas veces se ha reintentado a mano.
const (
	HdrRedriveCount   = "X-Redrive-Count"
	HdrRedriveAt      = "X-Redrive-Timestamp"
	HdrRedriveFromSeq = "X-Redrive-From-DLQ-Seq"
)

const (
	// DefaultRedriveLimit y MaxRedriveLimit acotan cuántos mensajes se reinyectan por operación,
	// para no inundar el pipeline (ni la base de datos) de golpe tras una incidencia.
	DefaultRedriveLimit = 100
	MaxRedriveLimit     = 5000
	DefaultListLimit    = 50
	MaxListLimit        = 1000

	scanBatch      = 256
	previewLen     = 160
	redrivePubWait = 5 * time.Second
	redriveIDMark  = "#dlq"
)

// DLQAdmin inspecciona y reprocesa la Dead Letter Queue. Lo comparten el endpoint HTTP
// administrativo y la CLI de operaciones; no toca el consumer ni los streams (solo lee la DLQ,
// publica en el subject principal y borra de la DLQ lo ya reinyectado).
type DLQAdmin struct {
	nc  *nats.Conn // solo si la conexión es propia (CLI); nil cuando la gestiona un Client
	js  jetstream.JetStream
	dlq jetstream.Stream
	cfg Config

	mu sync.Mutex // serializa re-drives dentro del proceso
}

// ConnectAdmin abre una conexión ligera para operar sobre una DLQ existente. A diferencia de
// Connect no crea ni modifica streams ni consumers, ni se suscribe a advisories, así que es
// seguro ejecutarlo contra producción mientras el servicio está en marcha.
func ConnectAdmin(ctx context.Context, cfg Config) (*DLQAdmin, error) {
	cfg.applyDefaults()
	nc, err := nats.Connect(cfg.URL, nats.Name("analytics-dlqctl"), nats.Timeout(5*time.Second))
	if err != nil {
		return nil, fmt.Errorf("queue: conectar a NATS (%s): %w", cfg.URL, err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("queue: jetstream: %w", err)
	}
	if _, err := js.Stream(ctx, cfg.Stream); err != nil {
		nc.Close()
		return nil, fmt.Errorf("queue: stream principal %q no encontrado: %w", cfg.Stream, err)
	}
	dlq, err := js.Stream(ctx, cfg.Stream+"_DLQ")
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("queue: stream DLQ %q no encontrado: %w", cfg.Stream+"_DLQ", err)
	}
	return &DLQAdmin{nc: nc, js: js, dlq: dlq, cfg: cfg}, nil
}

// Close cierra la conexión si es propia.
func (a *DLQAdmin) Close() error {
	if a.nc == nil {
		return nil
	}
	return a.nc.Drain()
}

// DLQ devuelve el gestor de la DLQ que comparte conexión con el Client.
func (c *Client) DLQ() *DLQAdmin { return c.admin }

// dlqMsg es un mensaje leído de la DLQ, sea por consumer o por GetMsg.
type dlqMsg struct {
	seq  uint64
	data []byte
	hdr  nats.Header
	ts   time.Time
}

func (a *DLQAdmin) Stats(ctx context.Context) (domain.DLQStats, error) {
	info, err := a.dlq.Info(ctx)
	if err != nil {
		return domain.DLQStats{}, fmt.Errorf("queue: info DLQ: %w", err)
	}
	s := domain.DLQStats{
		Stream: info.Config.Name, Subject: a.cfg.DLQSubject,
		Messages: info.State.Msgs, Bytes: info.State.Bytes,
		FirstSeq: info.State.FirstSeq, LastSeq: info.State.LastSeq,
	}
	if info.State.Msgs > 0 {
		s.Oldest = info.State.FirstTime
	}
	return s, nil
}

func (a *DLQAdmin) List(ctx context.Context, f domain.DLQFilter, limit int) ([]domain.DeadLetter, error) {
	if limit < 1 {
		limit = DefaultListLimit
	}
	if limit > MaxListLimit {
		limit = MaxListLimit
	}
	out := []domain.DeadLetter{}
	err := a.each(ctx, f, func(m dlqMsg) bool {
		out = append(out, toDeadLetter(m, false))
		return len(out) >= limit
	})
	return out, err
}

func (a *DLQAdmin) Get(ctx context.Context, seq uint64) (*domain.DeadLetter, error) {
	m, err := a.getMsg(ctx, seq)
	if err != nil {
		return nil, err
	}
	d := toDeadLetter(m, true)
	return &d, nil
}

func (a *DLQAdmin) getMsg(ctx context.Context, seq uint64) (dlqMsg, error) {
	raw, err := a.dlq.GetMsg(ctx, seq)
	if err != nil {
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			return dlqMsg{}, fmt.Errorf("DLQ seq=%d: %w", seq, domain.ErrNotFound)
		}
		return dlqMsg{}, fmt.Errorf("queue: leer DLQ seq=%d: %w", seq, err)
	}
	return dlqMsg{seq: raw.Sequence, data: raw.Data, hdr: raw.Header, ts: raw.Time}, nil
}

// each recorre en orden de secuencia los mensajes que cumplen el filtro hasta que fn devuelva true.
// Solo ve lo que había al empezar: lo que se publique en la DLQ durante el recorrido (p. ej. un
// mensaje reinyectado que vuelve a fallar) no se procesa en la misma pasada.
func (a *DLQAdmin) each(ctx context.Context, f domain.DLQFilter, fn func(dlqMsg) bool) error {
	if len(f.Seqs) > 0 {
		seqs := append([]uint64(nil), f.Seqs...)
		sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
		var prev uint64
		for _, s := range seqs {
			if s == prev {
				continue
			}
			prev = s
			m, err := a.getMsg(ctx, s)
			if errors.Is(err, domain.ErrNotFound) {
				continue // ya re-driveado o expirado
			}
			if err != nil {
				return err
			}
			if f.Reason != "" && m.hdr.Get(HdrDLQReason) != f.Reason {
				continue
			}
			if fn(m) {
				return nil
			}
		}
		return nil
	}

	info, err := a.dlq.Info(ctx)
	if err != nil {
		return fmt.Errorf("queue: info DLQ: %w", err)
	}
	if info.State.Msgs == 0 {
		return nil
	}
	upTo := info.State.LastSeq

	// Consumer efímero sin acks: no altera el estado de la DLQ y se autodestruye si se abandona
	ccfg := jetstream.ConsumerConfig{AckPolicy: jetstream.AckNonePolicy, InactiveThreshold: time.Minute}
	start := info.State.FirstSeq
	if f.AfterSeq+1 > start {
		start = f.AfterSeq + 1
	}
	if start > 1 {
		ccfg.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
		ccfg.OptStartSeq = start
	}
	cons, err := a.dlq.CreateConsumer(ctx, ccfg)
	if err != nil {
		return fmt.Errorf("queue: crear consumer de lectura: %w", err)
	}
	defer func() {
		dctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := a.dlq.DeleteConsumer(dctx, cons.CachedInfo().Name); err != nil {
			slog.Debug("dlq: borrar consumer de lectura", "error", err)
		}
	}()

	for ctx.Err() == nil {
		batch, err := cons.Fetch(scanBatch, jetstream.FetchMaxWait(2*time.Second))
		if err != nil {
			return fmt.Errorf("queue: leer DLQ: %w", err)
		}
		got := 0
		for msg := range batch.Messages() {
			meta, err := msg.Metadata()
			if err != nil {
				continue
			}
			if meta.Sequence.Stream > upTo {
				return nil
			}
			got++
			if f.Reason != "" && msg.Headers().Get(HdrDLQReason) != f.Reason {
				continue
			}
			if fn(dlqMsg{seq: meta.Sequence.Stream, data: msg.Data(), hdr: msg.Headers(), ts: meta.Timestamp}) {
				return nil
			}
		}
		if err := batch.Error(); err != nil && !errors.Is(err, nats.ErrTimeout) {
			return fmt.Errorf("queue: leer DLQ: %w", err)
		}
		if got == 0 {
			return nil
		}
		if ci, err := cons.Info(ctx); err == nil && ci.NumPending == 0 {
			return nil
		}
	}
	return ctx.Err()
}

// Redrive reinyecta en el subject principal los mensajes que cumplan el filtro (hasta limit) y los
// elimina de la DLQ. El orden es publicar -> confirmar -> borrar, así que un fallo a mitad nunca
// pierde mensajes; a lo sumo quedan en la DLQ y se reinyectan otra vez, lo que es inocuo porque:
//   - el Nats-Msg-Id de la reinyección es determinista por mensaje de la DLQ (el broker descarta
//     la repetición dentro de su ventana), y
//   - fuera de la ventana, el índice único (tenant_id, event_id) de PostgreSQL y la caché de
//     deduplicación del consumer descartan el evento repetido.
//
// El id NO reutiliza el original tal cual: el broker lo tiene visto desde la publicación inicial
// y descartaría en silencio una reinyección hecha dentro de su ventana de deduplicación.
func (a *DLQAdmin) Redrive(ctx context.Context, f domain.DLQFilter, limit int, dryRun bool) (domain.RedriveResult, error) {
	if limit < 1 {
		limit = DefaultRedriveLimit
	}
	if limit > MaxRedriveLimit {
		limit = MaxRedriveLimit
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	res := domain.RedriveResult{DryRun: dryRun}

	// Se recoge primero y se publica después: no se borra de la DLQ mientras se lee de ella
	var batch []dlqMsg
	if err := a.each(ctx, f, func(m dlqMsg) bool {
		if len(batch) >= limit {
			res.Truncated = true
			return true
		}
		batch = append(batch, m)
		return false
	}); err != nil {
		return res, err
	}
	res.Matched = len(batch)

	for _, m := range batch {
		if err := ctx.Err(); err != nil {
			return res, err
		}
		res.Seqs = append(res.Seqs, m.seq)
		if dryRun {
			continue
		}
		a.redriveOne(ctx, m, &res)
	}
	if !dryRun {
		slog.Info("dlq: re-drive finalizado", "reinyectados", res.Republished, "duplicados", res.Duplicates,
			"fallidos", res.Failed, "sin_borrar", res.DeleteFailed, "coincidentes", res.Matched)
	}
	return res, nil
}

func (a *DLQAdmin) redriveOne(ctx context.Context, m dlqMsg, res *domain.RedriveResult) {
	hdr := nats.Header{}
	for k, v := range m.hdr {
		if strings.HasPrefix(strings.ToLower(k), "x-dlq-") {
			continue // los metadatos del fallo anterior no viajan; el consumer añadirá los nuevos si vuelve a fallar
		}
		hdr[k] = append([]string(nil), v...)
	}
	count := atoi(m.hdr.Get(HdrRedriveCount))
	hdr.Set(HdrRedriveCount, strconv.Itoa(count+1))
	hdr.Set(HdrRedriveAt, time.Now().UTC().Format(time.RFC3339Nano))
	hdr.Set(HdrRedriveFromSeq, strconv.FormatUint(m.seq, 10))
	hdr.Set(nats.MsgIdHdr, redriveMsgID(m.hdr.Get(nats.MsgIdHdr), m.seq))

	pctx, cancel := context.WithTimeout(ctx, redrivePubWait)
	ack, err := a.js.PublishMsg(pctx, &nats.Msg{Subject: a.cfg.Subject, Data: m.data, Header: hdr})
	cancel()
	if err != nil {
		res.Failed++
		if res.FirstError == "" {
			res.FirstError = fmt.Sprintf("seq=%d: %v", m.seq, err)
		}
		observability.DLQRedrive("publish_error", 1)
		return
	}
	if ack.Duplicate {
		// Esta misma reinyección ya se hizo (p. ej. un re-drive anterior que no llegó a borrar)
		res.Duplicates++
		observability.DLQRedrive("duplicate", 1)
	} else {
		res.Republished++
		observability.DLQRedrive("republished", 1)
	}

	dctx, dcancel := context.WithTimeout(ctx, redrivePubWait)
	defer dcancel()
	if err := a.dlq.DeleteMsg(dctx, m.seq); err != nil && !errors.Is(err, jetstream.ErrMsgNotFound) {
		res.DeleteFailed++
		if res.FirstError == "" {
			res.FirstError = fmt.Sprintf("seq=%d reinyectado pero no borrado de la DLQ: %v", m.seq, err)
		}
		observability.DLQRedrive("delete_error", 1)
	}
}

// redriveMsgID deriva un Nats-Msg-Id estable por mensaje de la DLQ. Quita el sufijo de un
// re-drive anterior para que el id no crezca si el mensaje falla y se reinyecta varias veces.
func redriveMsgID(orig string, dlqSeq uint64) string {
	if i := strings.Index(orig, redriveIDMark); i >= 0 {
		orig = orig[:i]
	}
	if orig == "" {
		orig = "noid"
	}
	return fmt.Sprintf("%s%s%d", orig, redriveIDMark, dlqSeq)
}

func toDeadLetter(m dlqMsg, withBody bool) domain.DeadLetter {
	d := domain.DeadLetter{
		Seq:        m.seq,
		Reason:     m.hdr.Get(HdrDLQReason),
		Error:      m.hdr.Get(HdrDLQError),
		MsgID:      m.hdr.Get(nats.MsgIdHdr),
		Size:       len(m.data),
		FailedAt:   m.ts,
		Deliveries: parseUint(m.hdr.Get(HdrDLQDeliveries)),
	}
	if t, err := time.Parse(time.RFC3339Nano, m.hdr.Get(HdrDLQTimestamp)); err == nil {
		d.FailedAt = t
	}
	d.OriginalSeq = parseUint(m.hdr.Get(HdrDLQStreamSeq))
	d.RedriveCount = atoi(m.hdr.Get(HdrRedriveCount))

	// Extracción en mejor esfuerzo: un mensaje ilegible simpoemente no trae estos campos
	var ev struct {
		ID        string `json:"id"`
		TenantID  string `json:"tenant_id"`
		EventType string `json:"event_type"`
	}
	if json.Unmarshal(m.data, &ev) == nil {
		d.EventID, d.TenantID, d.EventType = ev.ID, ev.TenantID, ev.EventType
	}
	if d.TenantID == "" && d.EventID != "" {
		d.TenantID = domain.DefaultTenant // igual que el consumer al decodificar
	}

	body := strings.ToValidUTF8(string(m.data), "�")
	if withBody {
		d.Body = body
	} else {
		if len(body) > previewLen {
			body = strings.ToValidUTF8(body[:previewLen], "") + "…"
		}
		d.BodyPreview = body
	}
	return d
}

// parseUint y atoi leen cabeceras opcionales: un valor ausente o ilegible cuenta como 0.
func parseUint(s string) uint64 {
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

func atoi(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return n
}
