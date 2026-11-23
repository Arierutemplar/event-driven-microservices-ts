// Package export vuelca periódicamente los eventos históricos de PostgreSQL a un data lake en
// formato Parquet (S3/MinIO), particionado por fecha y tenant.
//
// Garantías:
//   - Un único exportador activo entre réplicas (advisory lock).
//   - Progreso por watermark de id persistido en PostgreSQL; solo avanza tras subir TODOS los
//     objetos del lote.
//   - Un lote se fija (pending_to) antes de subirlo: si algo falla, el reintento reutiliza el mismo
//     rango y por tanto las mismas claves de objeto, sobrescribiéndolas. El lake no acumula
//     duplicados aunque haya caídas del storage o reinicios a mitad de lote.
//   - Solo se exportan filas con más de Lag de antigüedad, para no saltarse transacciones que
//     aún no han confirmado (los ids de una BIGSERIAL no se confirman estrictamente en orden).
package export

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/Arierutemplar/event-driven-microservices-ts/internal/domain"
	"github.com/Arierutemplar/event-driven-microservices-ts/internal/observability"
)

// Config del exportador. Los valores cero toman el valor por defecto.
type Config struct {
	Interval      time.Duration // volcado como máximo cada Interval aunque el lote no se llene (5m)
	Poll          time.Duration // frecuencia con la que se comprueba si toca volcar (15s)
	Lag           time.Duration // antigüedad mínima de una fila para exportarla (30s)
	BatchSize     int           // filas por lote; alcanzarlo dispara un volcado inmediato (50000)
	Prefix        string        // prefijo de las claves, p. ej. "events" (vacío = raíz del bucket)
	UploadTimeout time.Duration // límite por intento de subida (30s)
	Retry         RetryPolicy
}

func (c *Config) applyDefaults() {
	if c.Interval <= 0 {
		c.Interval = 5 * time.Minute
	}
	if c.Poll <= 0 {
		c.Poll = 15 * time.Second
	}
	if c.Lag < 0 {
		c.Lag = 0
	} else if c.Lag == 0 {
		c.Lag = 30 * time.Second
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 50000
	}
	if c.UploadTimeout <= 0 {
		c.UploadTimeout = 30 * time.Second
	}
	c.Prefix = strings.Trim(c.Prefix, "/")
	c.Retry = c.Retry.withDefaults()
}

const maxFailureBackoff = 5 * time.Minute

// Exporter es el worker de exportación.
type Exporter struct {
	src   domain.ExportSource
	store ObjectStore
	cfg   Config

	lastRun     time.Time
	failures    int
	nextAttempt time.Time
	bucketReady bool
}

func New(src domain.ExportSource, store ObjectStore, cfg Config) *Exporter {
	cfg.applyDefaults()
	return &Exporter{src: src, store: store, cfg: cfg, lastRun: time.Now()}
}

// Run bloquea hasta que se cancele ctx. Un lote en vuelo se aborta; su rango queda fijado en
// PostgreSQL y se retoma en el siguiente arranque.
func (e *Exporter) Run(ctx context.Context) {
	slog.Info("export: parquet activo", "intervalo", e.cfg.Interval, "lote", e.cfg.BatchSize, "desfase", e.cfg.Lag)
	t := time.NewTicker(e.cfg.Poll)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			e.tick(ctx)
		}
	}
}

func (e *Exporter) tick(ctx context.Context) {
	now := time.Now()
	if now.Before(e.nextAttempt) {
		return
	}
	due := now.Sub(e.lastRun) >= e.cfg.Interval
	minRows := e.cfg.BatchSize
	if due {
		minRows = 1
	}
	rows, err := e.Drain(ctx, minRows)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		e.failures++
		wait := e.cfg.Poll << min(e.failures, 6)
		if wait > maxFailureBackoff {
			wait = maxFailureBackoff
		}
		e.nextAttempt = now.Add(wait)
		slog.Error("export: error", "fallos_consecutivos", e.failures, "proximo_intento", wait.Round(time.Second), "error", err)
		return
	}
	e.failures = 0
	e.nextAttempt = time.Time{}
	if due {
		e.lastRun = now
	}
	if rows > 0 {
		slog.Info("export: eventos volcados", "filas", rows)
	}
}

// Drain exporta lotes mientras haya al menos minRows filas elegibles y los lotes salgan llenos.
// Devuelve las filas exportadas. Si otra réplica tiene el lock, no hace nada.
func (e *Exporter) Drain(ctx context.Context, minRows int) (int, error) {
	release, ok, err := e.src.TryLock(ctx)
	if err != nil {
		observability.ParquetError("state")
		return 0, err
	}
	if !ok {
		return 0, nil
	}
	defer release()

	total := 0
	for {
		n, err := e.exportBatch(ctx, minRows)
		total += n
		if err != nil {
			return total, err
		}
		if n == 0 {
			return total, nil
		}
	}
}

// RunOnce fuerza un volcado inmediato de todo lo elegible (útil en pruebas y operaciones).
func (e *Exporter) RunOnce(ctx context.Context) (int, error) { return e.Drain(ctx, 1) }

func (e *Exporter) exportBatch(ctx context.Context, minRows int) (rows int, err error) {
	start := time.Now()
	st, err := e.src.State(ctx)
	if err != nil {
		observability.ParquetError("state")
		return 0, err
	}

	if st.PendingTo == 0 {
		upTo, n, err := e.src.Plan(ctx, st.LastID, time.Now().Add(-e.cfg.Lag), e.cfg.BatchSize)
		if err != nil {
			observability.ParquetError("plan")
			return 0, err
		}
		if n == 0 || n < minRows {
			return 0, nil
		}
		if err := e.src.MarkPending(ctx, upTo); err != nil {
			observability.ParquetError("plan")
			return 0, err
		}
		st.PendingTo = upTo
	}

	defer func() {
		if rows > 0 || err != nil {
			observability.ParquetBatch(time.Since(start), err == nil, st.PendingTo)
		}
	}()

	recs, err := e.src.Read(ctx, st.LastID, st.PendingTo)
	if err != nil {
		observability.ParquetError("read")
		return 0, err
	}

	if !e.bucketReady {
		if err := e.ensureBucket(ctx); err != nil {
			observability.ParquetError("bucket")
			return 0, err
		}
	}

	for _, p := range partition(recs, e.cfg.Prefix, st.LastID+1, st.PendingTo) {
		data, err := Encode(p.recs)
		if err != nil {
			observability.ParquetError("encode")
			return 0, err
		}
		if err := e.upload(ctx, p.key, data); err != nil {
			observability.ParquetError("upload")
			e.bucketReady = false // podría haberse borrado el bucket; se vuelve a comprobar
			return 0, fmt.Errorf("subir %s: %w", p.key, err)
		}
		observability.ParquetFile(len(p.recs), len(data))
	}

	if err := e.src.Commit(ctx, st.PendingTo); err != nil {
		observability.ParquetError("commit")
		return 0, err
	}
	return len(recs), nil
}

func (e *Exporter) ensureBucket(ctx context.Context) error {
	err := e.cfg.Retry.Do(ctx, func(c context.Context) error {
		c, cancel := context.WithTimeout(c, e.cfg.UploadTimeout)
		defer cancel()
		return e.store.EnsureBucket(c)
	}, e.logRetry("bucket"))
	if err != nil && IsPermanent(err) {
		// Sin permiso para listar/crear el bucket no implica que no se pueda escribir: se intenta igual
		slog.Warn("export: no se pudo verificar el bucket (se intentará subir igualmente)", "error", err)
		e.bucketReady = true
		return nil
	}
	if err == nil {
		e.bucketReady = true
	}
	return err
}

func (e *Exporter) upload(ctx context.Context, key string, data []byte) error {
	return e.cfg.Retry.Do(ctx, func(c context.Context) error {
		c, cancel := context.WithTimeout(c, e.cfg.UploadTimeout)
		defer cancel()
		return e.store.Put(c, key, data)
	}, func(attempt int, err error, wait time.Duration) {
		observability.ParquetUploadRetry()
		e.logRetry("subida "+key)(attempt, err, wait)
	})
}

func (e *Exporter) logRetry(what string) func(int, error, time.Duration) {
	return func(attempt int, err error, wait time.Duration) {
		slog.Warn("export: operación fallida, reintentando", "operacion", what, "intento", attempt, "max", e.cfg.Retry.MaxAttempts, "espera", wait.Round(time.Millisecond), "error", err)
	}
}

type part struct {
	key  string
	recs []domain.ExportRecord
}

var unsafeKeyChars = regexp.MustCompile(`[^A-Za-z0-9._-]`)

// sanitizeTenant evita que un tenant con caracteres raros altere la jerarquía de claves.
func sanitizeTenant(t string) string {
	t = unsafeKeyChars.ReplaceAllString(t, "_")
	if t == "" || t == "." || t == ".." {
		return "_"
	}
	return t
}

// partition agrupa por (día UTC, tenant) y construye claves deterministas:
//
//	[prefix/]year=YYYY/month=MM/day=DD/tenant=ID/events-<desdeID>-<hastaID>.parquet
//
// El nombre depende solo del rango del lote, así que reintentar sobrescribe en vez de duplicar.
func partition(recs []domain.ExportRecord, prefix string, fromID, toID int64) []part {
	groups := map[string]*part{}
	for _, r := range recs {
		t := r.CreatedAt.UTC()
		dir := fmt.Sprintf("year=%04d/month=%02d/day=%02d/tenant=%s", t.Year(), int(t.Month()), t.Day(), sanitizeTenant(r.TenantID))
		if prefix != "" {
			dir = prefix + "/" + dir
		}
		key := fmt.Sprintf("%s/events-%d-%d.parquet", dir, fromID, toID)
		g, ok := groups[key]
		if !ok {
			g = &part{key: key}
			groups[key] = g
		}
		g.recs = append(g.recs, r)
	}
	out := make([]part, 0, len(groups))
	for _, g := range groups {
		out = append(out, *g)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].key < out[j].key })
	return out
}
