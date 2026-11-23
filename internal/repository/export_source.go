package repository

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Arierutemplar/event-driven-microservices-ts/internal/domain"
)

const (
	exportStateName = "parquet"
	exportLockID    = 727274002 // distinto del lock de migraciones
)

var _ domain.ExportSource = (*PostgresExportSource)(nil)

// PostgresExportSource lee los eventos históricos de PostgreSQL para el exportador a Parquet.
type PostgresExportSource struct {
	pool *pgxpool.Pool
}

// ExportSource devuelve el origen de exportación que comparte el pool del repositorio.
func (r *PostgresRepository) ExportSource() *PostgresExportSource {
	return &PostgresExportSource{pool: r.pool}
}

// TryLock toma un advisory lock de sesión sobre una conexión dedicada. Si el proceso muere, la
// conexión se cierra y el lock se libera solo.
func (s *PostgresExportSource) TryLock(ctx context.Context) (func(), bool, error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("export: conexión para lock: %w", err)
	}
	var got bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", exportLockID).Scan(&got); err != nil {
		conn.Release()
		return nil, false, fmt.Errorf("export: lock: %w", err)
	}
	if !got {
		conn.Release()
		return nil, false, nil
	}
	return func() {
		uctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if _, err := conn.Exec(uctx, "SELECT pg_advisory_unlock($1)", exportLockID); err != nil {
			// Con el lock en estado dudoso se descarta la conexión en lugar de devolverla al pool
			if cerr := conn.Conn().Close(context.Background()); cerr != nil {
				slog.Warn("export: descartar conexión con lock dudoso", "error", cerr)
			}
			slog.Warn("export: liberar advisory lock", "error", err)
		}
		conn.Release()
	}, true, nil
}

func (s *PostgresExportSource) State(ctx context.Context) (domain.ExportState, error) {
	var last int64
	var pending *int64
	err := s.pool.QueryRow(ctx, `SELECT last_id, pending_to FROM export_state WHERE name = $1`, exportStateName).
		Scan(&last, &pending)
	if err != nil {
		return domain.ExportState{}, fmt.Errorf("export: leer estado: %w", err)
	}
	st := domain.ExportState{LastID: last}
	if pending != nil {
		st.PendingTo = *pending
	}
	return st, nil
}

// Plan elige un lote contiguo de ids cuyo contenido ya no puede cambiar. Un id menor puede confirmarse
// después de uno mayor, así que solo se exportan filas insertadas por transacciones más antiguas que
// cualquier escritor en curso (age(xmin) mayor que la edad del xid activo más antiguo) y el lote se
// corta en la primera fila que no cumple. created_at < before se mantiene como margen adicional.
func (s *PostgresExportSource) Plan(ctx context.Context, afterID int64, before time.Time, limit int) (int64, int, error) {
	var upTo int64
	var n int
	err := s.pool.QueryRow(ctx, `
		WITH oldest AS (
			SELECT COALESCE(max(age(backend_xid)), -1) AS a
			FROM pg_stat_activity
			WHERE backend_xid IS NOT NULL AND pid <> pg_backend_pid()
		), cand AS (
			SELECT id, age(xmin) > (SELECT a FROM oldest) AS settled
			FROM events WHERE id > $1 AND created_at < $2 ORDER BY id LIMIT $3
		)
		SELECT COALESCE(max(id), 0), count(*) FROM cand
		WHERE id < COALESCE((SELECT min(id) FROM cand WHERE NOT settled), 9223372036854775807)`,
		afterID, before, limit).Scan(&upTo, &n)
	if err != nil {
		return 0, 0, fmt.Errorf("export: planificar lote: %w", err)
	}
	return upTo, n, nil
}

func (s *PostgresExportSource) MarkPending(ctx context.Context, upTo int64) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE export_state SET pending_to = $1, updated_at = now()
		WHERE name = $2 AND pending_to IS NULL AND last_id < $1`, upTo, exportStateName)
	if err != nil {
		return fmt.Errorf("export: fijar lote pendiente: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return errors.New("export: el estado cambió durante la planificación (¿otro exportador activo?)")
	}
	return nil
}

func (s *PostgresExportSource) Read(ctx context.Context, afterID, upTo int64) ([]domain.ExportRecord, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, event_id, tenant_id, event_type, user_id,
		       COALESCE(payload::text, ''), COALESCE(msg_id, ''), created_at
		FROM events WHERE id > $1 AND id <= $2 ORDER BY id`, afterID, upTo)
	if err != nil {
		return nil, fmt.Errorf("export: leer eventos: %w", err)
	}
	defer rows.Close()

	var out []domain.ExportRecord
	for rows.Next() {
		var r domain.ExportRecord
		if err := rows.Scan(&r.ID, &r.EventID, &r.TenantID, &r.EventType, &r.UserID, &r.Payload, &r.MsgID, &r.CreatedAt); err != nil {
			return nil, fmt.Errorf("export: escanear evento: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("export: leer eventos: %w", err)
	}
	return out, nil
}

// Commit solo avanza si el lote pendiente sigue siendo el esperado, para no pisar a otro exportador.
func (s *PostgresExportSource) Commit(ctx context.Context, upTo int64) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE export_state SET last_id = $1, pending_to = NULL, updated_at = now()
		WHERE name = $2 AND pending_to = $1`, upTo, exportStateName)
	if err != nil {
		return fmt.Errorf("export: confirmar lote: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return errors.New("export: el lote pendiente ya no coincide (confirmado por otro exportador)")
	}
	return nil
}
