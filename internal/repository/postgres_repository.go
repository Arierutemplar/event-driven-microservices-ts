package repository

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Arierutemplar/event-driven-microservices-ts/internal/domain"
	"github.com/Arierutemplar/event-driven-microservices-ts/internal/observability"
)

var (
	_ domain.EventRepository      = (*PostgresRepository)(nil)
	_ domain.BatchEventRepository = (*PostgresRepository)(nil)
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// PostgresConfig agrupa los parámetros de conexión.
type PostgresConfig struct {
	Host        string
	Port        string
	User        string
	Password    string
	DBName      string
	SSLMode     string
	SSLRootCert string
}

// DSN construye la URL de conexión escapando credenciales.
func (c PostgresConfig) DSN() string {
	query := url.Values{"sslmode": {c.SSLMode}}
	if c.SSLRootCert != "" {
		query.Set("sslrootcert", c.SSLRootCert)
	}
	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(c.User, c.Password),
		Host:     c.Host + ":" + c.Port,
		Path:     c.DBName,
		RawQuery: query.Encode(),
	}
	return u.String()
}

// PostgresRepository implementa domain.EventRepository sobre PostgreSQL.
type PostgresRepository struct {
	pool *pgxpool.Pool
}

// NewPostgresRepository conecta (reintentando hasta que la base de datos
// esté disponible o venza ctx) y aplica la migración al arrancar.
func NewPostgresRepository(ctx context.Context, dsn string) (*PostgresRepository, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("repository: configuración postgres: %w", err)
	}

	var pingErr error
	for attempt := 0; attempt < 30; attempt++ {
		if pingErr = pool.Ping(ctx); pingErr == nil {
			break
		}
		select {
		case <-ctx.Done():
			pool.Close()
			return nil, fmt.Errorf("repository: esperando postgres: %w", ctx.Err())
		case <-time.After(time.Second):
		}
	}
	if pingErr != nil {
		pool.Close()
		return nil, fmt.Errorf("repository: postgres no disponible: %w", pingErr)
	}

	if err := migrate(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}
	return &PostgresRepository{pool: pool}, nil
}

// migrate aplica el esquema bajo un advisory lock para que varias réplicas
// arrancando a la vez no choquen entre sí. El SQL es idempotente.
func migrate(ctx context.Context, pool *pgxpool.Pool) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("repository: migración: %w", err)
	}
	defer conn.Release()

	const lockID = 727274001
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", lockID); err != nil {
		return fmt.Errorf("repository: lock de migración: %w", err)
	}
	defer func() {
		// Sin cancelación heredada: el lock de sesión debe liberarse aunque ctx haya vencido.
		if _, err := conn.Exec(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock($1)", lockID); err != nil {
			slog.Error("repository: liberar lock de migración", "error", err)
		}
	}()

	if _, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			name TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("repository: crear tabla de migraciones: %w", err)
	}

	// ReadDir devuelve las entradas ordenadas por nombre (001_, 002_, ...)
	files, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("repository: leer migraciones: %w", err)
	}
	for _, f := range files {
		var applied bool
		if err := conn.QueryRow(ctx,
			"SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE name = $1)",
			f.Name()).Scan(&applied); err != nil {
			return fmt.Errorf("repository: consultar migración %s: %w", f.Name(), err)
		}
		if applied {
			continue
		}
		sql, err := migrationsFS.ReadFile("migrations/" + f.Name())
		if err != nil {
			return fmt.Errorf("repository: leer %s: %w", f.Name(), err)
		}
		if _, err := conn.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("repository: aplicar %s: %w", f.Name(), err)
		}
		if _, err := conn.Exec(ctx,
			"INSERT INTO schema_migrations (name) VALUES ($1)", f.Name()); err != nil {
			return fmt.Errorf("repository: registrar migración %s: %w", f.Name(), err)
		}
	}
	return nil
}

// SaveBatch inserta todos los eventos en una única sentencia (unnest de arrays).
// Con MessageID, ON CONFLICT descarta duplicados provenientes de redeliveries.
// Los errores de datos (clase SQLSTATE 22/23) se marcan como domain.ErrPermanent.
func (r *PostgresRepository) SaveBatch(ctx context.Context, events []*domain.Event) error {
	n := len(events)
	if n == 0 {
		return nil
	}

	ids := make([]string, n)
	types := make([]string, n)
	users := make([]string, n)
	tenants := make([]string, n)
	payloads := make([]*string, n)
	created := make([]time.Time, n)
	msgIDs := make([]*string, n)

	for i, e := range events {
		ids[i], types[i], users[i], created[i] = e.ID, e.EventType, e.UserID, e.Timestamp
		tenants[i] = tenantOrDefault(e.TenantID)
		if len(e.Payload) > 0 {
			p := string(e.Payload)
			payloads[i] = &p
		}
		if e.MessageID != "" {
			m := e.MessageID
			msgIDs[i] = &m
		}
	}

	tag, err := r.pool.Exec(ctx,
		`INSERT INTO events (event_id, event_type, user_id, payload, created_at, msg_id, tenant_id)
		 SELECT t.a, t.b, t.c, t.d::jsonb, t.e, t.f, t.g
		 FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::timestamptz[], $6::text[], $7::text[])
		      AS t(a, b, c, d, e, f, g)
		 ON CONFLICT DO NOTHING`,
		ids, types, users, payloads, created, msgIDs, tenants)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && (strings.HasPrefix(pgErr.Code, "22") || strings.HasPrefix(pgErr.Code, "23")) {
			return fmt.Errorf("repository: insertar lote: %w: %v", domain.ErrPermanent, err)
		}
		return fmt.Errorf("repository: insertar lote: %w", err)
	}
	observability.DedupDropped("database", n-int(tag.RowsAffected()))
	return nil
}

func (r *PostgresRepository) Save(ctx context.Context, e *domain.Event) error {
	if e == nil {
		return fmt.Errorf("repository: evento nulo")
	}

	// Sin payload se guarda NULL; json.RawMessage vacío no es JSON válido.
	var payload any
	if len(e.Payload) > 0 {
		payload = []byte(e.Payload)
	}

	tag, err := r.pool.Exec(ctx,
		`INSERT INTO events (event_id, event_type, user_id, payload, created_at, tenant_id)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT DO NOTHING`,
		e.ID, e.EventType, e.UserID, payload, e.Timestamp, tenantOrDefault(e.TenantID))
	if err != nil {
		return fmt.Errorf("repository: insertar evento: %w", err)
	}
	observability.DedupDropped("database", 1-int(tag.RowsAffected()))
	return nil
}

func tenantOrDefault(t string) string {
	if t == "" {
		return domain.DefaultTenant
	}
	return t
}

// Ping comprueba la conectividad con la base de datos.
func (r *PostgresRepository) Ping(ctx context.Context) error {
	return r.pool.Ping(ctx)
}

func (r *PostgresRepository) GetHistory(ctx context.Context, limit int) ([]*domain.Event, error) {
	if limit <= 0 {
		limit = 100
	}

	rows, err := r.pool.Query(ctx,
		`SELECT event_id, event_type, user_id, tenant_id, payload, created_at
		 FROM events
		 ORDER BY created_at DESC, id DESC
		 LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("repository: consultar historial: %w", err)
	}
	defer rows.Close()

	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (*domain.Event, error) {
		var (
			e       domain.Event
			payload []byte
		)
		if err := row.Scan(&e.ID, &e.EventType, &e.UserID, &e.TenantID, &payload, &e.Timestamp); err != nil {
			return nil, err
		}
		if payload != nil {
			e.Payload = json.RawMessage(payload)
		}
		return &e, nil
	})
}

// GetHistoryForTenant consulta únicamente eventos de un tenant.
func (r *PostgresRepository) GetHistoryForTenant(ctx context.Context, tenantID string, limit int) ([]*domain.Event, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	if tenantID == "" {
		tenantID = domain.DefaultTenant
	}

	rows, err := r.pool.Query(ctx,
		`SELECT event_id, event_type, user_id, tenant_id, payload, created_at
		 FROM events
		 WHERE tenant_id = $1
		 ORDER BY created_at DESC, id DESC
		 LIMIT $2`, tenantID, limit)
	if err != nil {
		return nil, fmt.Errorf("repository: consultar historial por tenant: %w", err)
	}
	defer rows.Close()

	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (*domain.Event, error) {
		var (
			e       domain.Event
			payload []byte
		)
		if err := row.Scan(&e.ID, &e.EventType, &e.UserID, &e.TenantID, &payload, &e.Timestamp); err != nil {
			return nil, err
		}
		if payload != nil {
			e.Payload = json.RawMessage(payload)
		}
		return &e, nil
	})
}

// Close cierra el pool de conexiones.
func (r *PostgresRepository) Close() error {
	r.pool.Close()
	return nil
}
