-- Deduplicación por event_id (semántica exactly-once a nivel de almacenamiento).
-- Solo se ejecuta mientras el índice no exista: elimina duplicados históricos
-- (conserva la primera inserción de cada event_id) y crea el índice único parcial.
-- Los event_id vacíos no tienen identidad y quedan fuera del índice.
DO $$
BEGIN
    IF to_regclass('idx_events_event_id') IS NULL AND to_regclass('idx_events_tenant_event_id') IS NULL THEN
        CREATE TABLE IF NOT EXISTS duplicate_events_archive (
            archived_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            event_row_id BIGINT,
            event_id TEXT,
            event_type TEXT,
            user_id TEXT,
            tenant_id TEXT,
            payload JSONB,
            created_at TIMESTAMPTZ
        );

        INSERT INTO duplicate_events_archive (
            event_row_id, event_id, event_type, user_id, tenant_id, payload, created_at
        )
        SELECT a.id, a.event_id, a.event_type, a.user_id,
               'default', a.payload, a.created_at
        FROM events a
        JOIN events b
          ON a.event_id <> ''
         AND a.event_id = b.event_id
         AND a.id > b.id;

        DELETE FROM events a
        USING events b
        WHERE a.event_id <> ''
          AND a.event_id = b.event_id
          AND a.id > b.id;

        CREATE UNIQUE INDEX idx_events_event_id ON events (event_id) WHERE event_id <> '';
    END IF;
END
$$;
