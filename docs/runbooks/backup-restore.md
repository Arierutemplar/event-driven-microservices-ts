# Runbook: backup y restore (PostgreSQL, NATS JetStream, Redis)

Los comandos usan el stack de `docker-compose.yml` (`docker compose exec`). En producción
usa los mecanismos del proveedor gestionado (snapshots, PITR) y sustituye host,
credenciales y rutas. Los backups deben guardarse cifrados, fuera del host de origen,
con retención definida y verificación periódica de restauración.

| Dependencia | Qué contiene | Criticidad | Estrategia |
|---|---|---|---|
| PostgreSQL | Fuente de verdad: `events`, `schema_migrations`, `export_state`, `duplicate_events_archive` | Crítica | `pg_dump -Fc` diario + PITR/WAL en producción |
| NATS JetStream | Eventos aceptados (`202`) aún no persistidos (`EVENTS`) y DLQ (`EVENTS_DLQ`) | Alta mientras haya backlog | `nats backup stream` + réplicas `NATS_STREAM_REPLICAS=3` |
| Redis | Resumen derivado y marcas de deduplicación (TTL 7 días) | Baja: reconstruible | AOF (`--appendonly yes`) + snapshot RDB |

Verificado en local (Docker Compose, proyecto aislado): restore de PostgreSQL con
660/660 filas y 5/5 migraciones; restore de `EVENTS_DLQ` con 6/6 mensajes; AOF de Redis
reescrito con estado `ok`.

## PostgreSQL

### Backup

```bash
docker compose exec -T postgres pg_dump -U postgres -d analytics -Fc -f /tmp/analytics.dump
docker compose cp postgres:/tmp/analytics.dump ./analytics-$(date +%Y%m%d%H%M).dump
```

### Restore (siempre primero en una base aislada)

```bash
docker compose exec -T postgres createdb -U postgres analytics_restore
docker compose exec -T postgres pg_restore -U postgres -d analytics_restore --no-owner /tmp/analytics.dump

# Verificación: conteos y migraciones deben coincidir con el origen
docker compose exec -T postgres psql -U postgres -d analytics -At -c "select count(*) from events"
docker compose exec -T postgres psql -U postgres -d analytics_restore -At -c "select count(*) from events"
docker compose exec -T postgres psql -U postgres -d analytics_restore -At -c "select count(*) from schema_migrations"
```

`pg_restore` sobre una base que ya contiene las tablas falla con `already exists` y
`duplicate key`: restaura siempre en una base vacía.

### Promoción a producción

1. Detén el exportador Parquet (escala a 0 o desactiva `S3_ENDPOINT`) para que
   `export_state` no avance durante el cambio.
2. Detén el consumo: escala el backend a 0. JetStream retiene los eventos aceptados
   mientras dure la ventana (`NATS_STREAM_MAX_AGE`).
3. Apunta `POSTGRES_HOST`/`POSTGRES_DB` a la base restaurada y arranca una réplica.
   Las migraciones se aplican solas y son idempotentes (`schema_migrations`).
4. Comprueba `/ready`, que `analytics_nats_pending_messages` baja a 0 y ejecuta
   `scripts/load/e2e.js` con `PROFILE=smoke`.
5. Revisa `export_state` antes de reactivar el exportador: si el restore es anterior al
   último export, el rango ya exportado se volverá a escribir; valida las particiones
   del data lake para evitar duplicados.

Los eventos re-entregados que ya estuvieran en la base restaurada se descartan por el
índice único `(tenant_id, event_id)`.

## NATS JetStream

Requiere el CLI `nats` (por ejemplo, la imagen `natsio/nats-box`) con acceso al servidor.
Los subcomandos `nats stream backup/restore` están obsoletos; usa `nats backup ...`.

```bash
# Desde la red del proyecto Compose (sustituye <proyecto>_default)
docker run --rm -it --network <proyecto>_default -v "$PWD/js-backup:/backup" natsio/nats-box:latest

nats -s nats://nats:4222 backup stream EVENTS     /backup/EVENTS
nats -s nats://nats:4222 backup stream EVENTS_DLQ /backup/EVENTS_DLQ

# Restore (el stream no debe existir; el backup incluye su configuración)
nats -s nats://nats:4222 stream rm EVENTS_DLQ -f
nats -s nats://nats:4222 backup restore stream /backup/EVENTS_DLQ
nats -s nats://nats:4222 stream info EVENTS_DLQ
```

- `EVENTS` usa retención *work-queue*: tras el consumo queda vacío, así que su backup
  solo protege el backlog pendiente. Hazlo con el consumer detenido para obtener un
  estado consistente.
- `EVENTS_DLQ` (retención por límites, 30 días) sí conserva datos de diagnóstico: es el
  stream cuyo backup importa en operación normal.
- Tras restaurar `EVENTS`, el backend recrea su consumer durable al arrancar
  (`CreateOrUpdateConsumer`).
- En producción la protección principal es la replicación (`NATS_STREAM_REPLICAS=3`
  sobre un clúster de 3 nodos); el backup cubre la pérdida del clúster completo.

## Redis

Redis no es fuente de verdad. Compose lo arranca con AOF (`--appendonly yes`) sobre el
volumen `redis-data`.

```bash
docker compose exec -T redis redis-cli -a "$REDIS_PASSWORD" --no-auth-warning BGREWRITEAOF
docker compose exec -T redis redis-cli -a "$REDIS_PASSWORD" --no-auth-warning INFO persistence
# aof_enabled:1, aof_last_bgrewrite_status:ok, rdb_last_bgsave_status:ok
docker compose cp redis:/data ./redis-backup
```

Si Redis se pierde sin backup:

- El rate limiting y el ticketing no tienen estado persistente relevante.
- El resumen `/api/metrics` vuelve a cero para los eventos anteriores; el historial en
  PostgreSQL permanece intacto.
- Las marcas de deduplicación se pierden: un evento re-entregado se volvería a contar
  en el resumen (nunca se duplica en PostgreSQL gracias al índice único).

## Simulacro periódico (recomendado: trimestral)

1. Restaurar PostgreSQL en una base aislada y comparar conteos por tenant.
2. Restaurar `EVENTS_DLQ` en un servidor NATS de pruebas.
3. Ejecutar `scripts/resilience/outage.ps1` contra un entorno de staging.
4. Registrar RTO/RPO medidos y actualizar este runbook.
