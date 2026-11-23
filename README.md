# Analytics Service

Plataforma de analíticas de eventos en tiempo real. Una API en Go recibe eventos multi-tenant, los publica en **NATS JetStream**, un consumer los inserta por lotes en **PostgreSQL** (con reintentos, circuit breaker, deduplicación y Dead Letter Queue), un exportador los vuelca periódicamente a un **data lake Parquet en MinIO/S3**, y el resumen de métricas se difunde por **WebSocket** a un dashboard **React**. Todo está instrumentado con **Prometheus** y **Grafana**.

## Enterprise baseline

- Redis comparte el resumen de métricas y el rate limiting entre réplicas cuando `REDIS_URL` está definido.
- Producción exige PostgreSQL, NATS, Redis y autenticación; CORS/WebSocket no permiten `*`.
- Las migraciones quedan registradas en `schema_migrations`; los duplicados históricos se archivan antes de aplicar unicidad.
- Todas las respuestas HTTP incluyen `X-Request-ID` para correlación operativa.
- Prometheus incluye reglas SLO/alertas y `docs/runbooks/` describe el procedimiento ante incidentes.
- `k8s/base/` contiene la base Kustomize con HPA, PDB, probes y límites de recursos.
- Los secretos de Kubernetes deben venir de External Secrets, Sealed Secrets o el proveedor cloud; no apliques `secrets.example.yaml` tal cual.

## Stack tecnológico

| Capa | Tecnología |
|---|---|
| API y workers | Go (`net/http`, `pgx`, `nats.go/jetstream`, `gorilla/websocket`, `sony/gobreaker`) |
| Mensajería | NATS JetStream (stream `EVENTS` + DLQ `EVENTS_DLQ`) |
| Base transaccional | PostgreSQL (migraciones embebidas, `payload JSONB`) |
| Data lake (OLAP) | Apache Parquet (`parquet-go`) sobre MinIO / S3 (`minio-go`) |
| Observabilidad | Prometheus (`/metrics`) + Grafana (dashboards aprovisionados) |
| Frontend | React 19 + TypeScript + Vite + Tailwind CSS (servido con Nginx) |
| Despliegue | Docker / Docker Compose |

## Arquitectura

```mermaid
flowchart LR
    CLI["Clientes / Simulador de tráfico"] -->|"POST /api/events"| API

    subgraph BE["Backend Go"]
        API["API HTTP<br/>RateLimit · TenantAuth · Instrument"]
        JS[("NATS JetStream<br/>stream EVENTS")]
        W["Consumer / Worker<br/>lotes · breaker · dedup"]
        DLQ[("Dead Letter Queue<br/>EVENTS_DLQ")]
        SUM["Resumen en memoria<br/>MetricSummary"]
        WS["WebSocket /ws"]
        EXP["Exportador Parquet<br/>watermark por id"]
        ADM["Admin DLQ<br/>/api/dlq · dlqctl"]

        API -->|"publish + ack"| JS
        JS -->|"Fetch por lotes"| W
        W -->|"error permanente o MaxDeliver"| DLQ
        W --> SUM --> WS
        ADM -->|"re-drive"| JS
        DLQ --> ADM
    end

    W -->|"INSERT por lotes<br/>ON CONFLICT DO NOTHING"| PG[("PostgreSQL")]
    PG -->|"lee rango (last_id, pending_to]"| EXP
    EXP -->|"PutObject con reintentos"| S3[("MinIO · Data Lake<br/>year=/month=/day=/tenant=")]
    WS --> UI["Dashboard React"]

    API -.->|"/metrics"| PROM["Prometheus"]
    W -.-> PROM
    EXP -.-> PROM
    PROM --> GRAF["Grafana"]
```

Sin `NATS_URL` el servicio funciona en **modo directo**: el evento entra a un canal en memoria y un worker pool concurrente lo persiste y actualiza el resumen (sin garantías de durabilidad; útil para desarrollo).

### Clean Architecture

Las dependencias apuntan siempre hacia `domain`.

```
cmd/
  api/                     Composition root: cablea dependencias, rutas y apagado ordenado
  dlqctl/                  CLI de operaciones sobre la DLQ
internal/
  domain/                  Entidades y puertos (Event, MetricSummary, repositorios, DLQ, export)
  usecase/                 AnalyticsService: ingesta, worker pool, resumen
  handler/                 Adaptadores HTTP/WebSocket (ingesta, historial, DLQ)
  queue/                   NATS JetStream: publisher, consumer por lotes, DLQ y administración
  repository/              PostgresRepository (+ migrations/), breaker, MemoryRepository, ExportSource
  export/                  Exportador Parquet, cliente S3, política de reintentos
  resilience/              Circuit breaker
  dedup/                   Caché TTL de event_id
  middleware/              CORS, RateLimiter, APIKey, TenantAuth, AdminOnly, WSToken
  observability/           Métricas Prometheus e Instrument()
  integration/             Tests de integración y concurrencia
frontend/                  Dashboard React + Vite + Tailwind
grafana/                   Datasource y dashboards aprovisionados
```

| Capa | Responsabilidad | Depende de |
|---|---|---|
| `domain` | Modelos y puertos (interfaces) | — |
| `usecase` | Lógica de negocio y concurrencia | `domain` |
| `repository`, `queue`, `export` | Adaptadores de infraestructura | `domain` |
| `handler` | Traducción HTTP/WS ↔ casos de uso | `usecase`, `domain` |
| `middleware`, `observability`, `resilience`, `dedup` | Preocupaciones transversales | stdlib y librerías |

## Características clave

- **Ingesta durable**: el `202` se devuelve solo tras el ack de JetStream (stream en disco, volumen `nats-data`).
- **Inserciones por lotes**: `Fetch(BATCH_SIZE, BATCH_FLUSH_INTERVAL)` e `INSERT … unnest` en una única sentencia.
- **Idempotencia en cuatro capas** por `event_id`, y semántica *exactly-once* a nivel de almacenamiento.
- **Circuit breaker** sobre PostgreSQL: pausa el consumo en vez de quemar entregas.
- **DLQ y re-drive**: reintentos con `MaxDeliver: 3`, metadatos del fallo, API administrativa y CLI.
- **Multi-tenancy y seguridad**: `X-API-Key` + `X-Tenant-ID`, token de WebSocket, rate limiting por IP, endpoints de DLQ solo para administrador.
- **WebSockets en tiempo real**: difusión del resumen a todos los clientes; el dashboard reconecta solo.
- **Data lake Parquet**: exportación particionada por fecha y tenant, con reintentos y sin duplicados.
- **Observabilidad**: métricas de HTTP, NATS, DLQ, breaker, dedup y exportación; dashboards de Grafana.
- **Apagado ordenado**: drena HTTP, consumer, workers y exportador antes de salir.

## Inicio rápido (Docker Compose)

Requisitos: Docker con Compose v2.

```bash
docker compose up -d --build
docker compose ps            # todos los servicios deben quedar "healthy"
```

Prueba de humo:

```bash
curl -X POST http://localhost:8080/api/events \
  -H "Content-Type: application/json" \
  -d '{"id":"evt-1","event_type":"click","user_id":"u-42","payload":"boton_comprar"}'
curl http://localhost:8080/api/metrics
```

Abre el dashboard en <http://localhost> y Grafana en <http://localhost:3002>. Para detener y borrar volúmenes: `docker compose down -v`.

### Servicios y puertos

| Servicio | Puerto host → contenedor | URL / notas |
|---|---|---|
| Dashboard (Nginx) | `80` → 80 | <http://localhost> |
| API Go | `8080` → 8080 | <http://localhost:8080> |
| PostgreSQL | `5433` → 5432 | base `analytics`, usuario/clave `postgres`/`postgres` |
| NATS | `4222` → 4222 (clientes), `8222` → 8222 (monitorización) | JetStream con volumen `nats-data` |
| MinIO | `9000` → 9000 (API S3), `9001` → 9001 (consola) | credenciales `minioadmin`/`minioadmin` (solo desarrollo) |
| Prometheus | `9091` → 9090 | <http://localhost:9091> |
| Grafana | `3002` → 3000 | <http://localhost:3002>, `admin`/`admin` (`GRAFANA_USER` / `GRAFANA_PASSWORD`) |
| Vite dev server | `5173` (estricto) | `npm run dev`; `npm run preview` usa `4173` |

> La imagen de MinIO por defecto es `cgr.dev/chainguard/minio` (MinIO ya no publica `minio/minio` en Docker Hub). Cámbiala con `MINIO_IMAGE`.
> **Producción**: cambia las credenciales de PostgreSQL, MinIO y Grafana, y define `API_KEY`, `TENANT_API_KEYS` y `WS_TOKEN`.

### Modo producción

El backend exige `ENVIRONMENT=production` junto con PostgreSQL y NATS reales, autenticación (`API_KEY` o `TENANT_API_KEYS`) y listas explícitas de orígenes CORS/WebSocket:

```bash
ENVIRONMENT=production
POSTGRES_HOST=postgres.internal
NATS_URL=nats://nats.internal:4222
API_KEY=<secreto-externo>
CORS_ALLOWED_ORIGINS=https://analytics.example.com
WS_ALLOWED_ORIGINS=https://analytics.example.com
```

La especificación HTTP versionada está en [`openapi.yaml`](./openapi.yaml). `/health` expresa liveness y `/ready` falla mientras PostgreSQL, NATS o el proceso de apagado impidan recibir tráfico.

Grafana arranca con Prometheus como fuente de datos y dos dashboards aprovisionados desde `grafana/` (carpeta *Analytics*): **Overview** (eventos, EPS, RPS, latencia p50/p95/p99, rechazos 429/503) y **NATS & Backend** (JetStream, DLQ, batch inserts).

### Ejecución local sin Docker

```bash
# Backend en modo directo (sin NATS ni PostgreSQL)
go run ./cmd/api

# Backend completo contra la infraestructura de Compose
docker compose up -d postgres nats minio
POSTGRES_HOST=localhost POSTGRES_PORT=5433 POSTGRES_PASSWORD=postgres \
NATS_URL=nats://localhost:4222 \
S3_ENDPOINT=localhost:9000 S3_ACCESS_KEY=minioadmin S3_SECRET_KEY=minioadmin \
go run ./cmd/api

# Frontend
cd frontend && npm install && npm run dev      # http://localhost:5173
```

En PowerShell define las variables con `$env:NOMBRE="valor"` y usa `npm.cmd` si los scripts están deshabilitados.

## Endpoints de la API

| Ruta | Método | Protección | Descripción |
|---|---|---|---|
| `/api/events` | POST | Rate limit por IP + clave de tenant o `API_KEY` | Ingesta de un evento (`202` / `400` / `401` / `403` / `429` / `503`) |
| `/api/events/history?limit=N` | GET | `API_KEY` | Últimos eventos persistidos (def. 50, máx. 1000) |
| `/api/metrics` | GET | `API_KEY` | Resumen agregado actual |
| `/metrics` | GET | `API_KEY` | Métricas Prometheus |
| `/ws` | WebSocket | `WS_TOKEN` o `API_KEY` (`?token=`, `Authorization: Bearer` o `X-API-Key`) | Resumen en tiempo real |
| `/api/dlq?limit=&after=&reason=` | GET | **Solo `API_KEY`** | Estadísticas y listado de la DLQ; `?seq=N` devuelve un mensaje completo |
| `/api/dlq/redrive` | POST | **Solo `API_KEY`** | Reinyecta mensajes de la DLQ en el subject principal |
| `/health` | GET | — | Sonda de vida |
| `/ready` | GET | — | Readiness: `503` durante el apagado |

Con las variables de autenticación vacías, las rutas correspondientes quedan abiertas (modo desarrollo); los endpoints de DLQ **fallan cerrado** (`503`) sin `API_KEY`.

## Configuración

### Backend

| Variable | Defecto | Descripción |
|---|---|---|
| `API_KEY` | _(vacío)_ | Clave de **administración**: protege lecturas y `/metrics`, ingesta para cualquier tenant, vale como token de `/ws` y es la única válida para la DLQ. |
| `TENANT_API_KEYS` | _(vacío)_ | Claves de ingesta por tenant: `clave1:tenantA,clave2:tenantB`. La clave fija el tenant. |
| `WS_TOKEN` | _(vacío)_ | Token de solo lectura para `/ws` (apto para el navegador). |
| `RATE_LIMIT_RPS` / `RATE_LIMIT_BURST` | `20` / `40` | Token bucket por IP en `/api/events`. |
| `POSTGRES_HOST` | _(vacío)_ | Vacío = repositorio en memoria sin persistencia. |
| `POSTGRES_PORT` / `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` | `5432` / `postgres` / _(vacío)_ / `analytics` | Conexión a PostgreSQL. |
| `POSTGRES_SSLMODE` | `disable` | Usa `require` o `verify-full` en producción. |
| `NATS_URL` | _(vacío)_ | Activa la ingesta vía JetStream. Vacío = modo directo (canal + 4 workers). |
| `NATS_STREAM` | `EVENTS` | Stream principal (la DLQ es `<stream>_DLQ`). |
| `NATS_SUBJECT` | `events.ingest` | Subject de ingesta. |
| `NATS_DURABLE` | `analytics-batch-writer` | Nombre del consumer durable. |
| `NATS_DLQ_SUBJECT` | `analytics.dlq` | Subject de la Dead Letter Queue. |
| `NATS_MAX_DELIVER` | `3` | Entregas máximas antes de enviar a la DLQ. |
| `BATCH_SIZE` / `BATCH_FLUSH_INTERVAL` | `500` / `1s` | Tamaño y ventana máxima de un lote. |
| `DB_BREAKER_FAILURES` | `2` | Fallos consecutivos que abren el circuito (se limita a `NATS_MAX_DELIVER-1`). |
| `DB_BREAKER_OPEN_TIMEOUT` | `15s` | Tiempo abierto antes de sondear la BD. |
| `DB_BREAKER_SLOW_THRESHOLD` | `5s` | Un lote más lento cuenta como fallo. |
| `DEDUP_TTL` / `DEDUP_MAX_ENTRIES` | `10m` / `100000` | Memoria de `event_id` ya vistos. |
| `SHUTDOWN_TIMEOUT` | `25s` | Presupuesto del apagado ordenado (compose: `stop_grace_period: 30s`). |
| `LOG_LEVEL` | `info` | Nivel de log estructurado (`slog`): `debug`, `info`, `warn`, `error`. |
| `LOG_FORMAT` | `text` | Formato de log: `text` o `json`. |
| `S3_ENDPOINT` | _(vacío; compose: `minio:9000`)_ | `host:puerto`. Vacío = exportación a Parquet desactivada. |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` | _(vacío; compose: `minioadmin`)_ | Credenciales S3. |
| `S3_BUCKET` / `S3_REGION` | `analytics-lake` / `us-east-1` | Bucket (se crea si no existe y hay permisos). |
| `S3_USE_SSL` / `S3_PREFIX` | `false` / _(vacío)_ | TLS y prefijo opcional de las claves. |
| `EXPORT_INTERVAL` / `EXPORT_POLL` / `EXPORT_LAG` | `5m` / `15s` / `30s` | Cadencia de exportación, sondeo y desfase de seguridad. |
| `EXPORT_BATCH_SIZE` | `50000` | Filas por lote Parquet. |
| `EXPORT_UPLOAD_TIMEOUT` | `30s` | Timeout por intento de subida. |
| `EXPORT_RETRY_ATTEMPTS` / `_BASE_DELAY` / `_MAX_DELAY` | `5` / `500ms` / `10s` | Reintentos con backoff exponencial y jitter. |

Las variables numéricas o de duración con un valor inválido (o no positivo) no abortan el arranque: se registra un aviso y se usa el valor por defecto.

Fijos en código: puerto `:8080` y cola de 1000 eventos en modo directo. `EXPORT_POLL`, `EXPORT_UPLOAD_TIMEOUT` y `EXPORT_RETRY_*_DELAY` no están mapeados en `docker-compose.yml`; añádelos al bloque `environment` del backend si necesitas cambiarlos allí.

### Compose y frontend

| Variable | Defecto | Descripción |
|---|---|---|
| `MINIO_IMAGE` | `cgr.dev/chainguard/minio:latest` | Imagen de MinIO. |
| `GRAFANA_USER` / `GRAFANA_PASSWORD` | `admin` / `admin` | Acceso a Grafana. |
| `FRONTEND_INGEST_KEY` / `FRONTEND_TENANT_ID` | _(vacío)_ | Clave de tenant y tenant que usa el dashboard para ingestar (build arg). |
| `VITE_WS_URL` / `VITE_API_URL` | `ws://localhost:8080/ws` / `http://localhost:8080` | Endpoints del backend. |
| `VITE_WS_TOKEN` / `VITE_API_KEY` | _(vacío)_ | Token del WebSocket (`?token=`) y clave de lectura. |
| `VITE_INGEST_API_KEY` / `VITE_TENANT_ID` | _(vacío)_ | Cabeceras de ingesta. |
| `DEV_HOST` / `DEV_PORT` / `DEV_PREVIEW_PORT` | `localhost` / `5173` / `4173` | Host y puertos estrictos de Vite (`DEV_HOST=true` escucha en todas las interfaces). |

> ⚠️ Todo `VITE_*` queda visible en el bundle. Usa `WS_TOKEN` (solo lectura) y una clave de tenant, nunca la de administración.

## Flujo de ingesta con NATS JetStream

- **Durabilidad**: el `202` se devuelve solo tras el ack del stream. El stream usa retención `WorkQueue` (cada mensaje se elimina al confirmarse) y `MaxAge` de 72 h.
- **At-least-once + idempotencia**: cada mensaje lleva un `Nats-Msg-Id` guardado en `events.msg_id` (índice único); las reentregas no duplican filas.
- **Fallos transitorios** de BD → `NakWithDelay` con retardo creciente. En la 3.ª entrega fallida el evento va a la **DLQ** y solo entonces se hace `Ack` del original, de modo que nunca bloquea el stream.
- **Fallos permanentes** (p. ej. `\u0000` en un JSONB, SQLSTATE `22P05`): el lote se biseca para aislar solo al evento venenoso y este va a la DLQ. Los mensajes ilegibles también.
- **Metadatos de la DLQ**: cuerpo y `Nats-Msg-Id` originales más `X-DLQ-Reason` (`decode_error` | `permanent_error` | `max_deliveries_exceeded`), `X-DLQ-Error`, `X-DLQ-Timestamp` (RFC 3339), `X-DLQ-Original-Subject`, `X-DLQ-Deliveries` y `X-DLQ-Stream-Seq`. Si el worker muere sin hacer ack, un suscriptor al advisory `MAX_DELIVERIES` mueve igualmente el mensaje a la DLQ (retención 30 días).
- **Métricas**: `analytics_nats_published_total`, `_publish_errors_total`, `_consumed_total`, `_insert_errors_total`, `_decode_errors_total`, `analytics_nats_pending_messages`, `_ack_pending_messages`, `analytics_nats_batch_size`, `_batch_insert_duration_seconds` y `analytics_dlq_messages_total{reason}`.
- **Limitación**: el resumen (`/api/metrics`, WebSocket) es por instancia, se reinicia al arrancar y solo cuenta lotes persistidos por esa instancia.

## Resiliencia

**Circuit breaker** (`sony/gobreaker/v2`). Envuelve `Save`/`SaveBatch`. Se abre tras `DB_BREAKER_FAILURES` fallos consecutivos o un lote más lento que `DB_BREAKER_SLOW_THRESHOLD`. Abierto, el consumer **no hace Fetch**: los mensajes quedan intactos en JetStream sin gastar entregas. Pasado `DB_BREAKER_OPEN_TIMEOUT` pasa a semiabierto y sondea con `Ping`; si responde, cierra y reanuda solo. Los errores permanentes (datos inválidos) no cuentan. Métricas: `analytics_circuit_breaker_state{name}` (0 cerrado, 1 semiabierto, 2 abierto), `_transitions_total`, `_rejected_total`, `_slow_calls_total`.

**Idempotencia** por `event_id` en cuatro capas: (1) `Nats-Msg-Id` con ventana de 2 min en el stream; (2) caché TTL en el consumer; (3) caché en el worker pool del modo directo; (4) índice único `(tenant_id, event_id)` + `ON CONFLICT DO NOTHING`. Si el cliente no envía `id`, el servidor genera uno aleatorio. Métrica: `analytics_dedup_dropped_total{stage}`.

> ⚠️ La migración `003` **borra las filas históricas con `event_id` repetido** (conserva la de menor `id`) antes de crear el índice único.

**Apagado ordenado** (SIGINT/SIGTERM; una segunda señal fuerza la salida): `/ready` pasa a 503 → se cierra HTTP → el consumer vuelca el lote en curso y confirma → se drena el worker pool → se detiene el exportador → se cierran WebSockets, NATS y la BD. Todo limitado por `SHUTDOWN_TIMEOUT`.

## Operaciones: re-drive de la DLQ

Una vez corregida la causa (esquema, migración, BD), los mensajes del stream `<NATS_STREAM>_DLQ` se reinyectan en el subject principal para que el pipeline los reprocese.

- **Solo administrador y fail-closed**: `/api/dlq*` acepta únicamente `API_KEY` y responde `503` si no está definida. Tiene su propio limitador (2 req/s) y queda auditado en el log.
- **Selector obligatorio**: `seqs`, `reason` o `all=true`; un cuerpo vacío nunca reinyecta toda la cola. Máximo por operación: 5000 (por defecto 100). `dry_run` solo lista.
- **Sin pérdida**: publicar → confirmar (ack) → borrar de la DLQ. El `Nats-Msg-Id` es `<id original>#dlq<seq>`, determinista, así que repetir un re-drive no duplica.
- ⚠️ Los `decode_error` **siempre volverán a fallar**: filtra con `reason=permanent_error` o `max_deliveries_exceeded`.
- Métricas: `analytics_dlq_depth` (úsala para alertar) y `analytics_dlq_redriven_total{result}`.

**CLI** (`cmd/dlqctl`, incluida en la imagen del backend). Se conecta a NATS y no crea streams ni consumers:

```bash
dlqctl list [--reason R] [--after SEQ] [--limit N] [--json]
dlqctl show 42
dlqctl redrive --seq 3,7 --dry-run
dlqctl redrive --reason permanent_error --limit 500        # pide confirmación (--yes la omite)
dlqctl redrive --all --yes

docker compose exec -e NATS_URL=nats://nats:4222 backend dlqctl list      # dentro de Compose
NATS_URL=nats://localhost:4222 go run ./cmd/dlqctl list                   # desde el host
```

## Data lake: exportación a Parquet (MinIO / S3)

Un worker en segundo plano lee los eventos **desde PostgreSQL** (marca de agua por `events.id`, tabla `export_state`), los serializa a Parquet (Snappy) y los sube a un bucket S3. Se activa al definir `S3_ENDPOINT`.

```
s3://analytics-lake/year=2026/month=11/day=04/tenant=acme/events-31-40.parquet
```

El nombre incluye el rango de ids (`events-<desde>-<hasta>.parquet`) para que varios lotes del mismo día no se sobrescriban.

- **Disparadores**: cada `EXPORT_POLL` se evalúa; se exporta al acumular `EXPORT_BATCH_SIZE` filas elegibles o al cumplirse `EXPORT_INTERVAL`. Si el lote sale lleno se siguen drenando lotes.
- **Sin duplicados**: antes de subir se persiste el rango (`pending_to`). Un reintento o reinicio reutiliza exactamente ese rango y las mismas claves, así que los objetos se sobrescriben. La marca de agua avanza solo tras subir **todas** las particiones.
- **Un único exportador**: un advisory lock de PostgreSQL garantiza que solo una réplica exporta.
- **Desfase (`EXPORT_LAG`)**: solo se exportan filas más antiguas que el desfase, porque los ids de una secuencia no se confirman estrictamente en orden. ⚠️ Una transacción abierta durante más tiempo que el desfase podría quedar fuera.
- **Reintentos**: backoff exponencial con jitter. Los errores permanentes (credenciales, bucket inexistente, 4xx salvo 408/429) no se reintentan. Tras agotar reintentos, el exportador espera con backoff creciente (máx. 5 min) y retoma el mismo rango.
- **No se borran filas** de `events` tras exportar.
- **Métricas**: `analytics_parquet_exported_total` (archivos), `_exported_events_total`, `_bytes_transferred_total`, `_export_duration_seconds{result}`, `_export_errors_total{stage}`, `_upload_retries_total`, `_last_success_timestamp_seconds`, `_watermark_id`.

Consulta de ejemplo con DuckDB:

```sql
SELECT event_type, count(*)
FROM read_parquet('s3://analytics-lake/year=*/month=*/day=*/tenant=acme/*.parquet', hive_partitioning = true)
GROUP BY 1;
```

## Testing e integración

### Pruebas Go

```bash
go build ./... && go vet ./...
go test ./...                                   # unitarias; las de infraestructura se omiten sin variables TEST_*
go test -race ./...                             # con detector de condiciones de carrera
go test -race -count=20 ./internal/usecase      # repetir para detectar fallos intermitentes
go test -bench=Concurrent -run=^$ ./internal/usecase
```

`-race` requiere cgo y un compilador de C. En Windows, o sin compilador local, ejecútalo en un contenedor:

```bash
docker run --rm -v "$PWD:/src" -w /src -e CGO_ENABLED=1 golang:1.27 go test -race ./...
```

### Integración con NATS y PostgreSQL reales

Cubre el ciclo HTTP → JetStream → consumer → INSERT por lotes → resumen → WebSocket, la DLQ y su re-drive, el breaker, el exportador y 500 eventos concurrentes sin pérdidas ni duplicados. Cada test usa stream, consumer y `user_id` únicos y limpia lo que crea.

```bash
docker compose up -d nats postgres
TEST_NATS_URL=nats://localhost:4222 \
TEST_POSTGRES_DSN="postgres://postgres:postgres@localhost:5433/analytics?sslmode=disable" \
go test -race -count=1 ./...
```

> `TestPostgresRepository` no es idempotente sobre una base ya usada; para una ejecución limpia recrea el volumen (`docker compose down -v`).

`TestConcurrentDirectMode` (100 productores HTTP contra el worker pool, con clientes WebSocket entrando y saliendo) se ejecuta siempre, sin infraestructura.

### Ejemplos con curl

Payload de texto y de objeto JSON anidado:

```bash
curl -X POST http://localhost:8080/api/events -H "Content-Type: application/json" \
  -d '{"id":"evt-1","event_type":"click","user_id":"u-42","payload":"boton_comprar"}'

curl -X POST http://localhost:8080/api/events -H "Content-Type: application/json" \
  -d '{"id":"evt-2","event_type":"purchase","user_id":"u-42","payload":{"order_id":"A-100","total":59.9,"items":[{"sku":"X1","qty":2}]}}'
```

Multi-tenant (con `TENANT_API_KEYS="ka:acme"`; el tenant sale de la clave):

```bash
curl -X POST http://localhost:8080/api/events -H "Content-Type: application/json" \
  -H "X-API-Key: ka" -H "X-Tenant-ID: acme" \
  -d '{"id":"evt-3","event_type":"click","user_id":"u-42"}'
```

Respuestas: `401` clave ausente o inválida, `403` clave de otro tenant, `400` `X-Tenant-ID` inválido (1-64 caracteres `[A-Za-z0-9._-]`). `API_KEY` puede ingestar para cualquier tenant (por defecto `default`). El `id` es único **por tenant**.

Consultas (añade `-H "X-API-Key: $API_KEY"` si lo definiste):

```bash
curl http://localhost:8080/api/metrics
curl "http://localhost:8080/api/events/history?limit=10"
curl http://localhost:8080/metrics
```

WebSocket (el navegador no puede fijar cabeceras, por eso se usa `?token=`): `ws://localhost:8080/ws?token=<WS_TOKEN>`.

> En PowerShell usa `curl.exe` y envía el cuerpo desde un archivo (`--data-binary "@evento.json"`) para evitar problemas de comillas.

### Simulación de tráfico masivo

**Desde el dashboard**: el panel *Simulador de Tráfico* ofrece "Simular Ráfaga (50 Eventos)" (50 eventos `user_action` con payloads aleatorios; reintenta los `429`) y cargas de 50/200/1000 eventos con progreso y cancelación. Muestra spinner y toasts, y bloquea los botones mientras hay una simulación activa.

**Desde la terminal**. El rate limit por defecto (20 req/s, ráfaga 40 por IP) hará que aparezcan `429`; para pruebas de carga súbelo:

```bash
RATE_LIMIT_RPS=2000 RATE_LIMIT_BURST=4000 docker compose up -d backend

seq 1 1000 | xargs -P 50 -I{} curl -s -o /dev/null -w "%{http_code}\n" \
  -X POST http://localhost:8080/api/events -H "Content-Type: application/json" \
  -d '{"id":"load-{}","event_type":"user_action","user_id":"u-{}","payload":{"n":{}}}' | sort | uniq -c
```

```powershell
1..300 | ForEach-Object {
  $body = '{"id":"load-' + $_ + '","event_type":"user_action","user_id":"u-1","payload":{"n":' + $_ + '}}'
  Invoke-RestMethod -Method Post -Uri http://localhost:8080/api/events -ContentType "application/json" -Body $body | Out-Null
}
```

Comprueba el efecto en `/api/metrics`, en el dashboard (picos en tiempo real) y en Grafana (EPS, tamaño de lote, latencia de inserción).

### 🧪 Pruebas de Carga y Rendimiento (Benchmarking)

#### Objetivo

[scripts/k6-test.js](scripts/k6-test.js) es una prueba de carga con [k6](https://k6.io) sobre `POST /api/events` que valida:

- **Ingesta masiva**: que la API acepta más de **1,000 req/s** sostenidos sin degradar la latencia.
- **Estabilidad del pipeline** Go → NATS JetStream → consumer → PostgreSQL: que cada evento aceptado (`202`) acaba persistido, sin pérdidas ni duplicados, y que el backlog del stream se drena al terminar la carga.
- **Fan-out por WebSocket** (indirecto): cada evento aceptado dispara la difusión del resumen a los clientes `/ws`. El script **no abre conexiones WebSocket ni mide su latencia**; para observarla, abre el dashboard durante la prueba y revisa `analytics_nats_pending_messages` y la latencia de inserción en Grafana.

**Perfil de carga**: rampa de 10 → 100 usuarios virtuales en 30 s, 1 minuto sostenido a 100 VUs y bajada a 0 en 10 s. Cada petición envía un evento aleatorio (`page_view`, `button_click` o `purchase`) con `id` único, `user_id`, `timestamp` ISO 8601 y metadatos simulados en `payload` (navegador, país, página y, en las compras, datos del pedido).

#### Umbrales de aceptación (thresholds)

k6 termina con código de salida distinto de cero si alguno falla, por lo que sirve como *gate* en CI.

| Métrica k6 | Umbral | Significado |
|---|---|---|
| `http_req_duration` | `p(95) < 200` | El 95 % de las peticiones responde en menos de 200 ms |
| `http_req_failed` | `rate < 0.01` | Menos de 1 % de errores HTTP |
| `http_reqs` | `rate > 1000` | Throughput medio superior a 1,000 req/s (promedio de toda la prueba, incluidas rampa y bajada) |
| `checks` | `rate > 0.99` | Más de 99 % de respuestas `202` con `event_accepted` |

Además se reportan los contadores `events_accepted` (202), `events_rate_limited` (429) y `events_overloaded` (503).

#### Preparación: rate limiter

El limitador por IP (20 req/s, ráfaga 40) rechazará con `429` casi todo el tráfico de k6, que sale de una sola IP, y los umbrales fallarán. Levanta el backend con límites altos para la prueba y restáuralos después:

```bash
RATE_LIMIT_RPS=50000 RATE_LIMIT_BURST=100000 docker compose up -d backend
```

```powershell
$env:RATE_LIMIT_RPS="50000"; $env:RATE_LIMIT_BURST="100000"; docker compose up -d backend
```

#### Ejecución

**Linux / macOS / k6 en el PATH**:

```bash
k6 run scripts/k6-test.js
```

**Windows** (ruta de instalación por defecto):

```powershell
& "$env:ProgramFiles\k6\k6.exe" run scripts/k6-test.js
```

Variables opcionales (`-e`): `BASE_URL` (def. `http://localhost:8080`), `API_KEY` (cabecera `X-API-Key`, obligatoria si el backend define `API_KEY` o `TENANT_API_KEYS`) y `TENANT_ID` (`X-Tenant-ID`):

```bash
k6 run -e BASE_URL=http://localhost:8080 -e API_KEY=mi-clave -e TENANT_ID=acme scripts/k6-test.js
```

#### Verificar persistencia y limpiar

Todos los `id` generados empiezan por `k6-`. El consumer inserta más despacio de lo que la API acepta, así que JetStream actúa de buffer y la base tarda unos segundos en igualar el conteo tras la prueba (observa `analytics_nats_pending_messages` hasta que llegue a 0):

```bash
docker compose exec postgres psql -U postgres -d analytics -c "select count(*), count(distinct event_id) from events where event_id like 'k6-%'"
docker compose exec postgres psql -U postgres -d analytics -c "delete from events where event_id like 'k6-%'"
```

#### Resultado de referencia

Backend en Docker Compose sobre una máquina de desarrollo, con los límites del rate limiter elevados:

| Métrica | Resultado | Umbral |
|---|---|---|
| Throughput medio | ≈ 4,300 req/s | > 1,000 req/s |
| `http_req_duration` p(95) | ≈ 18 ms | < 200 ms |
| `http_req_failed` | 0 % | < 1 % |
| Eventos aceptados → persistidos | 431,322 → 431,322 (0 duplicados) | — |

Son cifras orientativas: dependen del hardware y de que el cliente k6 comparta máquina con el stack. Los resultados con otro `sleep` o tamaño de lote del consumer variarán.

### Forzar eventos a la Dead Letter Queue

**1. Error permanente (`permanent_error`)**: un `\u0000` dentro de un JSONB hace fallar el `INSERT` en PostgreSQL. Requiere NATS activo (el caso de Compose):

```bash
curl -X POST http://localhost:8080/api/events -H "Content-Type: application/json" \
  -d '{"id":"dlq-1","event_type":"user_action","user_id":"u-1","payload":"fallo\u0000forzado"}'
```

El botón "Simular Fallo (DLQ)" del dashboard envía este mismo evento.

**2. Mensaje ilegible (`decode_error`)**, publicando basura directamente en el subject de ingesta:

```bash
docker run --rm --network <proyecto>_default natsio/nats-box \
  nats --server nats://nats:4222 pub events.ingest "esto no es json"
```

(`docker network ls` muestra el nombre de la red de Compose.)

**Verificar y reprocesar**:

```bash
curl -H "X-API-Key: $API_KEY" http://localhost:8080/api/dlq
curl -H "X-API-Key: $API_KEY" "http://localhost:8080/api/dlq?seq=1"
curl -X POST -H "X-API-Key: $API_KEY" -d '{"reason":"permanent_error","limit":100,"dry_run":true}' \
  http://localhost:8080/api/dlq/redrive
docker compose exec -e NATS_URL=nats://nats:4222 backend dlqctl list
```

El contador `analytics_dlq_messages_total{reason="permanent_error"}` y el gauge `analytics_dlq_depth` suben; el dashboard muestra el indicador de DLQ. Reinyectar un evento con `\u0000` volverá a fallar: sirve para probar el ciclo completo (el contador de re-drives aumenta).

> Una caída de PostgreSQL **no** envía eventos a la DLQ: el circuit breaker pausa el consumo y los mensajes esperan en el stream hasta que la BD vuelve.

## Frontend

```bash
cd frontend
npm install
npm run dev        # http://localhost:5173 (puerto estricto: falla si está ocupado)
npm run build      # type-check (tsc estricto) + build de producción
```

- Conexión WebSocket con reconexión automática (backoff exponencial con jitter, también al recuperar red o foco) y estado Conectado/Desconectado.
- Total de eventos, eventos/s con sparkline, gráfica por tipo de evento y feed en tiempo real (`recent_events`).
- Indicador de salud: eventos enviados a la DLQ y rechazos observados (503 cola llena, 429 rate limit, errores de red).
- Panel de pruebas de ingesta y Simulador de Tráfico (ver arriba).

## Decisiones de diseño y limitaciones conocidas

- **Garantías de entrega**: con NATS es *at-least-once* con deduplicación (efecto *exactly-once* en el almacenamiento). En modo directo (sin `NATS_URL`) la cola es en memoria y un reinicio pierde los eventos aún no procesados.
- **Resumen no persistente**: `MetricSummary` empieza en cero al reiniciar; el historial completo permanece en PostgreSQL. Además, con ingesta por lotes solo se ve el último evento de cada lote.
- **IP del cliente**: el rate limiter usa `RemoteAddr`. Detrás de un proxy inverso hay que resolver la IP real de forma confiable (`X-Forwarded-For` es falsificable).
- **CORS abierto (`*`)**: apropiado para desarrollo; en producción restringe los orígenes.
- **Token de `/ws` estático**: es un secreto compartido sin expiración y, en la URL, puede quedar en logs de proxies. Para producción: tokens de corta duración (JWT) y `wss://`.
- **Lecturas no segmentadas por tenant**: el resumen y el historial son una vista global de operador; las claves de tenant solo sirven para **ingestar**.
- **Sin claves configuradas, `X-Tenant-ID` no es verificable**. Define `TENANT_API_KEYS` o `API_KEY` para que sea de confianza.
- **Cardinalidad de métricas**: la etiqueta `event_type` se limita a 50 valores; el resto se agrupa como `other`.
- **Crecimiento de la tabla**: `events` crece sin límite y no se purga tras exportar a Parquet; define una política de retención o particiona por `created_at`.
- **Sustituir el repositorio**: basta con implementar `domain.BatchEventRepository` e inyectarlo en `newEventRepository()` de `cmd/api/main.go`.
