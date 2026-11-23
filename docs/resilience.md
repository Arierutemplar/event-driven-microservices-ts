# Guía de resiliencia: caída y recuperación de dependencias

Esta guía describe cómo se comporta el servicio cuando cae PostgreSQL, NATS o Redis,
cómo comprobarlo de forma reproducible y qué hacer para recuperarse. El backup y la
restauración están en [runbooks/backup-restore.md](./runbooks/backup-restore.md); los
objetivos de servicio, en [slo.md](./slo.md).

## Comportamiento por dependencia

| Dependencia caída | Ingesta `POST /api/events` | `/ready` | Qué protege los datos | Recuperación |
|---|---|---|---|---|
| **NATS** | `503` (o timeout del cliente) mientras no hay conexión | `503` | Nada se acepta sin ack de JetStream: un `202` ya está en disco | Reconexión automática del cliente NATS; los clientes deben reintentar con el mismo `id` |
| **PostgreSQL** | Sigue devolviendo `202`: JetStream acumula el backlog | `503` | Circuit breaker: el consumer deja de hacer `Fetch`, sin gastar entregas | Semiabierto tras `DB_BREAKER_OPEN_TIMEOUT`; el backlog se drena solo |
| **Redis** | Errores puntuales mientras Redis no responde | `503` | El consumer actualiza Redis **antes** del ack: si falla, el lote vuelve a la cola | Reconexión automática; la deduplicación por `tenant:event_id` evita conteo doble |

Notas de diseño:

- `/ready` falla ante cualquier dependencia caída. En Kubernetes eso retira los pods del
  Service, así que con PostgreSQL caído la ingesta también deja de recibir tráfico
  aunque JetStream podría almacenarlo. Es una decisión conservadora; si se prefiere
  seguir aceptando eventos, el probe debería depender solo de NATS.
- La reintentabilidad es responsabilidad del cliente: debe reenviar el **mismo** `id`
  ante `429`, `503` o timeout. La deduplicación (JetStream `Nats-Msg-Id`, caché,
  índice único en PostgreSQL y Redis) garantiza que no se duplique.
- La ventana máxima de caída del consumer sin pérdida es `NATS_STREAM_MAX_AGE`
  (72 h por defecto).

## Prueba reproducible: `scripts/resilience/outage.ps1`

El script ejecuta [`scripts/load/e2e.js`](../scripts/load/e2e.js) con reintentos de
cliente (`E2E_RETRY=true`), detiene la dependencia indicada durante la carga y la
vuelve a arrancar. La prueba solo pasa si **todos** los eventos aceptados aparecen en
el resumen y en el historial de PostgreSQL (`events_lost == 0`).

Requisitos: Docker, k6 y PowerShell (Windows PowerShell 5.1 o PowerShell 7).

```powershell
docker compose up -d --build
./scripts/resilience/outage.ps1 -Service nats
./scripts/resilience/outage.ps1 -Service postgres -DownSeconds 15
./scripts/resilience/outage.ps1 -Service redis
```

Parámetros: `-BaseUrl`, `-ApiKey` (por defecto `$env:API_KEY`), `-Total`,
`-DownAfterSeconds`, `-DownSeconds`, `-Container` (contenedor concreto) y
`-ProjectName` (proyecto Compose distinto del directorio actual). El código de salida
es el de k6: `0` significa que no se perdió ningún evento.

Si ya tienes otros contenedores usando los puertos por defecto, levanta un proyecto
aislado con un override que remapee `ports` (con `!override`) y pásale
`-ProjectName` y `-BaseUrl`. Sube `RATE_LIMIT_RPS` para que el rate limiter no
interfiera con la prueba.

### Resultado de referencia

Ejecución local (Docker Compose aislado, 200 eventos, caída de 8 s tras 3 s de carga):

| Servicio detenido | Eventos aceptados | Peticiones fallidas (reintentadas) | Eventos perdidos |
|---|---|---|---|
| NATS | 200/200 | 10 de 215 | 0 |
| PostgreSQL | 200/200 | 0 de 222 | 0 |
| Redis | 200/200 | 5 de 210 | 0 |

Son resultados de un equipo de desarrollo con un solo nodo por dependencia: demuestran
la ausencia de pérdida ante caídas transitorias, no la capacidad ni la alta
disponibilidad de producción.

## Recuperación manual

1. Identifica la dependencia con `/ready` (el cuerpo indica cuál falla) y las alertas
   `AnalyticsCircuitBreakerOpen`, `AnalyticsNATSBacklog` o `AnalyticsHighHTTPErrorRate`.
2. Restaura la dependencia; no reinicies el backend antes: reconecta solo.
3. Vigila que `analytics_nats_pending_messages` vuelva a 0 y que
   `analytics_circuit_breaker_state` vuelva a 0 (cerrado).
4. Revisa la DLQ (`/api/dlq` o `dlqctl`) por si algún lote superó `NATS_MAX_DELIVER`.
5. Ejecuta `scripts/load/e2e.js` con `PROFILE=smoke` para confirmar el flujo completo.

Runbooks específicos: [database](./runbooks/database.md), [nats](./runbooks/nats.md),
[dlq](./runbooks/dlq.md), [export](./runbooks/export.md).

## Fuera del alcance local

- Caída de un nodo en un clúster NATS de 3 réplicas y failover de PostgreSQL/Redis
  gestionados: requieren infraestructura real.
- Particiones de red, latencia inyectada y caos a nivel de nodo de Kubernetes
  (por ejemplo, con Chaos Mesh o Litmus).
- Medición de RTO/RPO reales en el entorno objetivo.
