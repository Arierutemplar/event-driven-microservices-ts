# Objetivos de nivel de servicio (SLO)

Los SLO cubren el camino crítico: aceptar un evento en `POST /api/events` y que llegue a
PostgreSQL. Son objetivos iniciales; revísalos tras 30 días de datos reales.

| SLO | Objetivo | SLI (métrica) | Ventana |
|---|---|---|---|
| Disponibilidad de ingesta | 99 % de peticiones sin `5xx` | `analytics_http_requests_total{path="/api/events"}` | 30 días |
| Latencia de ingesta | p95 < 200 ms | `analytics_http_request_duration_seconds{path="/api/events",method="POST"}` | 5 min sostenido 10 min |
| Pérdida de eventos | < 0,1 % de los publicados sin explicar | Publicados − consumidos − DLQ − duplicados − Δbacklog | 30 min |

- Los `4xx` (validación, autenticación) y `429` (rate limit) no consumen presupuesto:
  son decisiones correctas del servicio. Los `503` sí (dependencia caída, cola llena).
- Un `202` significa que JetStream confirmó el evento en disco. La llegada a PostgreSQL
  se mide con el SLO de pérdida y se verifica con `scripts/load/e2e.js`.
- El presupuesto de error mensual es del 1 %: ~7 h 18 min de indisponibilidad total.

## Reglas en Prometheus

[`prometheus-rules.yml`](../prometheus-rules.yml) contiene reglas de grabación
(`analytics:*`) y alertas. [`prometheus-rules.test.yml`](../prometheus-rules.test.yml)
las prueba con `promtool test rules` (también en CI).

| Alerta | Condición | Severidad |
|---|---|---|
| `AnalyticsAvailabilityFastBurn` | Error > 14,4 % en 1 h **y** en 5 min (2 % del presupuesto en 1 h) | critical |
| `AnalyticsAvailabilitySlowBurn` | Error > 6 % en 6 h **y** en 30 min (5 % del presupuesto en 6 h) | warning |
| `AnalyticsIngestLatencyHigh` | p95 > 200 ms durante 10 min | warning |
| `AnalyticsEventLossSuspected` | Pérdida > 0,1 % durante 15 min con > 100 eventos publicados | critical |
| `AnalyticsDLQPublishFailing` | Fallos al publicar en la DLQ | critical |
| `AnalyticsTargetDown` | Prometheus no puede hacer scrape de una réplica | critical |

Las alertas operativas (`AnalyticsNATSBacklog`, `AnalyticsCircuitBreakerOpen`,
`AnalyticsDLQBacklog`, `AnalyticsExporterStalled`, `AnalyticsHighHTTPErrorRate`)
enlazan a sus runbooks en [`runbooks/`](./runbooks/).

### Cómo se calcula la pérdida

```
sin_explicar = Δpublicados − Δconsumidos − ΔDLQ − Δduplicados_consumer
               − Δpending − Δack_pending
```

- `analytics_nats_published_total` no cuenta duplicados detectados por JetStream.
- Un reenvío fuera de la ventana de deduplicación (2 min) se publica dos veces y se
  descarta en el consumer (`analytics_dedup_dropped_total{stage="consumer"}`).
- Si PostgreSQL cae, el backlog crece y explica la diferencia: no se considera pérdida
  (lo vigila `AnalyticsNATSBacklog`).
- Es un indicador agregado: detecta pérdidas sistemáticas, no un evento concreto. Para
  verificación exacta usa `scripts/load/e2e.js` o compara IDs con PostgreSQL.

## Alertmanager: conectar el notificador

En local, `docker compose --profile alerting up -d` arranca Alertmanager con
[`alertmanager.example.yml`](../alertmanager.example.yml) en <http://localhost:9093>.
Prometheus ya apunta a `alertmanager:9093`. Sin integraciones, las alertas se ven en la
UI pero no se envían.

En producción:

1. Usa un Alertmanager gestionado o desplegado en HA (≥ 2 réplicas con `--cluster.*`),
   o el del stack de monitorización del clúster (kube-prometheus-stack).
2. Copia `alertmanager.example.yml`, descomenta la integración (PagerDuty/Opsgenie
   para `critical`, Slack/email para `warning`) y referencia los secretos con
   `*_file`, montados desde un Secret, Vault Agent o ESO.
3. Con Prometheus Operator, carga las reglas como `PrometheusRule` y la configuración
   como `AlertmanagerConfig`; sin él, ajusta `alerting.alertmanagers` en
   `prometheus.yml` al Service real.
4. Prueba el circuito completo con una alerta sintética:

   ```bash
   amtool alert add AnalyticsSyntheticTest severity=critical slo=test \
     --annotation=summary="Prueba de guardia" --alertmanager.url=http://alertmanager:9093
   ```

   Comprueba la recepción, el acknowledgement, el escalado y la notificación de resolución.
5. Añade una alerta *dead man's switch* (`vector(1)`) hacia un servicio externo
   para detectar que Prometheus o Alertmanager han dejado de funcionar.

El diseño no se considera cerrado hasta que el canal de guardia haya recibido una
alerta real de prueba.
