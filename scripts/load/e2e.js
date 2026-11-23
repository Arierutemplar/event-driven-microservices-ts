// Prueba end-to-end de persistencia: no solo mide la aceptación HTTP (202), sino que comprueba
// que TODOS los eventos aceptados llegaron al almacenamiento tras pasar por la cola.
//
//   1. setup(): lee el contador del tenant (línea base) en /api/metrics.
//   2. Escenario: publica TOTAL eventos únicos con POST /api/events (shared-iterations).
//   3. teardown(): espera hasta E2E_TIMEOUT a que /api/metrics (agregado tras persistir en
//      PostgreSQL y confirmar en NATS) avance TOTAL eventos y a que /api/events/history
//      (PostgreSQL) devuelva los eventos de esta ejecución. Cada evento ausente suma a
//      `events_lost`; el umbral `events_lost == 0` hace fallar la prueba.
//
// Usa un tenant propio por ejecución (E2E_TENANT) para que el conteo no se mezcle con otro tráfico.
// Con API_KEY de administración, X-Tenant-ID elige el tenant; con una clave de tenant, usa ese tenant.
//
//   PROFILE=smoke API_KEY=... k6 run scripts/load/e2e.js
import http from "k6/http";
import { check, sleep } from "k6";
import { Counter } from "k6/metrics";

const profiles = {
  smoke: { total: 200, vus: 5, p95: 500 },
  load: { total: 5000, vus: 25, p95: 300 },
  stress: { total: 20000, vus: 100, p95: 500 },
};
const profileName = __ENV.PROFILE || "smoke";
const profile = profiles[profileName];
if (!profile) throw new Error(`Unknown PROFILE: ${profileName}`);

const total = Number(__ENV.TOTAL || profile.total);
const vus = Number(__ENV.VUS || profile.vus);
const timeoutS = Number(__ENV.E2E_TIMEOUT || 90);
if (!Number.isInteger(total) || total <= 0 || !Number.isInteger(vus) || vus <= 0) {
  throw new Error("TOTAL and VUS must be positive integers");
}
const baseURL = (__ENV.BASE_URL || "http://localhost:8080").replace(/\/$/, "");
// Cada VU y setup/teardown tienen su propio contexto de inicialización: el identificador de la
// ejecución se genera en setup() y se propaga por `data`, nunca a nivel de módulo.
function makeHeaders(tenant) {
  const h = { "Content-Type": "application/json", "X-Tenant-ID": tenant };
  if (__ENV.API_KEY) h["X-API-Key"] = __ENV.API_KEY;
  return h;
}

const eventsLost = new Counter("events_lost");
const retry = __ENV.E2E_RETRY === "true";
const retryMax = Number(__ENV.E2E_RETRY_MAX || 60);
const pace = Number(__ENV.E2E_PACE || 0); // segundos entre eventos por VU (alarga la prueba)

export const options = {
  scenarios: {
    publish: {
      executor: "shared-iterations",
      vus,
      iterations: total,
      maxDuration: __ENV.MAX_DURATION || "10m",
    },
  },
  thresholds: {
    "http_req_duration{endpoint:ingest}": [`p(95)<${Number(__ENV.P95_MS || profile.p95)}`],
    "checks{endpoint:ingest}": ["rate>0.999"],
    events_lost: ["count==0"],
  },
  teardownTimeout: `${timeoutS + 30}s`,
};

function summaryTotal(headers) {
  const res = http.get(`${baseURL}/api/metrics`, { headers, tags: { endpoint: "verify" } });
  if (res.status !== 200) throw new Error(`/api/metrics -> ${res.status}`);
  return res.json("total_events");
}

export function setup() {
  const ready = http.get(`${baseURL}/ready`, { timeout: "5s" });
  if (ready.status !== 200) throw new Error(`readiness failed: ${ready.status}`);
  const runID = __ENV.RUN_ID || `${Date.now()}-${Math.floor(Math.random() * 1e6)}`;
  const tenant = __ENV.E2E_TENANT || `k6-e2e-${runID}`;
  return { runID, tenant, baseline: summaryTotal(makeHeaders(tenant)) };
}

export default function (data) {
  const { runID } = data;
  const headers = makeHeaders(data.tenant);
  const id = `e2e-${runID}-${__VU}-${__ITER}`;
  const body = JSON.stringify({
    id,
    event_type: "e2e_check",
    user_id: `vu-${__VU}`,
    payload: { profile: profileName },
  });
  // E2E_RETRY=true simula a un cliente real durante una caída de dependencias: reintenta el mismo id
  // (la deduplicación evita duplicados) y solo cuenta el resultado final.
  let res = http.post(`${baseURL}/api/events`, body, { headers, tags: { endpoint: "ingest" }, timeout: "5s" });
  for (let i = 0; retry && res.status !== 202 && i < retryMax; i++) {
    sleep(1);
    res = http.post(`${baseURL}/api/events`, body, { headers, tags: { endpoint: "retry" }, timeout: "5s" });
  }
  check(res, { accepted: (r) => r.status === 202 }, { endpoint: "ingest" });
  if (pace > 0) sleep(pace);
}

export function teardown(data) {
  const { runID, tenant } = data;
  const headers = makeHeaders(tenant);
  const deadline = Date.now() + timeoutS * 1000;
  let persisted = 0;
  while (Date.now() < deadline) {
    persisted = summaryTotal(headers) - data.baseline;
    if (__ENV.E2E_DEBUG) console.log(`persisted=${persisted} baseline=${data.baseline}`);
    if (persisted >= total) break;
    sleep(1);
  }
  const missing = Math.max(0, total - persisted);
  if (missing > 0) {
    console.error(`E2E: solo ${persisted}/${total} eventos agregados tras ${timeoutS}s (tenant ${tenant})`);
    eventsLost.add(missing);
    return;
  }

  // PostgreSQL: el historial del tenant debe contener eventos de esta ejecución.
  const want = Math.min(total, 1000);
  const hist = http.get(`${baseURL}/api/events/history?limit=${want}`, { headers, tags: { endpoint: "verify" } });
  const events = hist.status === 200 ? hist.json() : [];
  const ours = events.filter((e) => typeof e.id === "string" && e.id.startsWith(`e2e-${runID}-`)).length;
  if (ours < want) {
    console.error(`E2E: historial con ${ours}/${want} eventos de esta ejecución (¿persistencia desactivada?)`);
    eventsLost.add(want - ours);
  }
}
