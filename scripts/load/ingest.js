import http from "k6/http";
import { check } from "k6";

const profiles = {
  smoke: { rate: 5, duration: "30s", vus: 10, maxVUs: 30, p95: 500, errors: 0.01 },
  load: { rate: 100, duration: "2m", vus: 50, maxVUs: 200, p95: 300, errors: 0.01 },
  stress: { rate: 500, duration: "3m", vus: 150, maxVUs: 500, p95: 500, errors: 0.02 },
};
const profileName = __ENV.PROFILE || "smoke";
const profile = profiles[profileName];
if (!profile) throw new Error(`Unknown PROFILE: ${profileName}`);

const rate = Number(__ENV.RATE || profile.rate);
const vus = Number(__ENV.VUS || profile.vus);
const maxVUs = Number(__ENV.MAX_VUS || profile.maxVUs);
if (!Number.isInteger(rate) || rate <= 0 || !Number.isInteger(vus) || vus <= 0 ||
    !Number.isInteger(maxVUs) || maxVUs < vus) {
  throw new Error("RATE and VUS must be positive integers; MAX_VUS must be >= VUS");
}
const baseURL = (__ENV.BASE_URL || "http://localhost:8080").replace(/\/$/, "");
const headers = { "Content-Type": "application/json" };
if (__ENV.API_KEY) headers["X-API-Key"] = __ENV.API_KEY;
if (__ENV.TENANT_ID) headers["X-Tenant-ID"] = __ENV.TENANT_ID;

export const options = {
  scenarios: {
    ingest: {
      executor: "constant-arrival-rate",
      rate,
      timeUnit: "1s",
      duration: __ENV.DURATION || profile.duration,
      preAllocatedVUs: vus,
      maxVUs,
      gracefulStop: "10s",
    },
  },
  thresholds: {
    "http_req_duration{endpoint:ingest}": [`p(95)<${Number(__ENV.P95_MS || profile.p95)}`],
    "http_req_failed{endpoint:ingest}": [`rate<${Number(__ENV.MAX_ERROR_RATE || profile.errors)}`],
    "checks{endpoint:ingest}": ["rate>0.99"],
    dropped_iterations: ["count==0"],
  },
};

export function setup() {
  const res = http.get(`${baseURL}/ready`, { timeout: "5s" });
  if (res.status !== 200) throw new Error(`readiness failed: ${res.status}`);
  return { runID: `${Date.now()}-${Math.floor(Math.random() * 1e9)}` };
}

export default function (data) {
  const res = http.post(`${baseURL}/api/events`, JSON.stringify({
    id: `load-${data.runID}-${__VU}-${__ITER}`,
    event_type: "load_test",
    user_id: `vu-${__VU}`,
    payload: { profile: profileName },
  }), { headers, tags: { endpoint: "ingest" }, timeout: "5s" });
  check(res, { "accepted": (r) => r.status === 202 }, { endpoint: "ingest" });
}
