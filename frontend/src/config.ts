const env = import.meta.env;

/** Añade ?token=… a la URL del WebSocket (los navegadores no pueden fijar cabeceras en `new WebSocket`). */
function withToken(raw: string, token: string | undefined): string {
  if (!token) return raw;
  const u = new URL(raw);
  u.searchParams.set("token", token);
  return u.toString();
}

/**
 * Endpoints del backend; sobreescribibles con VITE_WS_URL / VITE_API_URL / VITE_API_KEY.
 * Atención: todo VITE_* queda incrustado en el bundle y es visible para cualquiera que abra la
 * página. VITE_WS_TOKEN es un modo legado de desarrollo; en producción usa VITE_WS_TICKET_URL y
 * nunca la clave de administración.
 */
export const WS_TOKEN: string | undefined = env.VITE_WS_TOKEN || undefined;
/**
 * Endpoint (preferiblemente del mismo origen y autenticado por tu sesión/BFF) que devuelve un ticket
 * efímero ligado a un tenant (POST /api/ws-ticket). Si está definido, se ignora VITE_WS_TOKEN y el
 * bundle no contiene ningún secreto de WebSocket.
 */
export const WS_TICKET_URL: string | undefined = env.VITE_WS_TICKET_URL || undefined;
const BASE_WS_URL: string = env.VITE_WS_URL ?? "ws://localhost:8080/ws";
export const WS_URL: string = WS_TICKET_URL ? BASE_WS_URL : withToken(BASE_WS_URL, WS_TOKEN);
export const API_URL: string = (env.VITE_API_URL ?? "http://localhost:8080").replace(/\/$/, "");
export const API_KEY: string | undefined = env.VITE_API_KEY || undefined;

/** Credenciales de ingesta (POST /api/events): clave de tenant y tenant opcional. */
export const INGEST_API_KEY: string | undefined = env.VITE_INGEST_API_KEY || API_KEY;
export const TENANT_ID: string | undefined = env.VITE_TENANT_ID || undefined;

export function ingestHeaders(): Record<string, string> {
  const h: Record<string, string> = { "Content-Type": "application/json" };
  if (INGEST_API_KEY) h["X-API-Key"] = INGEST_API_KEY;
  if (TENANT_ID) h["X-Tenant-ID"] = TENANT_ID;
  return h;
}
