/** Añade `?key=value` a una URL absoluta. */
export function withQuery(raw: string, key: string, value: string): string {
  const u = new URL(raw);
  u.searchParams.set(key, value);
  return u.toString();
}

/**
 * Pide un ticket efímero (POST) y devuelve la URL del WebSocket con `?ticket=`.
 * `ticketEndpoint` debería ser un endpoint del mismo origen que autentique la sesión del usuario
 * (BFF/ingress que inyecta la API key del tenant): así ninguna clave queda en el bundle.
 */
export async function fetchTicketUrl(
  wsUrl: string,
  ticketEndpoint: string,
  headers: Record<string, string> = {},
  fetchImpl: typeof fetch = fetch,
): Promise<string> {
  const res = await fetchImpl(ticketEndpoint, { method: "POST", headers, credentials: "include" });
  if (!res.ok) throw new Error(`ws-ticket: HTTP ${res.status}`);
  const body: unknown = await res.json();
  const ticket = typeof body === "object" && body !== null ? (body as { ticket?: unknown }).ticket : undefined;
  if (typeof ticket !== "string" || ticket === "") throw new Error("ws-ticket: respuesta inválida");
  return withQuery(wsUrl, "ticket", ticket);
}
