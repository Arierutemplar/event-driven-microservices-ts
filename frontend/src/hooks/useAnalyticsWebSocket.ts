import { useCallback, useEffect, useRef, useState } from "react";
import { parseSummaryMessage, type ConnectionStatus, type MetricSummary } from "../types/analytics";

const INITIAL_DELAY_MS = 1000;
const MAX_DELAY_MS = 30000;

export interface AnalyticsWebSocket {
  metrics: MetricSummary | null;
  status: ConnectionStatus;
  isConnected: boolean;
  /** Intentos de reconexión consecutivos desde la última conexión exitosa. */
  retries: number;
  reconnect: () => void;
}

/**
 * Mantiene una conexión WebSocket con reconexión automática (backoff exponencial
 * con jitter, tope de 30 s). Reconecta de inmediato al volver la red o la pestaña.
 * Si se pasa `resolveUrl` (p. ej. para obtener un ticket efímero), se invoca antes de cada
 * intento de conexión, de modo que cada reconexión usa credenciales nuevas.
 * Al desmontar o cambiar `url` se cancelan el temporizador, los listeners del
 * navegador y los handlers del socket, de modo que no quedan referencias vivas.
 */
export function useAnalyticsWebSocket(url: string, resolveUrl?: () => Promise<string>): AnalyticsWebSocket {
  const [metrics, setMetrics] = useState<MetricSummary | null>(null);
  const [status, setStatus] = useState<ConnectionStatus>("connecting");
  const [retries, setRetries] = useState(0);

  const reconnectNow = useRef<() => void>(() => undefined);
  const resolveRef = useRef(resolveUrl);
  resolveRef.current = resolveUrl;

  useEffect(() => {
    let disposed = false;
    let socket: WebSocket | null = null;
    let resolving = false;
    let timer: number | undefined;
    let attempt = 0;

    const detach = (ws: WebSocket) => {
      ws.onopen = null;
      ws.onmessage = null;
      ws.onerror = null;
      ws.onclose = null;
    };

    const scheduleRetry = () => {
      setStatus("disconnected");
      const base = Math.min(INITIAL_DELAY_MS * 2 ** attempt, MAX_DELAY_MS);
      attempt += 1;
      setRetries(attempt);
      timer = window.setTimeout(connect, base * (0.75 + Math.random() * 0.5));
    };

    const open = (target: string) => {
      const ws = new WebSocket(target);
      socket = ws;

      ws.onopen = () => {
        attempt = 0;
        setRetries(0);
        setStatus("connected");
      };

      ws.onmessage = (ev: MessageEvent<string>) => {
        const summary = parseSummaryMessage(ev.data);
        if (summary) setMetrics(summary);
      };

      ws.onerror = () => ws.close();

      ws.onclose = () => {
        if (socket !== ws) return; // socket obsoleto, ya reemplazado
        socket = null;
        if (disposed) return;
        scheduleRetry();
      };
    };

    function connect() {
      window.clearTimeout(timer);
      if (disposed || resolving || (socket && socket.readyState <= WebSocket.OPEN)) return;

      setStatus("connecting");
      const resolver = resolveRef.current;
      if (!resolver) {
        open(url);
        return;
      }
      resolving = true;
      resolver().then(
        (target) => {
          resolving = false;
          if (!disposed && !socket) open(target);
        },
        () => {
          resolving = false;
          if (!disposed) scheduleRetry();
        },
      );
    }

    const wake = () => {
      if (document.visibilityState === "visible" && !socket && !resolving) {
        attempt = 0;
        connect();
      }
    };

    reconnectNow.current = () => {
      attempt = 0;
      if (socket) {
        detach(socket);
        socket.close();
        socket = null;
      }
      connect();
    };

    connect();
    window.addEventListener("online", wake);
    document.addEventListener("visibilitychange", wake);

    return () => {
      disposed = true;
      window.clearTimeout(timer);
      window.removeEventListener("online", wake);
      document.removeEventListener("visibilitychange", wake);
      if (socket) {
        detach(socket);
        socket.close();
        socket = null;
      }
      reconnectNow.current = () => undefined;
    };
  }, [url]);

  const reconnect = useCallback(() => reconnectNow.current(), []);

  return { metrics, status, isConnected: status === "connected", retries, reconnect };
}
