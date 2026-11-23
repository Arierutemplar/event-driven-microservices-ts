import { useAnalyticsWebSocket } from "./useAnalyticsWebSocket";
import { useEventFeed, useInitialSummary, useThroughput } from "./useEventFeed";
import type { ConnectionStatus, FeedItem, MetricSummary } from "../types/analytics";

export interface AnalyticsStream {
  /** Resumen en vivo si ya llegó un mensaje; si no, el cargado por REST al montar. */
  metrics: MetricSummary | null;
  /** Último resumen recibido por WebSocket (cambia de identidad con cada mensaje). */
  live: MetricSummary | null;
  status: ConnectionStatus;
  retries: number;
  reconnect: () => void;
  feed: FeedItem[];
  /** Eventos por segundo de los últimos 60 s. */
  throughput: number[];
  /** Media de eventos por segundo de los últimos 5 s. */
  eps: number;
  total: number;
}

/** Une conexión en tiempo real, resumen inicial, feed y rendimiento en una sola fuente de datos. */
export function useAnalyticsStream(url: string, resolveUrl?: () => Promise<string>): AnalyticsStream {
  const { metrics: live, status, retries, reconnect } = useAnalyticsWebSocket(url, resolveUrl);
  const initial = useInitialSummary();

  const metrics = live ?? initial;
  const total = metrics?.total_events ?? 0;
  const feed = useEventFeed(live);
  const throughput = useThroughput(total);
  const eps = throughput.slice(-5).reduce((a, b) => a + b, 0) / 5;

  return { metrics, live, status, retries, reconnect, feed, throughput, eps, total };
}
