import { WS_TICKET_URL, WS_URL } from "../config";
import { fetchTicketUrl } from "../lib/wsUrl";
import { useAnalyticsStream } from "../hooks/useAnalyticsStream";
import { useFlash } from "../hooks/useFlash";
import { useIngest } from "../hooks/useIngest";
import { useTrafficSimulator } from "../hooks/useTrafficSimulator";
import ConnectionBadge from "./ConnectionBadge";
import EventFeed from "./EventFeed";
import EventTypeChart from "./EventTypeChart";
import HealthIndicator from "./HealthIndicator";
import IngestPanel from "./IngestPanel";
import Sparkline from "./Sparkline";
import TrafficSimulator from "./TrafficSimulator";

interface DashboardProps {
  /** URL del WebSocket de analítica. */
  url?: string;
}

export default function Dashboard({ url = WS_URL }: DashboardProps) {
  const ticketURL = WS_TICKET_URL;
  const resolveUrl = ticketURL ? () => fetchTicketUrl(url, ticketURL) : undefined;
  const { metrics, live, status, retries, reconnect, feed, throughput, eps, total } = useAnalyticsStream(url, resolveUrl);
  const ingest = useIngest();
  const sim = useTrafficSimulator({ onResult: ingest.record });
  const flash = useFlash(live);

  return (
    <div className="min-h-screen bg-slate-950 p-6 text-slate-100">
      <div className="mx-auto max-w-6xl space-y-6">
        <header className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <h1 className="text-2xl font-semibold tracking-tight">Analytics en tiempo real</h1>
            <p className="text-xs text-slate-500">{url}</p>
          </div>
          <ConnectionBadge status={status} retries={retries} onReconnect={reconnect} />
        </header>

        <div className="grid gap-6 lg:grid-cols-3">
          <section
            className={`rounded-2xl border bg-slate-900 p-6 transition-colors duration-500 lg:col-span-2 ${
              flash ? "border-indigo-400" : "border-slate-800"
            }`}
          >
            <div className="flex items-start justify-between gap-4">
              <div>
                <p className="text-sm uppercase tracking-wider text-slate-400">Eventos procesados</p>
                <p className="mt-2 text-5xl font-bold tabular-nums">{total.toLocaleString()}</p>
              </div>
              <div className="text-right">
                <p className="text-sm uppercase tracking-wider text-slate-400">Eventos / s</p>
                <p className="mt-2 text-3xl font-semibold tabular-nums text-indigo-300">{eps.toFixed(1)}</p>
              </div>
            </div>
            <Sparkline values={throughput} className="mt-4 h-16 w-full" />
            <p className="mt-1 text-right text-xs text-slate-600">últimos 60 s</p>
          </section>

          <HealthIndicator dlqEvents={metrics?.dlq_events} ingest={ingest.stats} status={status} />
        </div>

        <div className="grid gap-6 lg:grid-cols-2">
          <EventTypeChart data={metrics?.events_by_type ?? null} />
          <EventFeed items={feed} />
        </div>

        <TrafficSimulator
          active={sim.active}
          progress={sim.progress}
          toast={sim.toast}
          locked={ingest.progress.running}
          onBurst={() => void sim.simulateBurst()}
          onFailure={() => void sim.simulateFailure()}
          onDismissToast={sim.dismiss}
        />

        <IngestPanel {...ingest} locked={sim.active !== null} />
      </div>
    </div>
  );
}
