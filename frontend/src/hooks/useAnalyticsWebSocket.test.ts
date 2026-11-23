import { act, renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { useAnalyticsWebSocket } from "./useAnalyticsWebSocket";

class FakeSocket {
  static instances: FakeSocket[] = [];
  static OPEN = 1;
  readyState = 0;
  onopen: (() => void) | null = null;
  onmessage: ((ev: { data: string }) => void) | null = null;
  onerror: (() => void) | null = null;
  onclose: (() => void) | null = null;
  constructor(public url: string) {
    FakeSocket.instances.push(this);
  }
  close() {
    this.readyState = 3;
    this.onclose?.();
  }
}

beforeEach(() => {
  FakeSocket.instances = [];
  vi.stubGlobal("WebSocket", FakeSocket);
});
afterEach(() => vi.unstubAllGlobals());

describe("useAnalyticsWebSocket", () => {
  it("resuelve una URL nueva (ticket) en cada conexión y recibe mensajes", async () => {
    let n = 0;
    const resolveUrl = vi.fn(async () => `ws://h/ws?ticket=t${++n}`);
    const { result } = renderHook(() => useAnalyticsWebSocket("ws://h/ws", resolveUrl));

    await waitFor(() => expect(FakeSocket.instances).toHaveLength(1));
    expect(FakeSocket.instances[0].url).toBe("ws://h/ws?ticket=t1");

    act(() => {
      FakeSocket.instances[0].onopen?.();
      FakeSocket.instances[0].onmessage?.({ data: JSON.stringify({ total_events: 7, events_by_type: null, last_event: null }) });
    });
    expect(result.current.isConnected).toBe(true);
    expect(result.current.metrics?.total_events).toBe(7);

    act(() => result.current.reconnect());
    await waitFor(() => expect(FakeSocket.instances).toHaveLength(2));
    expect(FakeSocket.instances[1].url).toBe("ws://h/ws?ticket=t2");
  });

  it("marca desconexión si no puede obtener el ticket", async () => {
    const { result } = renderHook(() => useAnalyticsWebSocket("ws://h/ws", () => Promise.reject(new Error("401"))));
    await waitFor(() => expect(result.current.status).toBe("disconnected"));
    expect(FakeSocket.instances).toHaveLength(0);
    expect(result.current.retries).toBe(1);
  });
});
