import { describe, expect, it, vi } from "vitest";
import { fetchTicketUrl, withQuery } from "./wsUrl";

describe("wsUrl", () => {
  it("añade el parámetro sin perder los existentes", () => {
    expect(withQuery("wss://h/ws?x=1", "ticket", "abc")).toBe("wss://h/ws?x=1&ticket=abc");
  });

  it("pide un ticket por POST y lo incorpora a la URL", async () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, json: async () => ({ ticket: "t.sig" }) });
    const url = await fetchTicketUrl("wss://h/ws", "/api/ws-ticket", { "X-Test": "1" }, fetchMock as unknown as typeof fetch);
    expect(url).toBe("wss://h/ws?ticket=t.sig");
    expect(fetchMock).toHaveBeenCalledWith("/api/ws-ticket", expect.objectContaining({ method: "POST" }));
  });

  it("falla si el backend rechaza o responde sin ticket", async () => {
    const denied = vi.fn().mockResolvedValue({ ok: false, status: 401 });
    await expect(fetchTicketUrl("wss://h/ws", "/t", {}, denied as unknown as typeof fetch)).rejects.toThrow("401");
    const empty = vi.fn().mockResolvedValue({ ok: true, json: async () => ({}) });
    await expect(fetchTicketUrl("wss://h/ws", "/t", {}, empty as unknown as typeof fetch)).rejects.toThrow("inválida");
  });
});
