import { describe, expect, it } from "vitest";
import { parseSummaryMessage } from "./analytics";

describe("parseSummaryMessage", () => {
  it("acepta un resumen válido", () => {
    const s = parseSummaryMessage(JSON.stringify({ total_events: 3, events_by_type: { a: 3 }, last_event: null }));
    expect(s?.total_events).toBe(3);
  });

  it("rechaza JSON roto y formas inesperadas", () => {
    expect(parseSummaryMessage("{no json")).toBeNull();
    expect(parseSummaryMessage(JSON.stringify({ total_events: "3" }))).toBeNull();
    expect(parseSummaryMessage(JSON.stringify({ total_events: 1, events_by_type: [] }))).toBeNull();
  });
});
