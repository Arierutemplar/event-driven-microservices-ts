import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import ConnectionBadge from "./ConnectionBadge";

describe("ConnectionBadge", () => {
  it("muestra el estado conectado sin botón de reconexión", () => {
    render(<ConnectionBadge status="connected" retries={0} onReconnect={() => undefined} />);
    expect(screen.getByRole("status").textContent).toContain("Conectado");
    expect(screen.queryByText("Reconectar ahora")).toBeNull();
  });

  it("permite reconectar manualmente cuando está desconectado", () => {
    const onReconnect = vi.fn();
    render(<ConnectionBadge status="disconnected" retries={3} onReconnect={onReconnect} />);
    expect(screen.getByRole("status").textContent).toContain("reintento 3");
    fireEvent.click(screen.getByText("Reconectar ahora"));
    expect(onReconnect).toHaveBeenCalledOnce();
  });
});
