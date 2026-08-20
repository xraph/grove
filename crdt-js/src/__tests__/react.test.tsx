import { describe, it, expect, vi, afterEach } from "vitest";
import { render, screen, act } from "@testing-library/react";
import { CRDTProvider, useDocument, useCollection } from "../react.js";
import type { Transport, PullResponse, PushResponse } from "../types.js";

const HLC0 = { ts: "0", c: 0, node: "" };

/** Transport that never returns data — isolates hooks from the network. */
const inertTransport: Transport = {
  async pull(): Promise<PullResponse> {
    return { changes: [], latest_hlc: HLC0 };
  },
  async push(): Promise<PushResponse> {
    return { merged: 0, latest_hlc: HLC0 };
  },
};

const config = {
  nodeID: "test-node",
  tables: ["docs"],
  transport: inertTransport,
  streaming: false as const,
  autoSync: false as const,
};

function DocView() {
  const { data, update } = useDocument<{ title?: string }>("docs", "doc-1");
  return (
    <div>
      <span data-testid="title">{data?.title ?? "empty"}</span>
      <button onClick={() => update("title", "hi")}>update</button>
    </div>
  );
}

function ListView() {
  const { items } = useCollection<{ title?: string }>("docs");
  return <span data-testid="count">{items.length}</span>;
}

describe("React hooks render without snapshot thrash", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("useDocument renders without a getSnapshot loop warning", async () => {
    const err = vi.spyOn(console, "error").mockImplementation(() => {});
    render(
      <CRDTProvider config={config}>
        <DocView />
      </CRDTProvider>
    );
    expect(screen.getByTestId("title").textContent).toBe("empty");
    await act(async () => {
      screen.getByRole("button").click();
    });
    expect(screen.getByTestId("title").textContent).toBe("hi");
    const warned = err.mock.calls.some((c) =>
      String(c[0]).includes("getSnapshot should be cached")
    );
    expect(warned).toBe(false);
  });

  it("useCollection renders without a getSnapshot loop warning", () => {
    const err = vi.spyOn(console, "error").mockImplementation(() => {});
    render(
      <CRDTProvider config={config}>
        <ListView />
      </CRDTProvider>
    );
    expect(screen.getByTestId("count").textContent).toBe("0");
    const warned = err.mock.calls.some((c) =>
      String(c[0]).includes("getSnapshot should be cached")
    );
    expect(warned).toBe(false);
  });
});
