import { describe, it, expect, vi, afterEach } from "vitest";
import { render, screen, act } from "@testing-library/react";
import {
  CRDTProvider,
  useDocument,
  useCollection,
  useList,
  useSet,
  useNestedDocument,
} from "../react.js";
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

function TodoListView() {
  const { items, insert } = useList<string>("docs", "doc-1", "todos");
  return (
    <div>
      <span data-testid="items">{items.join(",")}</span>
      <button onClick={() => insert("x")}>add</button>
    </div>
  );
}

function SetView() {
  const { elements } = useSet<string>("docs", "doc-1", "tags");
  return <span data-testid="tags">{elements.join(",")}</span>;
}

function NestedView() {
  const { data } = useNestedDocument<{ author?: string }>("docs", "doc-1", "meta");
  return <span data-testid="author">{data.author ?? "none"}</span>;
}

describe("all hooks render and update without thrash", () => {
  it("useList renders and updates on insert", async () => {
    const err = vi.spyOn(console, "error").mockImplementation(() => {});
    render(
      <CRDTProvider config={config}>
        <TodoListView />
      </CRDTProvider>
    );
    expect(screen.getByTestId("items").textContent).toBe("");
    await act(async () => {
      screen.getByText("add").click();
    });
    expect(screen.getByTestId("items").textContent).toBe("x");
    expect(err.mock.calls.some((c) =>
      String(c[0]).includes("getSnapshot should be cached"))).toBe(false);
  });

  it("useSet and useNestedDocument render without warnings", () => {
    const err = vi.spyOn(console, "error").mockImplementation(() => {});
    render(
      <CRDTProvider config={config}>
        <SetView />
        <NestedView />
      </CRDTProvider>
    );
    expect(screen.getByTestId("tags").textContent).toBe("");
    expect(screen.getByTestId("author").textContent).toBe("none");
    expect(err.mock.calls.some((c) =>
      String(c[0]).includes("getSnapshot should be cached"))).toBe(false);
  });
});

describe("shared empty snapshots are frozen", () => {
  it("mutating an empty useSet result throws instead of corrupting the shared singleton", () => {
    // useSet's getSnapshot returns the shared EMPTY_ARRAY singleton
    // directly (not a copy) whenever the field is absent, so this
    // exercises the actual runtime path, not just the SSR fallback.
    let captured: unknown[] | null = null;
    function CaptureView() {
      const { elements } = useSet<string>("docs", "doc-1", "tags");
      captured = elements;
      return null;
    }
    render(
      <CRDTProvider config={config}>
        <CaptureView />
      </CRDTProvider>
    );
    expect(captured).toEqual([]);
    expect(() => (captured as unknown[]).push("x")).toThrow(TypeError);
  });
});
