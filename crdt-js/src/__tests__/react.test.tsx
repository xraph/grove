import { describe, it, expect, vi, afterEach } from "vitest";
import { useEffect } from "react";
import { render, screen, act } from "@testing-library/react";
import {
  CRDTProvider,
  useCRDT,
  useDocument,
  useCollection,
  useList,
  useSet,
  useNestedDocument,
  useSyncStatus,
} from "../react.js";
import { SyncEngine } from "../sync.js";
import type {
  Transport,
  StreamTransport,
  StreamSubscription,
  StreamEventHandler,
  StreamEvent,
  PresenceEvent,
  PullResponse,
  PushResponse,
} from "../types.js";
import type { StorePlugin, PresenceHook } from "../plugin.js";

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

function SyncView() {
  const { pendingCount, sync } = useSyncStatus();
  return (
    <div>
      <span data-testid="pending">{pendingCount}</span>
      <button onClick={() => void sync()}>sync</button>
    </div>
  );
}

describe("sync integration", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("pushes pending changes through SyncEngine and clears them", async () => {
    const pushed: number[] = [];
    const transport: Transport = {
      async pull() { return { changes: [], latest_hlc: HLC0 }; },
      async push(req) {
        pushed.push(req.changes.length);
        return { merged: req.changes.length, latest_hlc: HLC0 };
      },
    };

    // A no-op hook (one that never calls engine.sync()) would still leave
    // `pushed` empty and pendingCount at 0 here, so the discriminating
    // assertion is the spy: it only fires if useCRDT actually delegates to
    // SyncEngine rather than running (or skipping) its own inline cycle.
    const syncSpy = vi.spyOn(SyncEngine.prototype, "sync");

    render(
      <CRDTProvider config={{ ...config, transport }}>
        <DocView />
        <SyncView />
      </CRDTProvider>
    );
    expect(screen.getByTestId("pending").textContent).toBe("0");

    // Create a real pending change before syncing, so a push actually has
    // something to carry.
    await act(async () => {
      screen.getByRole("button", { name: "update" }).click();
    });
    expect(screen.getByTestId("pending").textContent).toBe("1");

    await act(async () => {
      screen.getByText("sync").click();
    });

    expect(syncSpy).toHaveBeenCalledTimes(1);
    expect(pushed).toEqual([1]);
    expect(screen.getByTestId("pending").textContent).toBe("0");
  });
});

/** A StreamSubscription whose test can fire events into the handler the
 *  hook registered, without a real SSE connection. */
function makeControllableStream(): StreamSubscription & {
  emit(event: StreamEvent): void;
} {
  let handler: StreamEventHandler | null = null;
  return {
    on(h) {
      handler = h;
      return () => {
        handler = null;
      };
    },
    connect() {},
    disconnect() {},
    connected: true,
    lastHLC: null,
    emit(event) {
      handler?.(event);
    },
  };
}

describe("presence rerouting", () => {
  it("routes inbound presence stream events through client.applyPresenceEvent and the plugin chain", () => {
    const fakeStream = makeControllableStream();
    const streamingTransport: StreamTransport = {
      ...inertTransport,
      subscribe() {
        return fakeStream;
      },
    };

    const onPresenceEvent = vi.fn();
    const plugin: StorePlugin & PresenceHook = {
      name: "presence-probe",
      onPresenceEvent,
    };

    let client!: ReturnType<typeof useCRDT>["client"];
    function Probe() {
      const result = useCRDT({
        ...config,
        transport: streamingTransport,
        streaming: true,
      });
      client = result.client;
      useEffect(() => {
        result.store.use(plugin);
        return () => result.store.removePlugin(plugin.name);
        // eslint-disable-next-line react-hooks/exhaustive-deps
      }, [result.store]);
      return null;
    }

    render(<Probe />);

    const event: PresenceEvent = {
      type: "join",
      node_id: "peer-1",
      topic: "room-a",
      data: { name: "Ada" },
    };
    act(() => {
      fakeStream.emit({ type: "presence", data: event });
    });

    // The hook's stream handler ran the event through the plugin chain
    // (via client.applyPresenceEvent), not just PresenceManager directly.
    expect(onPresenceEvent).toHaveBeenCalledTimes(1);
    expect(onPresenceEvent).toHaveBeenCalledWith({
      type: "join",
      nodeId: "peer-1",
      topic: "room-a",
      data: { name: "Ada" },
    });

    // ...and the event still reached PresenceManager, so state is visible.
    expect(client.presence.getPresence("room-a")).toEqual([
      { node_id: "peer-1", topic: "room-a", data: { name: "Ada" }, updated_at: expect.any(Number) },
    ]);
  });
});
