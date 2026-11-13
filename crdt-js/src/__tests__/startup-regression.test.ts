import { expect, it, vi } from "vitest";
import { CRDTStore } from "../store.js";
import { HybridClock } from "../hlc.js";
import type { ChangeRecord, DocumentState, StorageAdapter } from "../types.js";

it("hydration merges early streams before any persistence and notifies document subscribers", async () => {
  let load!: (state: Map<string, Map<string, DocumentState>>) => void;
  const saves: DocumentState[] = [];
  const storage: StorageAdapter = {
    loadState: () =>
      new Promise((resolve) => {
        load = resolve;
      }),
    loadPendingChanges: async () => [],
    saveDocument: async (_table, _pk, doc) => {
      saves.push(doc);
    },
    savePendingChanges: async () => {},
    deleteDocument: async () => {},
  };
  const clock = new HybridClock("me", { nowFn: () => 1000 });
  const store = new CRDTStore("me", clock, storage, { persistDebounceMs: 0 });
  const listener = vi.fn();
  store.subscribeDocument("docs", "1", listener);
  const hlc = { ts: "100000000000", c: 7, node: "me" };
  store.applyChanges([
    {
      table: "docs",
      pk: "1",
      field: "title",
      crdt_type: "lww",
      node_id: "peer",
      hlc: { ts: "1001000000", c: 0, node: "peer" },
      value: "new title",
    },
  ]);
  expect(saves).toHaveLength(0);
  listener.mockClear();
  load(
    new Map([
      [
        "docs",
        new Map([
          [
            "1",
            {
              table: "docs",
              pk: "1",
              tombstone: false,
              fields: {
                title: {
                  type: "lww",
                  hlc: { ts: "1000000000", c: 0, node: "me" },
                  node_id: "me",
                  value: "old title",
                },
                body: { type: "lww", hlc, node_id: "me", value: "saved body" },
              },
            },
          ],
        ]),
      ],
    ]),
  );
  await store.ready;
  expect(store.getDocument("docs", "1")).toMatchObject({
    title: "new title",
    body: "saved body",
  });
  expect(saves).toHaveLength(1);
  expect(saves[0].fields.body.value).toBe("saved body");
  expect(listener).toHaveBeenCalled();
  // Persisted clocks are trusted even after wall time moves backward by > max drift.
  store.setField("docs", "1", "body", "after restart");
  expect(store.getDocument("docs", "1")?.body).toBe("after restart");
});

it("empty exact remove tags survive JSON while omitted legacy tags retain their meaning", async () => {
  for (const exact of [false, true]) {
    const store = new CRDTStore(
      "me",
      new HybridClock("me", { nowFn: () => 1000 }),
    );
    await store.ready;
    store.addToSet("docs", "1", "tags", ["x"]);
    const remove: ChangeRecord = {
      table: "docs",
      pk: "1",
      field: "tags",
      crdt_type: "set",
      node_id: "peer",
      hlc: { ts: "1001000000", c: 0, node: "peer" },
      set_op: { op: "remove", elements: ["x"], ...(exact ? { tags: [] } : {}) },
    };
    store.applyChanges([JSON.parse(JSON.stringify(remove))]);
    expect(store.getDocument("docs", "1")?.tags).toEqual(exact ? ["x"] : []);
  }
});
