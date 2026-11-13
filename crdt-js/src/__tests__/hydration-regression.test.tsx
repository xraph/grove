import { act, render } from "@testing-library/react";
import { expect, it } from "vitest";
import { CRDTProvider, useCRDTContext } from "../react.js";
import type { CRDTContextValue } from "../react.js";
import type {
  StorageAdapter,
  DocumentState,
  StreamTransport,
  StreamEventHandler,
} from "../types.js";

it("regression: stream during hydration preserves persisted fields", async () => {
  let resolve!: (value: Map<string, Map<string, DocumentState>>) => void;
  const storage: StorageAdapter = {
    loadState: () =>
      new Promise((r) => {
        resolve = r;
      }),
    loadPendingChanges: async () => [],
    saveDocument: async () => {},
    deleteDocument: async () => {},
    savePendingChanges: async () => {},
  };
  const handlers = new Set<StreamEventHandler>();
  const tr: StreamTransport = {
    pull: async () => ({
      changes: [],
      latest_hlc: { ts: "0", c: 0, node: "" },
    }),
    push: async () => ({ merged: 0, latest_hlc: { ts: "0", c: 0, node: "" } }),
    subscribe: () => ({
      connected: true,
      lastHLC: null,
      on: (h) => {
        handlers.add(h);
        return () => {
          handlers.delete(h);
        };
      },
      connect() {},
      disconnect() {},
    }),
  };
  let ctx!: CRDTContextValue;
  function Probe() {
    ctx = useCRDTContext();
    return null;
  }
  const view = render(
    <CRDTProvider
      config={{ nodeID: "me", transport: tr, storage, autoSync: false }}
    >
      <Probe />
    </CRDTProvider>,
  );
  try {
    await act(async () => {
      for (const h of handlers)
        h({
          type: "change",
          data: {
            table: "docs",
            pk: "1",
            field: "title",
            crdt_type: "lww",
            node_id: "peer",
            hlc: { ts: "200", c: 0, node: "peer" },
            value: "new title",
          },
        });
      resolve(
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
                      node_id: "peer",
                      hlc: { ts: "100", c: 0, node: "peer" },
                      value: "old title",
                    },
                    body: {
                      type: "lww",
                      node_id: "peer",
                      hlc: { ts: "100", c: 0, node: "peer" },
                      value: "persisted body",
                    },
                  },
                },
              ],
            ]),
          ],
        ]),
      );
      await ctx.store.ready;
    });
    expect(ctx.store.getDocument("docs", "1")?.body).toBe("persisted body");
  } finally {
    view.unmount();
  }
  expect(handlers.size).toBe(0);
});
