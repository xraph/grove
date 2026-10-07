import { describe, it, expect } from "vitest";
import { SyncEngine } from "../sync.js";
import { CRDTClient } from "../client.js";
import { CRDTStore } from "../store.js";
import type { Transport, ChangeRecord } from "../types.js";

// Undo and redo emit compensating changes, so an undo survives a sync and
// every replica ends up showing the same thing. (Go parity: the server
// only knows about changes it is sent.)

const HLC0 = { ts: "0", c: 0, node: "" };

function relay(): Transport {
  const log: ChangeRecord[] = [];
  return {
    async pull() { return { changes: [...log], latest_hlc: HLC0 }; },
    async push(req) {
      log.push(...req.changes);
      return { merged: req.changes.length, latest_hlc: HLC0 };
    },
  };
}

function pair() {
  const t = relay();
  const mk = (node: string) => {
    const client = new CRDTClient({ nodeID: node, transport: t });
    const store = new CRDTStore(node, client.clock, undefined, { persistDebounceMs: 0 });
    return { store, engine: new SyncEngine(client, store) };
  };
  const a = mk("a");
  const b = mk("b");
  const syncAll = async () => {
    await a.engine.sync();
    await b.engine.sync();
    await a.engine.sync();
  };
  return { a: a.store, b: b.store, syncAll };
}

const doc = (s: CRDTStore) => s.getDocument<Record<string, unknown>>("t", "1");

describe("undo survives sync", () => {
  it("lww", async () => {
    const { a, b, syncAll } = pair();
    a.setField("t", "1", "title", "v1");
    await syncAll();
    a.setField("t", "1", "title", "v2");
    await syncAll();

    expect(a.undo()).toBe(true);
    expect(a.pendingCount).toBe(1);
    await syncAll();
    expect(doc(a)?.title).toBe("v1");
    expect(doc(b)?.title).toBe("v1");
  });

  it("counter", async () => {
    const { a, b, syncAll } = pair();
    a.incrementCounter("t", "1", "n", 5);
    await syncAll();
    b.incrementCounter("t", "1", "n", 2);
    a.incrementCounter("t", "1", "n", 3);
    await syncAll();

    expect(a.undo()).toBe(true);
    await syncAll();
    expect(doc(a)?.n).toBe(7);
    expect(doc(b)?.n).toBe(7);
  });

  it("set add and remove", async () => {
    const { a, b, syncAll } = pair();
    a.addToSet("t", "1", "tags", ["x"]);
    await syncAll();
    a.addToSet("t", "1", "tags", ["x", "y"]);
    await syncAll();
    a.undo(); // the second add: y goes, x stays because the first add still holds it
    await syncAll();
    expect(doc(a)?.tags).toEqual(["x"]);
    expect(doc(b)?.tags).toEqual(["x"]);

    a.removeFromSet("t", "1", "tags", ["x"]);
    await syncAll();
    a.undo();
    await syncAll();
    expect(doc(a)?.tags).toEqual(["x"]);
    expect(doc(b)?.tags).toEqual(["x"]);
  });

  it("list insert and delete", async () => {
    const { a, b, syncAll } = pair();
    const first = a.insertIntoList("t", "1", "items", "a");
    a.insertIntoList("t", "1", "items", "c", first.hlc);
    a.insertIntoList("t", "1", "items", "b", first.hlc);
    await syncAll();
    expect(doc(b)?.items).toEqual(["a", "b", "c"]);

    a.undo(); // the insert of "b"
    await syncAll();
    expect(doc(a)?.items).toEqual(["a", "c"]);
    expect(doc(b)?.items).toEqual(["a", "c"]);

    a.deleteFromList("t", "1", "items", first.hlc);
    await syncAll();
    a.undo(); // "a" comes back where it was
    await syncAll();
    expect(doc(a)?.items).toEqual(["a", "c"]);
    expect(doc(b)?.items).toEqual(["a", "c"]);
  });

  it("text insert, delete and format", async () => {
    const { a, b, syncAll } = pair();
    a.insertText("t", "1", "body", 0, "hello world");
    await syncAll();
    a.deleteText("t", "1", "body", 5, 6);
    a.formatText("t", "1", "body", 0, 5, { bold: true });
    await syncAll();

    a.undo(); // format
    a.undo(); // delete
    await syncAll();
    for (const s of [a, b]) {
      expect(s.getText("t", "1", "body")).toBe("hello world");
      expect(s.getTextDelta("t", "1", "body")).toEqual([{ insert: "hello world" }]);
    }
  });

  it("text insert undo removes only what it inserted, beside a concurrent edit", async () => {
    const { a, b, syncAll } = pair();
    a.insertText("t", "1", "body", 0, "abc");
    await syncAll();
    b.insertText("t", "1", "body", 0, "X");
    await syncAll();

    a.undo(); // the "abc" insert
    await syncAll();
    expect(a.getText("t", "1", "body")).toBe("X");
    expect(b.getText("t", "1", "body")).toBe("X");
  });

  it("document path", async () => {
    const { a, b, syncAll } = pair();
    a.setDocumentField("t", "1", "meta", "author.name", "ann");
    await syncAll();
    a.setDocumentField("t", "1", "meta", "author.name", "bob");
    a.setDocumentField("t", "1", "meta", "author.age", 30);
    await syncAll();

    a.undo(); // age set: the path did not exist before
    a.undo(); // name back to ann
    await syncAll();
    expect(doc(a)?.meta).toEqual(doc(b)?.meta);
    expect(JSON.stringify(doc(a)?.meta)).toContain("ann");
    expect(JSON.stringify(doc(a)?.meta)).not.toContain("30");
  });

  it("redo re-applies after a sync", async () => {
    const { a, b, syncAll } = pair();
    a.setField("t", "1", "title", "v1");
    a.setField("t", "1", "title", "v2");
    await syncAll();
    a.undo();
    await syncAll();
    expect(a.redo()).toBe(true);
    await syncAll();
    expect(doc(a)?.title).toBe("v2");
    expect(doc(b)?.title).toBe("v2");
    expect(a.undo()).toBe(true);
    await syncAll();
    expect(doc(b)?.title).toBe("v1");
  });
});

describe("undo of a record delete", () => {
  it("drops the tombstone while it is still pending", async () => {
    const { a, b, syncAll } = pair();
    a.setField("t", "1", "title", "kept");
    await syncAll();
    a.deleteDocument("t", "1");
    expect(a.undo()).toBe(true);
    expect(a.pendingCount).toBe(0);
    await syncAll();
    expect(doc(a)?.title).toBe("kept");
    expect(doc(b)?.title).toBe("kept");
  });

  it("returns false once the tombstone was pushed, because tombstones are sticky", async () => {
    const { a, b, syncAll } = pair();
    a.setField("t", "1", "title", "gone");
    a.deleteDocument("t", "1");
    await syncAll();
    expect(a.undo()).toBe(false);
    expect(doc(a)).toBeNull();
    expect(doc(b)).toBeNull();
  });
});
