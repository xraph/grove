import { describe, it, expect } from "vitest";
import { PresenceManager } from "../presence.js";
import { CRDTClient } from "../client.js";
import type { Transport, PresenceState, PresenceUpdate } from "../types.js";

const HLC0 = { ts: "0", c: 0, node: "" };

describe("presence", () => {
  it("getPresence is referentially stable between events", () => {
    const pm = new PresenceManager("me");
    pm.applyEvent({ type: "join", node_id: "peer", topic: "t", data: { a: 1 } });
    expect(pm.getPresence("t")).toBe(pm.getPresence("t"));
  });

  it("getPresence returns a new reference after an event", () => {
    const pm = new PresenceManager("me");
    pm.applyEvent({ type: "join", node_id: "peer", topic: "t", data: { a: 1 } });
    const first = pm.getPresence("t");
    pm.applyEvent({ type: "update", node_id: "peer", topic: "t", data: { a: 2 } });
    expect(pm.getPresence("t")).not.toBe(first);
  });

  it("an empty topic returns a stable empty array", () => {
    const pm = new PresenceManager("me");
    expect(pm.getPresence("nope")).toBe(pm.getPresence("nope"));
  });

  it("seed populates peers already present (D6)", () => {
    const pm = new PresenceManager("me");
    const states: PresenceState[] = [
      { node_id: "bob", topic: "t", data: { name: "Bob" }, updated_at: Date.now() },
      { node_id: "me", topic: "t", data: {}, updated_at: Date.now() },
    ];
    pm.seed("t", states);
    const peers = pm.getPresence("t");
    expect(peers).toHaveLength(1);
    expect(peers[0].node_id).toBe("bob");
  });

  it("prune drops peers older than maxAge", () => {
    const pm = new PresenceManager("me");
    pm.seed("t", [
      { node_id: "stale", topic: "t", data: {}, updated_at: Date.now() - 60_000 },
      { node_id: "fresh", topic: "t", data: {}, updated_at: Date.now() },
    ]);
    pm.prune(30_000);
    expect(pm.getPresence("t").map((p) => p.node_id)).toEqual(["fresh"]);
  });

  it("prune drops a seeded peer whose updated_at arrived as an RFC3339 string", () => {
    // crdt/presence_types.go declares UpdatedAt as time.Time, so the wire
    // carries an RFC3339 STRING while types.ts says number. seed() now
    // stores server states, so a seeded peer used to carry a string where
    // applyEvent puts Date.now() — and `state.updated_at < cutoff` is then
    // a string-vs-number comparison that is always false. Seeded peers
    // never expired.
    const pm = new PresenceManager("me");
    const staleISO = new Date(Date.now() - 60_000).toISOString();
    pm.seed("t", [
      {
        node_id: "stale",
        topic: "t",
        data: {},
        updated_at: staleISO as unknown as number,
      },
      { node_id: "fresh", topic: "t", data: {}, updated_at: Date.now() },
    ]);

    // Normalized on ingest, so what the manager holds is epoch ms.
    expect(typeof pm.getPeer("t", "stale")!.updated_at).toBe("number");
    expect(pm.getPeer("t", "stale")!.updated_at).toBe(Date.parse(staleISO));

    pm.prune(30_000);
    expect(pm.getPresence("t").map((p) => p.node_id)).toEqual(["fresh"]);
  });

  it("seed leaves a numeric updated_at untouched", () => {
    const pm = new PresenceManager("me");
    const at = Date.now() - 1234;
    pm.seed("t", [{ node_id: "bob", topic: "t", data: {}, updated_at: at }]);
    expect(pm.getPeer("t", "bob")!.updated_at).toBe(at);
  });

  it("joinPresence seeds from the server snapshot", async () => {
    const snapshot: PresenceState[] = [
      { node_id: "bob", topic: "t", data: { name: "Bob" }, updated_at: Date.now() },
    ];
    const sent: PresenceUpdate[] = [];
    const transport: Transport = {
      async pull() { return { changes: [], latest_hlc: HLC0 }; },
      async push() { return { merged: 0, latest_hlc: HLC0 }; },
      async updatePresence(u) { sent.push(u); },
      async getPresence() { return snapshot; },
    };
    const client = new CRDTClient({ nodeID: "me", transport });
    await client.joinPresence("t", { name: "Me" });
    expect(sent).toHaveLength(1);
    expect(client.presence.getPresence("t").map((p) => p.node_id)).toEqual(["bob"]);
    await client.leaveAllPresence();
  });
});
