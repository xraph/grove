import { afterEach, expect, it, vi } from "vitest";
import { CRDTClient } from "../client.js";
import type { PresenceState, Transport } from "../types.js";

const zero = { ts: "0", c: 0, node: "" };
const resync = { type: "resync", topic: "", node_id: "" } as const;
const peer = (node_id: string): PresenceState => ({ node_id, topic: "room", data: {}, updated_at: Date.now() });
function setup() {
  const transport: Transport = {
    pull: async () => ({ changes: [], latest_hlc: zero }),
    push: async () => ({ merged: 0, latest_hlc: zero }),
    updatePresence: vi.fn(async () => {}),
    getPresence: vi.fn(async () => [peer("idle")]),
  };
  const client = new CRDTClient({ nodeID: "me", transport, presence: { heartbeatInterval: 100 } });
  return { client, transport };
}
afterEach(() => vi.useRealTimers());

it("clears ghost peers and restores idle peers after overflow without reconnect", async () => {
  vi.useFakeTimers();
  const { client, transport } = setup();
  await client.updatePresence("room", {});
  client.applyPresenceEvent({ type: "join", node_id: "gone", topic: "room", data: {} });
  client.applyPresenceEvent(resync);
  expect(client.presence.getPresence("room")).toEqual([]);
  await vi.advanceTimersByTimeAsync(0);
  expect(client.presence.getPresence("room").map(p => p.node_id)).toEqual(["idle"]);
  expect(transport.getPresence).toHaveBeenCalledTimes(1);
  await client.leaveAllPresence();
});

it("coalesces repeated overflow and discards snapshots made stale by later loss", async () => {
  vi.useFakeTimers();
  const { client, transport } = setup();
  await client.updatePresence("room", {});
  let finish!: (states: PresenceState[]) => void;
  transport.getPresence = vi.fn()
    .mockImplementationOnce(() => new Promise<PresenceState[]>(resolve => { finish = resolve; }))
    .mockResolvedValue([peer("idle")]);
  client.applyPresenceEvent(resync);
  for (let i = 0; i < 20; i++) client.applyPresenceEvent(resync);
  expect(transport.getPresence).toHaveBeenCalledTimes(1);
  const observed: string[][] = [];
  const unsub = client.presence.subscribe("room", () => observed.push(client.presence.getPresence("room").map(p => p.node_id)));
  finish([peer("gone")]);
  await vi.advanceTimersByTimeAsync(0);
  expect(transport.getPresence).toHaveBeenCalledTimes(2);
  expect(observed).toEqual([["idle"]]);
  unsub();
  await client.leaveAllPresence();
});

it("retries failed resync on a heartbeat, keeping ghosts hidden", async () => {
  vi.useFakeTimers();
  const { client, transport } = setup();
  await client.updatePresence("room", {});
  transport.getPresence = vi.fn().mockRejectedValueOnce(new Error("offline")).mockResolvedValue([peer("idle")]);
  client.presence.seed("room", [peer("gone")]);
  client.applyPresenceEvent(resync);
  await vi.advanceTimersByTimeAsync(0);
  expect(client.presence.getPresence("room")).toEqual([]);
  await vi.advanceTimersByTimeAsync(100);
  expect(client.presence.getPresence("room").map(p => p.node_id)).toEqual(["idle"]);
  expect(transport.getPresence).toHaveBeenCalledTimes(2);
  await client.leaveAllPresence();
  expect(vi.getTimerCount()).toBe(0);
});

it("preserves departures during resync and ignores snapshots completing after leave", async () => {
  vi.useFakeTimers();
  const { client, transport } = setup();
  await client.updatePresence("room", {});
  let finish!: (states: PresenceState[]) => void;
  transport.getPresence = () => new Promise(resolve => { finish = resolve; });
  client.applyPresenceEvent(resync);
  client.applyPresenceEvent({ type: "leave", node_id: "gone", topic: "room" });
  finish([peer("gone"), peer("idle")]);
  await vi.advanceTimersByTimeAsync(0);
  expect(client.presence.getPresence("room").map(p => p.node_id)).toEqual(["idle"]);
  client.applyPresenceEvent(resync);
  await client.leavePresence("room");
  finish([peer("gone")]);
  await vi.advanceTimersByTimeAsync(0);
  expect(client.presence.getPresence("room")).toEqual([]);
});

it("does not invalidate a slow snapshot on every heartbeat", async () => {
  vi.useFakeTimers();
  const { client, transport } = setup();
  await client.updatePresence("room", {});
  let finish!: (states: PresenceState[]) => void;
  transport.getPresence = vi.fn(() => new Promise<PresenceState[]>(resolve => { finish = resolve; }));
  client.applyPresenceEvent(resync);
  await vi.advanceTimersByTimeAsync(500);
  finish([peer("idle")]);
  await vi.advanceTimersByTimeAsync(0);
  expect(transport.getPresence).toHaveBeenCalledTimes(1);
  expect(client.presence.getPresence("room").map(p => p.node_id)).toEqual(["idle"]);
  await client.leaveAllPresence();
});
