import { act, render } from "@testing-library/react";
import { StrictMode } from "react";
import { afterEach, expect, it, vi } from "vitest";
import { CRDTProvider, useRoom } from "../react.js";
import type { UseRoomReturn } from "../react.js";
import type { ParticipantData } from "../room.js";
import type { PresenceUpdate, Transport } from "../types.js";

const zero = { ts: "0", c: 0, node: "" };
afterEach(() => vi.useRealTimers());

it("paces useRoom updates in StrictMode and leaves without a late rejoin", async () => {
  vi.useFakeTimers();
  const writes: PresenceUpdate[] = [];
  const transport: Transport = {
    pull: async () => ({ changes: [], latest_hlc: zero }),
    push: async () => ({ merged: 0, latest_hlc: zero }),
    updatePresence: async update => { writes.push(update); },
    getPresence: async () => [],
  };
  let room!: UseRoomReturn<ParticipantData>;
  function Probe() {
    room = useRoom("room", { nodeId: "me", name: "Ada" }, { typingTimeout: 100 });
    return null;
  }
  const view = render(<StrictMode><CRDTProvider config={{ nodeID: "me", transport, streaming: false, autoSync: false }}><Probe /></CRDTProvider></StrictMode>);
  await act(async () => { await vi.advanceTimersByTimeAsync(0); });
  expect(writes).toHaveLength(1);
  await act(async () => {
    for (let x = 0; x < 100; x++) room.updateCursor({ x, y: 0 });
    room.setTyping(true);
    await vi.advanceTimersByTimeAsync(50);
  });
  expect(writes).toHaveLength(2);
  expect(writes[1].data.cursor).toEqual({ x: 99, y: 0 });
  await act(async () => { await vi.advanceTimersByTimeAsync(100); });
  expect(room.self?.is_typing).toBe(false);
  await act(async () => { room.leave(); });
  expect(room.self).toBeNull();
  await act(async () => { view.unmount(); await vi.advanceTimersByTimeAsync(30_000); });
  expect(writes[writes.length - 1].data).toBeNull();
  expect(writes.filter(w => w.data === null)).toHaveLength(1);
  expect(vi.getTimerCount()).toBe(0);
});

it("uses the configured fetch and auth for room joins, rejecting full rooms before presence", async () => {
  const errors: unknown[] = [];
  const fetch = vi.fn(async (_url: RequestInfo | URL, init?: RequestInit) => {
    expect(new Headers(init?.headers).get("Authorization")).toBe("Bearer test");
    return new Response(JSON.stringify({ error: "room full" }), { status: 409, headers: { "content-type": "application/json" } });
  });
  function Probe() {
    useRoom("room", { nodeId: "me" }, { onError: error => errors.push(error) });
    return null;
  }
  const view = render(<CRDTProvider config={{ nodeID: "me", baseURL: "https://example.test/sync", fetch,
    auth: { getHeaders: () => ({ Authorization: "Bearer test" }) }, streaming: false, autoSync: false }}><Probe /></CRDTProvider>);
  await act(async () => {});
  expect(fetch).toHaveBeenCalledTimes(1);
  expect(String(fetch.mock.calls[0][0])).toContain("/rooms/room/join");
  expect(errors).toHaveLength(1);
  await act(async () => { view.unmount(); });
  expect(fetch).toHaveBeenCalledTimes(1);
});
