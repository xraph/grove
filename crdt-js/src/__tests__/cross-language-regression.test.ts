import { afterAll, beforeAll, expect, it } from "vitest";
import { spawn, execFileSync, type ChildProcess } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { RoomClient } from "../room.js";
import { CRDTClient } from "../client.js";
import { CRDTStore } from "../store.js";
import type { ChangeRecord, DocumentState, FieldState } from "../types.js";

let server: ChildProcess | undefined;
let scratch: string | undefined;
let baseURL = "";
beforeAll(async () => {
  scratch = mkdtempSync(join(tmpdir(), "grove-crdt-review-"));
  const binary = join(scratch, "server");
  execFileSync("go", ["build", "-o", binary, "."], {
    cwd: fileURLToPath(
      new URL("../../../crdt-dart/tool/conformance_server/", import.meta.url),
    ),
    env: { ...process.env, GOWORK: "off" },
    timeout: 300000,
  });
  server = spawn(binary, ["-addr", "127.0.0.1:0"], {
    stdio: ["pipe", "pipe", "pipe"],
  });
  baseURL = await new Promise<string>((resolve, reject) => {
    const timeout = setTimeout(
      () => reject(new Error("server startup timed out")),
      15000,
    );
    let text = "";
    server!.stdout!.on("data", (chunk) => {
      text += String(chunk);
      const match = /LISTENING (http:\/\/[^\s]+)/.exec(text);
      if (match) {
        clearTimeout(timeout);
        resolve(match[1] + "/sync");
      }
    });
    server!.once("error", (error) => {
      clearTimeout(timeout);
      reject(error);
    });
    server!.once("exit", (code) => {
      clearTimeout(timeout);
      reject(new Error(`server exited: ${code}`));
    });
  });
}, 300000);
afterAll(async () => {
  if (server && server.exitCode === null) {
    await new Promise<void>((resolve) => {
      server!.once("exit", () => resolve());
      server!.kill();
    });
  }
  if (scratch) rmSync(scratch, { recursive: true, force: true });
});

it("TypeScript can update room metadata on the actual Go server", async () => {
  const rooms = new RoomClient({ baseURL });
  await rooms.createRoom("review-room");
  await rooms.updateMetadata("review-room", { title: "changed" });
  expect((await rooms.getRoom("review-room"))?.metadata).toEqual({
    title: "changed",
  });
});

it("TypeScript and Go choose the same LWW winner for Unicode node IDs", async () => {
  const client = new CRDTClient({
    nodeID: "reader",
    baseURL,
    tables: ["notes"],
  });
  const store = new CRDTStore("reader", client.clock);
  await store.ready;
  const changes: ChangeRecord[] = ["\ue000", "\u{1f600}"].map((node) => ({
    table: "notes",
    pk: "unicode",
    field: "title",
    crdt_type: "lww",
    hlc: { ts: "100", c: 0, node },
    node_id: node,
    value: node,
  }));
  store.applyChanges(changes);
  const pushed = await client.push(changes);
  expect(pushed.merged).toBe(2);
  const response = await fetch(
    baseURL.replace(/\/sync$/, "/admin/state?table=notes&pk=unicode"),
  );
  expect(response.ok).toBe(true);
  const remote = (await response.json()) as DocumentState;
  const winner = remote.fields.title.value;
  expect(winner).toBe("\u{1f600}");
  expect(store.getDocument("notes", "unicode")?.title).toBe(winner);
});

it("Go and TypeScript retain document replacements when stale peers reconnect", async () => {
  const client = new CRDTClient({
    nodeID: "reader",
    baseURL,
    tables: ["notes"],
  });
  const store = new CRDTStore("reader", client.clock);
  await store.ready;
  const stamp = (ts: string) => ({ ts, c: 0, node: "peer" });
  const leaf = (ts: string, value: unknown): FieldState => ({
    type: "lww",
    hlc: stamp(ts),
    node_id: "peer",
    value,
  });
  const old: FieldState = {
    type: "document",
    hlc: stamp("100"),
    node_id: "peer",
    doc_state: {
      fields: { gone: leaf("100", true), keep: leaf("100", "old") },
    },
  };
  const reset: FieldState = {
    type: "document",
    hlc: stamp("200"),
    node_id: "peer",
    doc_state: {
      reset_hlc: stamp("200"),
      fields: { keep: leaf("200", "new") },
    },
  };
  const change = (state: FieldState): ChangeRecord => ({
    table: "notes",
    pk: "reset",
    field: "meta",
    crdt_type: "document",
    node_id: "peer",
    hlc: state.hlc,
    state,
  });
  for (const state of [old, reset, old]) {
    store.applyChanges([change(state)]);
    expect((await client.push([change(state)])).merged).toBe(1);
  }
  expect(store.getDocument("notes", "reset")?.meta).toEqual({ keep: "new" });
  const response = await fetch(
    baseURL.replace(/\/sync$/, "/admin/state?table=notes&pk=reset"),
  );
  expect(response.ok).toBe(true);
  const remote = (await response.json()) as DocumentState;
  expect(remote.fields.meta.doc_state?.fields.gone).toBeUndefined();
  expect(remote.fields.meta.doc_state?.reset_hlc).toEqual(stamp("200"));
  const newer: ChangeRecord = {
    ...change(old),
    state: undefined,
    hlc: stamp("300"),
    value: { path: "later", value: true },
  };
  store.applyChanges([newer]);
  store.setDocumentField("notes", "reset", "meta", "local", true);
  store.applyChanges([change(old)]);
  expect(store.getDocument("notes", "reset")?.meta).toEqual({
    keep: "new",
    later: true,
    local: true,
  });
});
