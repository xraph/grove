import { afterAll, beforeAll, expect, it } from "vitest";
import { spawn, execFileSync, type ChildProcess } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { RoomClient } from "../room.js";
import { CRDTClient } from "../client.js";
import { CRDTStore } from "../store.js";
import type { ChangeRecord, DocumentState } from "../types.js";

// Opt-in review probes assert the required behavior against a real Go server.
// Run with GROVE_CRDT_REVIEW=1 until the production defects are fixed.
const enabled = process.env.GROVE_CRDT_REVIEW === "1";
const review = it.runIf(enabled);
let server: ChildProcess | undefined;
let scratch: string | undefined;
let baseURL = "";
beforeAll(async () => {
  if (!enabled) return;
  scratch = mkdtempSync(join(tmpdir(), "grove-crdt-review-"));
  const binary = join(scratch, "server");
  execFileSync("go", ["build", "-o", binary, "."], {
    cwd: fileURLToPath(
      new URL("../../../crdt-dart/tool/conformance_server/", import.meta.url),
    ),
    env: { ...process.env, GOWORK: "off" },
    timeout: 120000,
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
}, 120000);
afterAll(async () => {
  if (server && server.exitCode === null) {
    await new Promise<void>((resolve) => {
      server!.once("exit", () => resolve());
      server!.kill();
    });
  }
  if (scratch) rmSync(scratch, { recursive: true, force: true });
});

review(
  "TypeScript can update room metadata on the actual Go server",
  async () => {
    const rooms = new RoomClient({ baseURL });
    await rooms.createRoom("review-room");
    await rooms.updateMetadata("review-room", { title: "changed" });
    expect((await rooms.getRoom("review-room"))?.metadata).toEqual({
      title: "changed",
    });
  },
);

review(
  "TypeScript and Go choose the same LWW winner for Unicode node IDs",
  async () => {
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
  },
);
