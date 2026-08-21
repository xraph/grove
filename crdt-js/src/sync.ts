/**
 * Sync orchestration: the pull → apply → push → clear cycle.
 *
 * Lives in the core rather than in the React hook so non-React consumers
 * get the same behavior, and so SyncHook has real call sites.
 */

import type { ChangeRecord, HLC, SyncReport } from "./types.js";
import type { CRDTClient } from "./client.js";
import type { CRDTStore } from "./store.js";

export class SyncEngine {
  private _lastSyncTime: number | null = null;
  private _lastPulledHLC: HLC | null = null;
  private inFlight: Promise<SyncReport> | null = null;
  private timer: ReturnType<typeof setInterval> | null = null;
  private onlineHandler: (() => void) | null = null;

  constructor(
    private client: CRDTClient,
    private store: CRDTStore
  ) {}

  /** Timestamp of the last successful sync, ms since epoch. */
  get lastSyncTime(): number | null {
    return this._lastSyncTime;
  }

  /** Server HLC watermark from the last successful pull. */
  get lastPulledHLC(): HLC | null {
    return this._lastPulledHLC;
  }

  /**
   * Run one full sync. Concurrent calls share the in-flight run rather
   * than racing each other into a double push.
   */
  sync(): Promise<SyncReport> {
    if (this.inFlight) return this.inFlight;
    this.inFlight = this.run().finally(() => {
      this.inFlight = null;
    });
    return this.inFlight;
  }

  private async run(): Promise<SyncReport> {
    const plugins = this.store.pluginManager;
    const report: SyncReport = { pulled: 0, pushed: 0, merged: 0, conflicts: 0 };

    // Snapshot BEFORE any await in this run — including the pull leg —
    // so writes landing anywhere during the round trip (while pull is in
    // flight, or while push is in flight) stay pending instead of being
    // cleared out from under the user. This runs synchronously as part of
    // the same tick that called sync(), before control ever yields.
    const snapshot: ChangeRecord[] = this.store.getPendingChanges();

    // --- Pull ---
    const pullEvent = plugins.dispatchBeforePull({
      tables: [],
      since: this._lastPulledHLC ?? undefined,
    });
    if (pullEvent) {
      const resp = await this.client.pull(
        pullEvent.tables.length > 0 ? pullEvent.tables : undefined,
        pullEvent.since
      );
      report.pulled = resp.changes.length;
      if (resp.changes.length > 0) {
        this.store.applyChanges(resp.changes);
      }
      if (resp.latest_hlc) this._lastPulledHLC = resp.latest_hlc;
      plugins.dispatchAfterPull({ ...pullEvent, changes: resp.changes });
    }

    // --- Push ---
    if (snapshot.length > 0) {
      const toPush = plugins.dispatchBeforePush(snapshot);
      if (toPush && toPush.length > 0) {
        const resp = await this.client.push(toPush);
        report.pushed = toPush.length;
        report.merged = resp.merged;
        this.store.clearPendingChanges(snapshot);
        plugins.dispatchAfterPush({ pushed: toPush.length, changes: toPush });
      }
    }

    this._lastSyncTime = Date.now();
    return report;
  }

  /**
   * Sync periodically, and immediately whenever the environment reports it
   * is back online. Returns a stop function — call it on unmount.
   *
   * A failed sync is swallowed: the pending queue is durable, so the next
   * tick retries. Read lastSyncTime for status.
   */
  start(options?: { intervalMs?: number }): () => void {
    this.stop();
    const interval = options?.intervalMs ?? 30_000;

    this.timer = setInterval(() => {
      void this.sync().catch(() => {});
    }, interval);

    // Symmetric feature-detect: only install the listener if we can also
    // remove it later — a host exposing one without the other must not
    // end up with a listener stop() can never clean up.
    if (
      typeof globalThis.addEventListener === "function" &&
      typeof globalThis.removeEventListener === "function"
    ) {
      this.onlineHandler = () => { void this.sync().catch(() => {}); };
      globalThis.addEventListener("online", this.onlineHandler);
    }

    return () => this.stop();
  }

  /** Stop periodic syncing and remove the online listener. */
  stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    if (this.onlineHandler && typeof globalThis.removeEventListener === "function") {
      globalThis.removeEventListener("online", this.onlineHandler);
      this.onlineHandler = null;
    }
  }
}
