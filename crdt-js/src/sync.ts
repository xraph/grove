/**
 * Sync orchestration: the pull → apply → push → clear cycle.
 *
 * Lives in the core rather than in the React hook so non-React consumers
 * get the same behavior, and so SyncHook has real call sites.
 */

import type { ChangeRecord, HLC, SyncReport } from "./types.js";
import type { CRDTClient } from "./client.js";
import type { CRDTStore } from "./store.js";
import { isPushRejection } from "./errors.js";

/** A pending change the server refused, with the error it answered. */
export interface PushRejection {
  change: ChangeRecord;
  error: Error;
}

/**
 * Drives one pull -> apply -> push -> clear cycle, on a timer or on demand.
 *
 * A note for anyone writing a `beforePush` plugin: the pending queue is
 * cleared using the snapshot taken BEFORE the hook ran, not the array the
 * hook returned. So `beforePush` must return the same record objects it
 * was handed, or cancel the push outright by returning null. Returning a
 * SUBSET is not filtering — the withheld records are cleared from the
 * queue along with the pushed ones and are gone for good. The pre-hook
 * snapshot is deliberate: it is also what keeps writes that land mid-round-
 * trip from being cleared out from under the user.
 */
export class SyncEngine {
  private _lastSyncTime: number | null = null;
  private _lastPulledHLC: HLC | null = null;
  private inFlight: Promise<SyncReport> | null = null;
  private timer: ReturnType<typeof setInterval> | null = null;
  private onlineHandler: (() => void) | null = null;
  private rejectionHandlers = new Set<(rejections: PushRejection[]) => void>();

  constructor(
    private client: CRDTClient,
    private store: CRDTStore
  ) {}

  /** Timestamp of the last successful sync, ms since epoch. */
  get lastSyncTime(): number | null {
    return this._lastSyncTime;
  }

  /**
   * Notified when the server refuses pending changes (a validation failure
   * or a hook rejection). Those changes are dropped from the pending queue,
   * because resending them would only be refused again and would hold up
   * every change queued behind them. They stay applied locally, so surface
   * them to the user. Returns an unsubscribe function.
   */
  onPushRejected(handler: (rejections: PushRejection[]) => void): () => void {
    this.rejectionHandlers.add(handler);
    return () => { this.rejectionHandlers.delete(handler); };
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
      // `clearPendingChanges(snapshot)` below clears the PRE-hook array.
      // A beforePush plugin that returns a subset therefore loses the
      // records it withheld — see the note on this class and on the
      // beforePush hook itself.
      const toPush = plugins.dispatchBeforePush(snapshot);
      if (toPush && toPush.length > 0) {
        try {
          const resp = await this.client.push(toPush);
          report.pushed = toPush.length;
          report.merged = resp.merged;
          this.store.clearPendingChanges(snapshot);
          plugins.dispatchAfterPush({ pushed: toPush.length, changes: toPush });
        } catch (err) {
          if (!isPushRejection(err)) throw err;
          // The server merged nothing from the batch. Find out which
          // changes it refuses by pushing them one at a time.
          await this.isolateRejected(toPush, snapshot, report);
        }
      }
    }

    this._lastSyncTime = Date.now();
    return report;
  }

  /**
   * Push a refused batch one change at a time. Accepted changes are kept,
   * changes refused on their own are dropped and reported. A transient
   * failure stops the walk and leaves the rest pending for the next sync.
   */
  private async isolateRejected(
    toPush: ChangeRecord[],
    snapshot: ChangeRecord[],
    report: SyncReport
  ): Promise<void> {
    const accepted: ChangeRecord[] = [];
    const rejections: PushRejection[] = [];
    let failure: unknown = null;

    for (const change of toPush) {
      try {
        const resp = await this.client.push([change]);
        accepted.push(change);
        report.merged += resp.merged;
      } catch (err) {
        if (!isPushRejection(err)) {
          failure = err;
          break;
        }
        rejections.push({ change, error: err as Error });
      }
    }

    report.pushed = accepted.length;
    report.rejected = rejections.length;
    // A clean walk settles the whole snapshot, matching the success path
    // (see the beforePush note on this class). After a transient failure
    // only what was settled leaves the queue.
    this.store.clearPendingChanges(
      failure === null ? snapshot : [...accepted, ...rejections.map((r) => r.change)]
    );
    if (accepted.length > 0) {
      this.store.pluginManager.dispatchAfterPush({ pushed: accepted.length, changes: accepted });
    }
    if (rejections.length > 0) {
      for (const handler of this.rejectionHandlers) handler(rejections);
    }
    if (failure !== null) throw failure;
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
