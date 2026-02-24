/**
 * Hybrid Logical Clock implementation for TypeScript.
 *
 * Port of grove/crdt/clock.go. Provides a totally ordered, causally
 * consistent clock that requires no coordination between nodes.
 */

import type { HLC } from "./types.js";

/** The zero HLC value. */
export const HLC_ZERO: HLC = { ts: 0, c: 0, node: "" };

/**
 * Compare two HLC values.
 * Returns -1 if a < b, 0 if equal, 1 if a > b.
 * Ordering: timestamp first, then counter, then node (lexicographic tiebreak).
 */
export function hlcCompare(a: HLC, b: HLC): number {
  if (a.ts < b.ts) return -1;
  if (a.ts > b.ts) return 1;
  if (a.c < b.c) return -1;
  if (a.c > b.c) return 1;
  if (a.node < b.node) return -1;
  if (a.node > b.node) return 1;
  return 0;
}

/** Returns true if a is strictly after b. */
export function hlcAfter(a: HLC, b: HLC): boolean {
  return hlcCompare(a, b) > 0;
}

/** Returns true if the HLC is zero-valued (unset). */
export function hlcIsZero(h: HLC): boolean {
  return h.ts === 0 && h.c === 0 && h.node === "";
}

/** Returns the greater of two HLCs. */
export function hlcMax(a: HLC, b: HLC): HLC {
  return hlcAfter(a, b) ? a : b;
}

/** Returns a deterministic string representation matching Go's HLC.String(). */
export function hlcString(h: HLC): string {
  return `HLC{ts:${h.ts} c:${h.c} node:${h.node}}`;
}

/** Default max clock drift in nanoseconds (5 seconds). */
const DEFAULT_MAX_DRIFT_NS = 5_000_000_000;

/**
 * HybridClock generates monotonically increasing HLC timestamps.
 *
 * Port of Go HybridClock from grove/crdt/clock.go.
 * Uses Date.now() * 1_000_000 to convert milliseconds to nanoseconds
 * for compatibility with the Go server.
 */
export class HybridClock {
  readonly nodeID: string;
  private last: HLC;
  private maxDriftNs: number;
  private nowFn: () => number;

  constructor(
    nodeID: string,
    options?: {
      /** Max tolerable clock drift in milliseconds (default: 5000). */
      maxDriftMs?: number;
      /** Override wall clock source (returns ms since epoch). For testing. */
      nowFn?: () => number;
    }
  ) {
    this.nodeID = nodeID;
    this.last = { ...HLC_ZERO };
    this.maxDriftNs = (options?.maxDriftMs ?? 5000) * 1_000_000;
    this.nowFn = options?.nowFn ?? (() => Date.now());
  }

  /**
   * Generate a new HLC that is causally after all previously observed values.
   */
  now(): HLC {
    const physicalNow = this.nowFn() * 1_000_000; // ms → ns

    if (physicalNow > this.last.ts) {
      this.last = { ts: physicalNow, c: 0, node: this.nodeID };
    } else {
      this.last = {
        ts: this.last.ts,
        c: this.last.c + 1,
        node: this.nodeID,
      };
    }

    return { ...this.last };
  }

  /**
   * Merge a received remote HLC into the local clock state.
   * Ensures the next now() is causally after both local and remote.
   *
   * Port of Go HybridClock.Update() from clock.go lines 134-177.
   */
  update(remote: HLC): void {
    const physicalNow = this.nowFn() * 1_000_000;

    // Clamp remote timestamp to prevent runaway clocks.
    const maxAllowed = physicalNow + this.maxDriftNs;
    const remoteTS = Math.min(remote.ts, maxAllowed);

    if (physicalNow > this.last.ts && physicalNow > remoteTS) {
      // Physical clock is ahead of both — reset counter.
      this.last = { ts: physicalNow, c: 0, node: this.nodeID };
    } else if (this.last.ts === remoteTS) {
      // Local and remote have the same timestamp — take max counter + 1.
      const counter = Math.max(this.last.c, remote.c);
      this.last = { ts: this.last.ts, c: counter + 1, node: this.nodeID };
    } else if (this.last.ts > remoteTS) {
      // Local is ahead — just increment counter.
      this.last = { ts: this.last.ts, c: this.last.c + 1, node: this.nodeID };
    } else {
      // Remote is ahead — adopt remote timestamp, increment its counter.
      this.last = { ts: remoteTS, c: remote.c + 1, node: this.nodeID };
    }
  }
}
