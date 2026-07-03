/**
 * Hybrid Logical Clock implementation for TypeScript.
 *
 * Port of grove/crdt/clock.go. Provides a totally ordered, causally
 * consistent clock that requires no coordination between nodes.
 *
 * PRECISION: timestamps are nanosecond int64s, which exceed JavaScript's
 * 2^53 safe-integer range — a numeric ts silently rounds (~±128ns at
 * current epochs), corrupting recomputed map keys (origin/tag/node
 * identity must match Go's exact strings) and clock comparisons. The wire
 * therefore carries ts as a DECIMAL STRING (Go emits strings; legacy
 * numbers are still accepted), all comparisons are exact via canonical
 * decimal strings, and the clock generates with BigInt.
 */

import type { HLC } from "./types.js";

/** The zero HLC value. */
export const HLC_ZERO: HLC = { ts: 0, c: 0, node: "" };

/** Canonical decimal string of an HLC timestamp (number or string). */
export function hlcTsString(ts: number | string): string {
  if (typeof ts === "number") {
    return Math.trunc(ts).toString();
  }
  // Normalize (strip leading zeros / '+', tolerate "") without precision loss.
  try {
    return BigInt(ts === "" ? 0 : ts).toString();
  } catch {
    return "0";
  }
}

/** Exact comparison of two canonical non-negative decimal strings. */
function cmpDecimal(a: string, b: string): number {
  const negA = a.startsWith("-");
  const negB = b.startsWith("-");
  if (negA !== negB) return negA ? -1 : 1;
  let sign = 1;
  if (negA) {
    sign = -1;
    a = a.slice(1);
    b = b.slice(1);
  }
  if (a.length !== b.length) return (a.length < b.length ? -1 : 1) * sign;
  if (a === b) return 0;
  return (a < b ? -1 : 1) * sign;
}

/**
 * Compare two HLC values (exact — no float rounding).
 * Returns -1 if a < b, 0 if equal, 1 if a > b.
 * Ordering: timestamp first, then counter, then node (lexicographic tiebreak).
 */
export function hlcCompare(a: HLC, b: HLC): number {
  const ts = cmpDecimal(hlcTsString(a.ts), hlcTsString(b.ts));
  if (ts !== 0) return ts;
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
  return h.c === 0 && h.node === "" && hlcTsString(h.ts) === "0";
}

/** Returns the greater of two HLCs. */
export function hlcMax(a: HLC, b: HLC): HLC {
  return hlcAfter(a, b) ? a : b;
}

/**
 * Returns a deterministic string representation matching Go's HLC.String()
 * exactly (Go formats the int64 as plain decimal).
 */
export function hlcString(h: HLC): string {
  return `HLC{ts:${hlcTsString(h.ts)} c:${h.c} node:${h.node}}`;
}

const NS_PER_MS = 1_000_000n;

function tsBigInt(ts: number | string): bigint {
  try {
    return BigInt(hlcTsString(ts));
  } catch {
    return 0n;
  }
}

/**
 * HybridClock generates monotonically increasing HLC timestamps.
 *
 * Port of Go HybridClock from grove/crdt/clock.go. Timestamps are minted
 * with BigInt (ms → ns) and emitted as decimal strings so they stay exact
 * end to end.
 */
export class HybridClock {
  readonly nodeID: string;
  private lastTs: bigint;
  private lastC: number;
  private maxDriftNs: bigint;
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
    this.lastTs = 0n;
    this.lastC = 0;
    this.maxDriftNs = BigInt(options?.maxDriftMs ?? 5000) * NS_PER_MS;
    this.nowFn = options?.nowFn ?? (() => Date.now());
  }

  private emit(): HLC {
    return { ts: this.lastTs.toString(), c: this.lastC, node: this.nodeID };
  }

  /**
   * Generate a new HLC that is causally after all previously observed values.
   */
  now(): HLC {
    const physicalNow = BigInt(this.nowFn()) * NS_PER_MS;

    if (physicalNow > this.lastTs) {
      this.lastTs = physicalNow;
      this.lastC = 0;
    } else {
      this.lastC += 1;
    }

    return this.emit();
  }

  /**
   * Merge a received remote HLC into the local clock state.
   * Ensures the next now() is causally after both local and remote.
   *
   * Port of Go HybridClock.Update().
   */
  update(remote: HLC): void {
    const physicalNow = BigInt(this.nowFn()) * NS_PER_MS;

    // Clamp remote timestamp to prevent runaway clocks.
    const maxAllowed = physicalNow + this.maxDriftNs;
    let remoteTS = tsBigInt(remote.ts);
    if (remoteTS > maxAllowed) remoteTS = maxAllowed;

    if (physicalNow > this.lastTs && physicalNow > remoteTS) {
      // Physical clock is ahead of both — reset counter.
      this.lastTs = physicalNow;
      this.lastC = 0;
    } else if (this.lastTs === remoteTS) {
      // Local and remote have the same timestamp — take max counter + 1.
      this.lastC = Math.max(this.lastC, remote.c) + 1;
    } else if (this.lastTs > remoteTS) {
      // Local is ahead — just increment counter.
      this.lastC += 1;
    } else {
      // Remote is ahead — adopt its timestamp, counter + 1.
      this.lastTs = remoteTS;
      this.lastC = remote.c + 1;
    }
  }

  /** The last HLC value issued or merged (for tests/inspection). */
  get last(): HLC {
    return this.emit();
  }
}
