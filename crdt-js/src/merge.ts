/**
 * Client-side CRDT merge functions.
 *
 * Port of grove/crdt/register.go, counter.go, set.go, and merge.go.
 * These functions are used to merge remote changes into local state.
 */

import type {
  HLC,
  ChangeRecord,
  FieldState,
  PNCounterState,
  ORSetState,
  ORSetTag,
} from "./types.js";
import { hlcAfter, hlcString } from "./hlc.js";

// --- LWW Register ---

/** LWW register value for merging. */
export interface LWWValue {
  value: unknown;
  hlc: HLC;
  nodeID: string;
}

/**
 * Merge two LWW values. Higher HLC wins.
 * Port of MergeLWW() from register.go.
 */
export function mergeLWW(
  local: LWWValue | null,
  remote: LWWValue | null
): LWWValue {
  if (local == null) return remote!;
  if (remote == null) return local;

  if (hlcAfter(remote.hlc, local.hlc)) {
    return remote;
  }
  return local;
}

// --- PN-Counter ---

/** Create an empty PN-Counter state. */
export function newPNCounterState(): PNCounterState {
  return { inc: {}, dec: {} };
}

/**
 * Merge two PN-Counter states by taking max per node.
 * Port of MergeCounter() from counter.go.
 */
export function mergeCounter(
  local: PNCounterState | null,
  remote: PNCounterState | null
): PNCounterState {
  if (local == null) return remote!;
  if (remote == null) return local;

  const merged = newPNCounterState();

  // Merge increments: take max per node.
  for (const [node, v] of Object.entries(local.inc)) {
    merged.inc[node] = v;
  }
  for (const [node, v] of Object.entries(remote.inc)) {
    if (!(node in merged.inc) || v > merged.inc[node]) {
      merged.inc[node] = v;
    }
  }

  // Merge decrements: take max per node.
  for (const [node, v] of Object.entries(local.dec)) {
    merged.dec[node] = v;
  }
  for (const [node, v] of Object.entries(remote.dec)) {
    if (!(node in merged.dec) || v > merged.dec[node]) {
      merged.dec[node] = v;
    }
  }

  return merged;
}

/**
 * Get the resolved value of a PN-Counter.
 * Value = sum(increments) - sum(decrements).
 * Port of PNCounterState.Value() from counter.go.
 */
export function counterValue(state: PNCounterState): number {
  let inc = 0;
  let dec = 0;
  for (const v of Object.values(state.inc)) inc += v;
  for (const v of Object.values(state.dec)) dec += v;
  return inc - dec;
}

// --- OR-Set ---

/**
 * Compute the deterministic tag key matching Go's tagKey() from set.go.
 * Format: "nodeID:HLC{ts:<ts> c:<c> node:<node>}"
 */
export function tagKey(tag: ORSetTag): string {
  return tag.node + ":" + hlcString(tag.hlc);
}

/** Create an empty OR-Set state. */
export function newORSetState(): ORSetState {
  return { entries: {}, removed: {} };
}

/**
 * Merge two OR-Set states (union of entries, union of removed).
 * Port of MergeSet() from set.go.
 */
export function mergeSet(
  local: ORSetState | null,
  remote: ORSetState | null
): ORSetState {
  if (local == null) return remote!;
  if (remote == null) return local;

  const merged = newORSetState();

  // Union entries.
  for (const [elem, tags] of Object.entries(local.entries)) {
    merged.entries[elem] = [...(merged.entries[elem] ?? []), ...tags];
  }
  for (const [elem, tags] of Object.entries(remote.entries)) {
    merged.entries[elem] = [...(merged.entries[elem] ?? []), ...tags];
  }

  // Deduplicate tags per element.
  for (const elem of Object.keys(merged.entries)) {
    merged.entries[elem] = deduplicateTags(merged.entries[elem]);
  }

  // Union removed.
  for (const [key, v] of Object.entries(local.removed)) {
    if (v) merged.removed[key] = true;
  }
  for (const [key, v] of Object.entries(remote.removed)) {
    if (v) merged.removed[key] = true;
  }

  return merged;
}

/**
 * Get the effective elements of an OR-Set (those with at least one
 * non-removed tag).
 * Port of ORSetState.Elements() from set.go.
 */
export function setElements(state: ORSetState): unknown[] {
  const result: unknown[] = [];
  const keys = Object.keys(state.entries).sort();

  for (const key of keys) {
    const tags = state.entries[key];
    if (hasActiveTags(tags, state.removed)) {
      try {
        result.push(JSON.parse(key));
      } catch {
        result.push(key);
      }
    }
  }

  return result;
}

/** Check if any tag is not in the removed set. */
function hasActiveTags(
  tags: ORSetTag[],
  removed: Record<string, boolean>
): boolean {
  for (const tag of tags) {
    if (!removed[tagKey(tag)]) {
      return true;
    }
  }
  return false;
}

/** Deduplicate tags by their key. */
function deduplicateTags(tags: ORSetTag[]): ORSetTag[] {
  const seen = new Set<string>();
  const result: ORSetTag[] = [];
  for (const t of tags) {
    const k = tagKey(t);
    if (!seen.has(k)) {
      seen.add(k);
      result.push(t);
    }
  }
  return result;
}

// --- Field-Level Merge ---

/**
 * Merge a ChangeRecord into existing FieldState.
 * Dispatches to the correct merge function based on CRDTType.
 */
export function mergeFieldState(
  local: FieldState | null,
  change: ChangeRecord
): FieldState {
  switch (change.crdt_type) {
    case "lww": {
      const localLWW: LWWValue | null = local
        ? { value: local.value, hlc: local.hlc, nodeID: local.node_id }
        : null;
      const remoteLWW: LWWValue = {
        value: change.value,
        hlc: change.hlc,
        nodeID: change.node_id,
      };
      const winner = mergeLWW(localLWW, remoteLWW);
      return {
        type: "lww",
        hlc: winner.hlc,
        node_id: winner.nodeID,
        value: winner.value,
      };
    }

    case "counter": {
      let localCounter = local?.counter_state ?? newPNCounterState();
      // Apply delta from the change.
      if (change.counter_delta) {
        const delta = change.counter_delta;
        const remoteCounter = newPNCounterState();
        // Copy local state.
        for (const [k, v] of Object.entries(localCounter.inc)) {
          remoteCounter.inc[k] = v;
        }
        for (const [k, v] of Object.entries(localCounter.dec)) {
          remoteCounter.dec[k] = v;
        }
        // Apply delta to the remote node's counters.
        remoteCounter.inc[change.node_id] =
          (remoteCounter.inc[change.node_id] ?? 0) + delta.inc;
        remoteCounter.dec[change.node_id] =
          (remoteCounter.dec[change.node_id] ?? 0) + delta.dec;
        localCounter = mergeCounter(localCounter, remoteCounter);
      }
      return {
        type: "counter",
        hlc: change.hlc,
        node_id: change.node_id,
        counter_state: localCounter,
      };
    }

    case "set": {
      let localSet = local?.set_state ?? newORSetState();
      // Apply set operation from the change.
      if (change.set_op) {
        const op = change.set_op;
        if (op.op === "add") {
          for (const elem of op.elements) {
            const key = JSON.stringify(elem);
            const newTag: ORSetTag = { node: change.node_id, hlc: change.hlc };
            localSet.entries[key] = [
              ...(localSet.entries[key] ?? []),
              newTag,
            ];
          }
        } else if (op.op === "remove") {
          for (const elem of op.elements) {
            const key = JSON.stringify(elem);
            const tags = localSet.entries[key] ?? [];
            for (const t of tags) {
              localSet.removed[tagKey(t)] = true;
            }
          }
        }
      }
      return {
        type: "set",
        hlc: change.hlc,
        node_id: change.node_id,
        set_state: localSet,
      };
    }

    default:
      // Unknown type — treat as LWW fallback.
      return {
        type: change.crdt_type,
        hlc: change.hlc,
        node_id: change.node_id,
        value: change.value,
      };
  }
}
