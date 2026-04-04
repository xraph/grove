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
  RGAListState,
  RGANode,
  DocumentCRDTState,
} from "./types.js";
import { hlcAfter, hlcCompare, hlcString } from "./hlc.js";

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

// --- RGA List ---

/** Compute a deterministic string key for an HLC (used as node map key). */
function hlcKey(hlc: HLC): string {
  return `${hlc.ts}:${hlc.c}:${hlc.node}`;
}

/** Create an empty RGA list state. */
export function newRGAListState(): RGAListState {
  return { nodes: {} };
}

/**
 * Merge two RGA list states. For each node key, the version with the
 * higher HLC wins; if equal, prefer non-tombstoned.
 */
export function mergeListState(
  local: RGAListState | undefined,
  remote: RGAListState | undefined
): RGAListState {
  if (!local) return remote ?? newRGAListState();
  if (!remote) return local;

  const merged = newRGAListState();

  // Copy local nodes.
  for (const [key, node] of Object.entries(local.nodes)) {
    merged.nodes[key] = { ...node };
  }

  // Merge remote nodes.
  for (const [key, remoteNode] of Object.entries(remote.nodes)) {
    const existing = merged.nodes[key];
    if (!existing) {
      merged.nodes[key] = { ...remoteNode };
    } else {
      // Higher HLC wins.
      if (hlcAfter(remoteNode.id, existing.id)) {
        merged.nodes[key] = { ...remoteNode };
      } else if (!hlcAfter(existing.id, remoteNode.id)) {
        // Equal HLC — prefer non-tombstoned.
        if (existing.tombstone && !remoteNode.tombstone) {
          merged.nodes[key] = { ...remoteNode };
        }
      }
    }
  }

  return merged;
}

/**
 * Resolve an RGA list to an ordered array of live (non-tombstoned) values.
 *
 * Walks the linked list from root (parent_id is zero HLC) and
 * uses HLC ordering for sibling resolution.
 */
export function listElements(state: RGAListState): unknown[] {
  const nodes = Object.values(state.nodes);
  if (nodes.length === 0) return [];

  // Build children map: parent key → sorted children.
  const childrenMap = new Map<string, RGANode[]>();
  for (const node of nodes) {
    const pk = hlcKey(node.parent_id);
    let children = childrenMap.get(pk);
    if (!children) {
      children = [];
      childrenMap.set(pk, children);
    }
    children.push(node);
  }

  // Sort each group by HLC descending (newest first, matching RGA insert-right semantics).
  for (const children of childrenMap.values()) {
    children.sort((a, b) => -hlcCompare(a.id, b.id));
  }

  // DFS traversal from root.
  const rootKey = hlcKey({ ts: 0, c: 0, node: "" });
  const result: unknown[] = [];

  function walk(parentKey: string): void {
    const children = childrenMap.get(parentKey);
    if (!children) return;
    for (const child of children) {
      if (!child.tombstone) {
        result.push(child.value);
      }
      walk(hlcKey(child.id));
    }
  }

  walk(rootKey);
  return result;
}

/**
 * Return the ordered node IDs (HLCs) for live elements in an RGA list.
 * Order matches listElements().
 */
export function listNodeIds(state: RGAListState): HLC[] {
  const nodes = Object.values(state.nodes);
  if (nodes.length === 0) return [];

  const childrenMap = new Map<string, RGANode[]>();
  for (const node of nodes) {
    const pk = hlcKey(node.parent_id);
    let children = childrenMap.get(pk);
    if (!children) {
      children = [];
      childrenMap.set(pk, children);
    }
    children.push(node);
  }

  for (const children of childrenMap.values()) {
    children.sort((a, b) => -hlcCompare(a.id, b.id));
  }

  const rootKey = hlcKey({ ts: 0, c: 0, node: "" });
  const result: HLC[] = [];

  function walk(parentKey: string): void {
    const children = childrenMap.get(parentKey);
    if (!children) return;
    for (const child of children) {
      if (!child.tombstone) {
        result.push(child.id);
      }
      walk(hlcKey(child.id));
    }
  }

  walk(rootKey);
  return result;
}

// --- Nested Document CRDT ---

/** Create an empty document CRDT state. */
export function newDocumentCRDTState(): DocumentCRDTState {
  return { fields: {} };
}

/**
 * Merge two nested document CRDT states by merging each field using
 * the same mergeFieldState logic (recursive for nested documents).
 */
export function mergeDocumentState(
  local: DocumentCRDTState | undefined,
  remote: DocumentCRDTState | undefined
): DocumentCRDTState {
  if (!local) return remote ?? newDocumentCRDTState();
  if (!remote) return local;

  const merged = newDocumentCRDTState();

  // Copy local fields.
  for (const [key, field] of Object.entries(local.fields)) {
    merged.fields[key] = { ...field };
  }

  // Merge remote fields.
  for (const [key, remoteField] of Object.entries(remote.fields)) {
    const existing = merged.fields[key];
    if (!existing) {
      merged.fields[key] = { ...remoteField };
    } else {
      // For nested docs and lists, merge the sub-state.
      if (existing.type === "list" && remoteField.type === "list") {
        merged.fields[key] = {
          ...existing,
          hlc: hlcAfter(remoteField.hlc, existing.hlc) ? remoteField.hlc : existing.hlc,
          list_state: mergeListState(existing.list_state, remoteField.list_state),
        };
      } else if (existing.type === "document" && remoteField.type === "document") {
        merged.fields[key] = {
          ...existing,
          hlc: hlcAfter(remoteField.hlc, existing.hlc) ? remoteField.hlc : existing.hlc,
          doc_state: mergeDocumentState(existing.doc_state, remoteField.doc_state),
        };
      } else {
        // Different types or scalar types — higher HLC wins.
        if (hlcAfter(remoteField.hlc, existing.hlc)) {
          merged.fields[key] = { ...remoteField };
        }
      }
    }
  }

  return merged;
}

/**
 * Resolve a nested document CRDT state to a plain object.
 * Recursively resolves nested documents and lists.
 */
export function documentResolve(state: DocumentCRDTState): Record<string, unknown> {
  const result: Record<string, unknown> = {};

  for (const [key, field] of Object.entries(state.fields)) {
    switch (field.type) {
      case "lww":
        result[key] = field.value;
        break;
      case "counter":
        result[key] = field.counter_state ? counterValue(field.counter_state) : 0;
        break;
      case "set":
        result[key] = field.set_state ? setElements(field.set_state) : [];
        break;
      case "list":
        result[key] = field.list_state ? listElements(field.list_state) : [];
        break;
      case "document":
        result[key] = field.doc_state ? documentResolve(field.doc_state) : {};
        break;
      default:
        result[key] = field.value;
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

    case "list": {
      let localList = local?.list_state ?? newRGAListState();
      // Apply list operation from the change.
      if (change.list_op) {
        const op = change.list_op;
        if (op.op === "insert" && op.node_id) {
          const key = hlcKey(op.node_id);
          localList.nodes[key] = {
            id: op.node_id,
            node_id: change.node_id,
            parent_id: op.parent_id ?? { ts: 0, c: 0, node: "" },
            value: op.value,
          };
        } else if (op.op === "delete" && op.node_id) {
          const key = hlcKey(op.node_id);
          const existing = localList.nodes[key];
          if (existing) {
            localList.nodes[key] = { ...existing, tombstone: true };
          }
        }
      }
      return {
        type: "list",
        hlc: change.hlc,
        node_id: change.node_id,
        list_state: localList,
      };
    }

    case "document": {
      let localDoc = local?.doc_state ?? newDocumentCRDTState();
      // For document type, the change carries a field path in the value.
      // Apply as an LWW field within the nested document.
      if (change.value !== undefined) {
        const path = change.field;
        localDoc.fields[path] = {
          type: "lww",
          hlc: change.hlc,
          node_id: change.node_id,
          value: change.value,
        };
      }
      return {
        type: "document",
        hlc: change.hlc,
        node_id: change.node_id,
        doc_state: localDoc,
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
