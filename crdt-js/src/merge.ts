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
import { mergeText, newTextState, applyTextOpTo, textValue } from "./text.js";
import { withEntry, withoutKeys, withAppended, withFlags } from "./immutable.js";

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

/**
 * Element-scoped removal key (matches Go's removedKey). Tags are per-ADD,
 * not per-(element, add) — a multi-element add shares one tag — so
 * removals keyed by tag alone would leak across elements added together.
 */
export function removedKey(elem: string, tag: ORSetTag): string {
  return elem + "|" + tagKey(tag);
}

/** Removal check honoring both element-scoped and legacy tag-only keys. */
export function tagRemoved(
  removed: Record<string, boolean>,
  elem: string,
  tag: ORSetTag
): boolean {
  return Boolean(removed[removedKey(elem, tag)] || removed[tagKey(tag)]);
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
    if (hasActiveTags(key, tags, state.removed)) {
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
  elem: string,
  tags: ORSetTag[],
  removed: Record<string, boolean>
): boolean {
  for (const tag of tags) {
    if (!tagRemoved(removed, elem, tag)) {
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

/**
 * Compute a deterministic string key for an HLC (used as node map key).
 * MUST match Go's rgaNodeKey (HLC.String()) so state carried across
 * engines keys identically.
 */
function hlcKey(hlc: HLC): string {
  return hlcString(hlc);
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

  // Merge remote nodes. Same key = same node; tombstone wins if either
  // side is tombstoned (matches Go MergeList).
  for (const [key, remoteNode] of Object.entries(remote.nodes)) {
    const existing = merged.nodes[key];
    if (!existing) {
      merged.nodes[key] = { ...remoteNode };
    } else if (remoteNode.tombstone && !existing.tombstone) {
      merged.nodes[key] = { ...existing, tombstone: true };
    }
  }

  return merged;
}

/**
 * Ordered live nodes of an RGA list, tombstones skipped.
 *
 * Uses an explicit stack rather than recursion: sequential appends build a
 * LINEAR parent chain, so recursion depth equals list length and overflows
 * the stack in the low thousands.
 */
function walkList(state: RGAListState): RGANode[] {
  const nodes = Object.values(state.nodes);
  if (nodes.length === 0) return [];

  // Build children map: parent key → children.
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

  // Sort each sibling group by HLC descending (RGA insert-right semantics).
  for (const children of childrenMap.values()) {
    children.sort((a, b) => -hlcCompare(a.id, b.id));
  }

  const out: RGANode[] = [];
  const stack: RGANode[] = [];
  const roots = childrenMap.get(hlcKey({ ts: 0, c: 0, node: "" }));
  // Push reversed so the first sibling pops first.
  if (roots) for (let i = roots.length - 1; i >= 0; i--) stack.push(roots[i]);

  while (stack.length > 0) {
    const node = stack.pop()!;
    if (!node.tombstone) out.push(node);
    const children = childrenMap.get(hlcKey(node.id));
    if (children) for (let i = children.length - 1; i >= 0; i--) stack.push(children[i]);
  }

  return out;
}

/**
 * Resolve an RGA list to an ordered array of live (non-tombstoned) values.
 */
export function listElements(state: RGAListState): unknown[] {
  return walkList(state).map((n) => n.value);
}

/**
 * Return the ordered node IDs (HLCs) for live elements in an RGA list.
 * Order matches listElements().
 */
export function listNodeIds(state: RGAListState): HLC[] {
  return walkList(state).map((n) => n.id);
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
      case "text":
        result[key] = field.text_state ? textValue(field.text_state) : "";
        break;
      default:
        result[key] = field.value;
    }
  }

  return result;
}

// --- Field-Level Merge ---

/**
 * Merge two full FieldStates of the same type (state-based propagation).
 * Mirrors Go MergeEngine.MergeField.
 */
export function mergeFullFieldState(
  local: FieldState | null,
  remote: FieldState
): FieldState {
  if (!local) return remote;
  const newerMeta = hlcAfter(remote.hlc, local.hlc)
    ? { hlc: remote.hlc, node_id: remote.node_id }
    : { hlc: local.hlc, node_id: local.node_id };

  switch (remote.type) {
    case "lww":
      return hlcAfter(remote.hlc, local.hlc) ? { ...remote } : { ...local };
    case "counter":
      return {
        type: "counter",
        ...newerMeta,
        counter_state: mergeCounter(
          local.counter_state ?? newPNCounterState(),
          remote.counter_state ?? newPNCounterState()
        ),
      };
    case "set":
      return {
        type: "set",
        ...newerMeta,
        set_state: mergeSet(
          local.set_state ?? newORSetState(),
          remote.set_state ?? newORSetState()
        ),
      };
    case "list":
      return {
        type: "list",
        ...newerMeta,
        list_state: mergeListState(local.list_state, remote.list_state),
      };
    case "document":
      return {
        type: "document",
        ...newerMeta,
        doc_state: mergeDocumentState(local.doc_state, remote.doc_state),
      };
    case "text":
      return {
        type: "text",
        ...newerMeta,
        text_state: mergeText(local.text_state ?? newTextState(), remote.text_state ?? newTextState()),
      };
    default:
      return hlcAfter(remote.hlc, local.hlc) ? { ...remote } : { ...local };
  }
}

/**
 * Merge a ChangeRecord into existing FieldState.
 * Dispatches to the correct merge function based on CRDTType.
 * Mirrors Go crdt.ApplyChange.
 */
export function mergeFieldState(
  local: FieldState | null,
  change: ChangeRecord
): FieldState {
  // Full-state carrier: pure state-based merge.
  if (change.state) {
    return mergeFullFieldState(local, change.state);
  }

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
      // The delta is the sending node's CUMULATIVE totals (a snapshot),
      // merged max-per-node — idempotent under redelivery. Matches Go
      // ApplyChange; the store emits cumulative totals accordingly.
      if (change.counter_delta) {
        const remoteCounter = newPNCounterState();
        remoteCounter.inc[change.node_id] = change.counter_delta.inc;
        remoteCounter.dec[change.node_id] = change.counter_delta.dec;
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
      const localSet = local?.set_state ?? newORSetState();
      let entries = localSet.entries;
      let removed = localSet.removed;

      if (change.set_op) {
        const op = change.set_op;
        if (op.op === "add") {
          const newTag: ORSetTag = { node: change.node_id, hlc: change.hlc };
          for (const elem of op.elements) {
            const key = JSON.stringify(elem);
            entries = withEntry(entries, key, withAppended(entries[key], newTag));
          }
        } else if (op.op === "remove") {
          const keys: string[] = [];
          if (op.tags && op.tags.length > 0) {
            // Exact observed-remove: the op names the tags it saw, scoped
            // to the elements it removes.
            for (const elem of op.elements) {
              const key = JSON.stringify(elem);
              for (const t of op.tags) keys.push(removedKey(key, t));
            }
          } else {
            // Legacy remove: only tags older than the remove's HLC —
            // concurrent-or-newer adds survive (add-wins). Matches Go.
            for (const elem of op.elements) {
              const key = JSON.stringify(elem);
              for (const t of entries[key] ?? []) {
                if (hlcAfter(change.hlc, t.hlc)) keys.push(removedKey(key, t));
              }
            }
          }
          removed = withFlags(removed, keys);
        }
      }

      return {
        type: "set",
        hlc: change.hlc,
        node_id: change.node_id,
        set_state: entries === localSet.entries && removed === localSet.removed
          ? localSet
          : { entries, removed },
      };
    }

    case "list": {
      const localList = local?.list_state ?? newRGAListState();
      let nodes = localList.nodes;

      const tombstoneAt = (id: HLC): void => {
        const key = hlcKey(id);
        const existing = nodes[key];
        nodes = withEntry(nodes, key, existing
          ? { ...existing, tombstone: true }
          // Keep the tombstone even when the insert hasn't arrived:
          // a late insert must stay deleted (matches Go).
          : {
              id,
              node_id: change.node_id,
              parent_id: { ts: 0, c: 0, node: "" },
              value: undefined,
              tombstone: true,
            });
      };

      if (change.list_op) {
        const op = change.list_op;
        if (op.op === "insert" && op.node_id) {
          nodes = withEntry(nodes, hlcKey(op.node_id), {
            id: op.node_id,
            node_id: change.node_id,
            parent_id: op.parent_id ?? { ts: 0, c: 0, node: "" },
            value: op.value,
          });
        } else if (op.op === "delete" && op.node_id) {
          tombstoneAt(op.node_id);
        } else if (op.op === "move" && op.node_id) {
          // Move = tombstone the old id + re-insert under the new parent
          // with the op's HLC as the new id (matches Go).
          tombstoneAt(op.node_id);
          nodes = withEntry(nodes, hlcKey(change.hlc), {
            id: change.hlc,
            node_id: change.node_id,
            parent_id: op.parent_id ?? { ts: 0, c: 0, node: "" },
            value: op.value,
          });
        }
      }

      return {
        type: "list",
        hlc: change.hlc,
        node_id: change.node_id,
        list_state: nodes === localList.nodes ? localList : { nodes },
      };
    }

    case "document": {
      const localDoc = local?.doc_state ?? newDocumentCRDTState();
      // Document changes carry {path, value} in the record value (the
      // path is INSIDE the nested document; change.field is the column).
      const payload = change.value as { path?: string; value?: unknown } | undefined;
      const path = payload?.path;
      if (path) {
        if (change.tombstone) {
          // LWW-guarded path delete (matches Go applyDocumentChange).
          const existing = localDoc.fields[path];
          if (existing && hlcAfter(change.hlc, existing.hlc)) {
            const prefix = path + ".";
            return {
              type: "document",
              hlc: change.hlc,
              node_id: change.node_id,
              doc_state: {
                fields: withoutKeys(
                  localDoc.fields,
                  (k) => k === path || k.startsWith(prefix)
                ),
              },
            };
          }
        } else {
          const remoteDoc = newDocumentCRDTState();
          remoteDoc.fields[path] = {
            type: "lww",
            hlc: change.hlc,
            node_id: change.node_id,
            value: payload?.value,
          };
          return {
            type: "document",
            hlc: change.hlc,
            node_id: change.node_id,
            doc_state: mergeDocumentState(localDoc, remoteDoc),
          };
        }
      }
      return {
        type: "document",
        hlc: change.hlc,
        node_id: change.node_id,
        doc_state: localDoc,
      };
    }

    case "text": {
      const localText = local?.text_state ?? newTextState();
      const nextText = change.text_op
        ? applyTextOpTo(localText, change.text_op, change.node_id, change.hlc)
        : localText;
      // Stamp with whichever of local/change is newer (Go pickNewer
      // parity) — a redelivered older op must not regress the field clock.
      const keepLocal = local && hlcAfter(local.hlc, change.hlc);
      return {
        type: "text",
        hlc: keepLocal ? local.hlc : change.hlc,
        node_id: keepLocal ? local.node_id : change.node_id,
        text_state: nextText,
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
