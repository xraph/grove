/**
 * Horizon-based tombstone compaction. Port of crdt/compact.go, written
 * pure so it composes with copy-on-write state.
 *
 * The horizon is a stability floor the CALLER guarantees: every replica
 * has observed all operations with HLC < horizon, and no in-flight
 * operation references addresses older than it. Passing a horizon that
 * does not hold will diverge replicas.
 */

import type {
  RGAListState, ORSetState, ORSetTag, TextState, TextFragment,
  DocumentState, FieldState, HLC,
} from "./types.js";
import { hlcAfter, hlcIsZero, hlcString } from "./hlc.js";
import { removedKey, tagKey } from "./merge.js";

/**
 * Drop tombstoned LEAF nodes older than the horizon, cascading until no
 * more can be dropped. Survivor ordering is untouched by construction: a
 * node with children is never removed, so no anchor is ever orphaned.
 */
export function compactListState(
  state: RGAListState,
  before: HLC
): { state: RGAListState; dropped: number } {
  if (hlcIsZero(before) || Object.keys(state.nodes).length === 0) {
    return { state, dropped: 0 };
  }

  let nodes = state.nodes;
  let dropped = 0;

  for (;;) {
    // Recompute anchors each round: dropping a leaf may expose its parent.
    const hasChild = new Set<string>();
    for (const node of Object.values(nodes)) {
      hasChild.add(hlcString(node.parent_id));
    }

    const survivors: Record<string, typeof nodes[string]> = {};
    let droppedThisRound = 0;
    for (const [key, node] of Object.entries(nodes)) {
      if (node.tombstone && !hasChild.has(key) && hlcAfter(before, node.id)) {
        droppedThisRound++;
        continue;
      }
      survivors[key] = node;
    }

    if (droppedThisRound === 0) break;
    nodes = survivors;
    dropped += droppedThisRound;
  }

  return dropped === 0 ? { state, dropped: 0 } : { state: { nodes }, dropped };
}

/** True when this tag has been observed-removed for this element. */
function tagRemoved(
  removed: Record<string, boolean>,
  elem: string,
  tag: ORSetTag
): boolean {
  return Boolean(removed[removedKey(elem, tag)] || removed[tagKey(tag)]);
}

/**
 * Drop observed-removed tags older than the horizon along with their
 * element-scoped removal markers, pruning entries left tagless.
 *
 * Only the element-scoped marker is consumed. A legacy tag-only marker is
 * SHARED evidence — a multi-element add shares one tag — so deleting it
 * would resurrect sibling elements that still rely on it, and which
 * sibling would depend on iteration order. The tiny legacy marker stays.
 */
export function compactSetState(
  state: ORSetState,
  before: HLC
): { state: ORSetState; dropped: number } {
  if (hlcIsZero(before)) return { state, dropped: 0 };

  const entries: Record<string, ORSetTag[]> = {};
  const removed = { ...state.removed };
  let dropped = 0;

  for (const [elem, tags] of Object.entries(state.entries)) {
    const kept: ORSetTag[] = [];
    for (const tag of tags) {
      if (tagRemoved(state.removed, elem, tag) && hlcAfter(before, tag.hlc)) {
        delete removed[removedKey(elem, tag)];
        dropped++;
        continue;
      }
      kept.push(tag);
    }
    if (kept.length > 0) entries[elem] = kept;
  }

  return dropped === 0 ? { state, dropped: 0 } : { state: { entries, removed }, dropped };
}

/**
 * Skeletonize tombstoned fragments whose origin is older than the horizon:
 * content is freed and adjacent skeletons coalesce, but addresses are
 * PRESERVED so every cursor anchor stays resolvable.
 */
export function compactTextState(
  state: TextState,
  before: HLC
): { state: TextState; dropped: number } {
  if (hlcIsZero(before)) return { state, dropped: 0 };

  const frags: Record<string, TextFragment[]> = {};
  let dropped = 0;

  for (const [key, list] of Object.entries(state.frags)) {
    const skeletonized = list.map((f) => {
      if (f.tombstone && f.content !== "" && hlcAfter(before, f.origin)) {
        dropped++;
        const { attrs: _attrs, ...rest } = f;
        return { ...rest, content: "" };
      }
      return f;
    });

    // Coalesce adjacent skeletons (contiguous, both tombstoned and empty).
    const out: TextFragment[] = [];
    for (const f of skeletonized) {
      const prev = out[out.length - 1];
      if (
        prev && prev.tombstone && prev.content === "" &&
        f.tombstone && f.content === "" &&
        prev.start + prev.length === f.start
      ) {
        out[out.length - 1] = { ...prev, length: prev.length + f.length };
        dropped++;
        continue;
      }
      out.push(f);
    }
    frags[key] = out;
  }

  return dropped === 0 ? { state, dropped: 0 } : { state: { frags }, dropped };
}

/** Compact every compactable field of one document. */
export function compactDocument(
  doc: DocumentState,
  before: HLC
): { doc: DocumentState; dropped: number } {
  if (hlcIsZero(before)) return { doc, dropped: 0 };

  const fields: Record<string, FieldState> = {};
  let dropped = 0;

  for (const [name, fs] of Object.entries(doc.fields)) {
    if (fs.type === "list" && fs.list_state) {
      const r = compactListState(fs.list_state, before);
      dropped += r.dropped;
      fields[name] = r.dropped ? { ...fs, list_state: r.state } : fs;
    } else if (fs.type === "set" && fs.set_state) {
      const r = compactSetState(fs.set_state, before);
      dropped += r.dropped;
      fields[name] = r.dropped ? { ...fs, set_state: r.state } : fs;
    } else if (fs.type === "text" && fs.text_state) {
      const r = compactTextState(fs.text_state, before);
      dropped += r.dropped;
      fields[name] = r.dropped ? { ...fs, text_state: r.state } : fs;
    } else {
      fields[name] = fs;
    }
  }

  return dropped === 0 ? { doc, dropped: 0 } : { doc: { ...doc, fields }, dropped };
}
