/**
 * Shallow structural-sharing helpers for copy-on-write CRDT state.
 *
 * Every helper returns a NEW container and leaves the input untouched.
 * Untouched substructure is shared by reference, never cloned — that
 * sharing is what makes identity-keyed snapshot caching correct.
 */

/** Record with one key replaced. The other values are shared by reference. */
export function withEntry<T>(
  rec: Record<string, T>,
  key: string,
  value: T
): Record<string, T> {
  return { ...rec, [key]: value };
}

/** Record with every key matching `drop` removed. Survivors are shared. */
export function withoutKeys<T>(
  rec: Record<string, T>,
  drop: (key: string) => boolean
): Record<string, T> {
  const out: Record<string, T> = {};
  for (const k of Object.keys(rec)) {
    if (!drop(k)) out[k] = rec[k];
  }
  return out;
}

/** Array with one element appended. */
export function withAppended<T>(arr: readonly T[] | undefined, value: T): T[] {
  return arr ? [...arr, value] : [value];
}

/** Record with several keys set to `true`. Returns the input if `keys` is empty. */
export function withFlags(
  rec: Record<string, boolean>,
  keys: readonly string[]
): Record<string, boolean> {
  if (keys.length === 0) return rec;
  const out = { ...rec };
  for (const k of keys) out[k] = true;
  return out;
}
