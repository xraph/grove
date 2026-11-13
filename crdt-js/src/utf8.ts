/**
 * Compare strings by their UTF-8 bytes, the order Go's sort.Strings uses.
 * Plain JS comparison orders UTF-16 code units, which puts characters
 * above U+FFFF before U+E000 to U+FFFF.
 */
export function compareUTF8(a: string, b: string): number {
  const n = Math.min(a.length, b.length);
  for (let i = 0; i < n; i++) {
    const x = a.charCodeAt(i);
    const y = b.charCodeAt(i);
    if (x !== y) return utf8Rank(x) - utf8Rank(y);
  }
  return a.length - b.length;
}

// Shift surrogates above U+E000 to U+FFFF so code units compare in code
// point order, which is UTF-8 byte order.
function utf8Rank(unit: number): number {
  if (unit >= 0xd800 && unit <= 0xdfff) return unit + 0x2000;
  if (unit >= 0xe000) return unit - 0x800;
  return unit;
}
