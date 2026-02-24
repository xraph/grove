import { describe, it, expect } from "vitest";
import {
  HLC_ZERO,
  hlcCompare,
  hlcAfter,
  hlcIsZero,
  hlcMax,
  hlcString,
  HybridClock,
} from "../hlc.js";
import type { HLC } from "../types.js";

// ---------------------------------------------------------------------------
// hlcCompare
// ---------------------------------------------------------------------------

describe("hlcCompare", () => {
  it("returns -1 when a.ts < b.ts", () => {
    const a: HLC = { ts: 100, c: 0, node: "a" };
    const b: HLC = { ts: 200, c: 0, node: "b" };
    expect(hlcCompare(a, b)).toBe(-1);
  });

  it("returns 1 when a.ts > b.ts", () => {
    const a: HLC = { ts: 200, c: 0, node: "a" };
    const b: HLC = { ts: 100, c: 0, node: "b" };
    expect(hlcCompare(a, b)).toBe(1);
  });

  it("returns 0 for identical HLCs", () => {
    const a: HLC = { ts: 100, c: 1, node: "a" };
    expect(hlcCompare(a, a)).toBe(0);
  });

  it("breaks tie by counter when timestamps are equal", () => {
    const a: HLC = { ts: 100, c: 1, node: "a" };
    const b: HLC = { ts: 100, c: 2, node: "b" };
    expect(hlcCompare(a, b)).toBe(-1);
  });

  it("breaks tie by node ID when timestamp and counter are equal", () => {
    const a: HLC = { ts: 100, c: 1, node: "a" };
    const b: HLC = { ts: 100, c: 1, node: "b" };
    expect(hlcCompare(a, b)).toBe(-1);
  });

  it("returns 1 when node a > node b with same ts and counter", () => {
    const a: HLC = { ts: 100, c: 1, node: "b" };
    const b: HLC = { ts: 100, c: 1, node: "a" };
    expect(hlcCompare(a, b)).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// hlcAfter
// ---------------------------------------------------------------------------

describe("hlcAfter", () => {
  it("returns true when a is strictly after b", () => {
    const a: HLC = { ts: 200, c: 0, node: "a" };
    const b: HLC = { ts: 100, c: 0, node: "a" };
    expect(hlcAfter(a, b)).toBe(true);
  });

  it("returns false when a equals b", () => {
    const a: HLC = { ts: 100, c: 0, node: "a" };
    expect(hlcAfter(a, { ...a })).toBe(false);
  });

  it("returns false when a is before b", () => {
    const a: HLC = { ts: 100, c: 0, node: "a" };
    const b: HLC = { ts: 200, c: 0, node: "a" };
    expect(hlcAfter(a, b)).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// hlcIsZero
// ---------------------------------------------------------------------------

describe("hlcIsZero", () => {
  it("returns true for HLC_ZERO", () => {
    expect(hlcIsZero(HLC_ZERO)).toBe(true);
  });

  it("returns false when ts is non-zero", () => {
    expect(hlcIsZero({ ts: 1, c: 0, node: "" })).toBe(false);
  });

  it("returns false when counter is non-zero", () => {
    expect(hlcIsZero({ ts: 0, c: 1, node: "" })).toBe(false);
  });

  it("returns false when node is non-empty", () => {
    expect(hlcIsZero({ ts: 0, c: 0, node: "x" })).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// hlcMax
// ---------------------------------------------------------------------------

describe("hlcMax", () => {
  it("returns the greater HLC", () => {
    const a: HLC = { ts: 100, c: 0, node: "a" };
    const b: HLC = { ts: 200, c: 0, node: "b" };
    expect(hlcMax(a, b)).toBe(b);
  });

  it("returns b when they are equal", () => {
    const a: HLC = { ts: 100, c: 0, node: "a" };
    const b: HLC = { ts: 100, c: 0, node: "a" };
    // hlcMax returns a if hlcAfter(a,b), else b. Equal => returns b.
    expect(hlcMax(a, b)).toBe(b);
  });

  it("returns the one with higher node on tiebreak", () => {
    const a: HLC = { ts: 100, c: 0, node: "a" };
    const b: HLC = { ts: 100, c: 0, node: "b" };
    // "b" > "a" => hlcAfter(b,a) is true, hlcAfter(a,b) is false => returns b
    expect(hlcMax(a, b)).toBe(b);
  });
});

// ---------------------------------------------------------------------------
// hlcString
// ---------------------------------------------------------------------------

describe("hlcString", () => {
  it("produces deterministic Go-compatible string", () => {
    expect(hlcString({ ts: 12345, c: 7, node: "n1" })).toBe(
      "HLC{ts:12345 c:7 node:n1}"
    );
  });

  it("handles HLC_ZERO", () => {
    expect(hlcString(HLC_ZERO)).toBe("HLC{ts:0 c:0 node:}");
  });
});

// ---------------------------------------------------------------------------
// HybridClock
// ---------------------------------------------------------------------------

describe("HybridClock", () => {
  describe("constructor", () => {
    it("initializes with given nodeID", () => {
      const clock = new HybridClock("node-1");
      expect(clock.nodeID).toBe("node-1");
    });
  });

  describe("now()", () => {
    it("returns HLC with correct nodeID", () => {
      const clock = new HybridClock("node-1", { nowFn: () => 1000 });
      expect(clock.now().node).toBe("node-1");
    });

    it("returns monotonically increasing HLCs", () => {
      const clock = new HybridClock("node-1");
      const h1 = clock.now();
      const h2 = clock.now();
      expect(hlcAfter(h2, h1)).toBe(true);
    });

    it("increments counter when physical clock has not advanced", () => {
      const clock = new HybridClock("node-1", { nowFn: () => 1000 });
      const h1 = clock.now();
      const h2 = clock.now();
      expect(h1.c).toBe(0);
      expect(h2.c).toBe(1);
      expect(h1.ts).toBe(h2.ts);
    });

    it("resets counter when physical clock advances", () => {
      let time = 1000;
      const clock = new HybridClock("node-1", { nowFn: () => time });
      clock.now(); // c=0
      clock.now(); // c=1
      time = 2000;
      const h3 = clock.now();
      expect(h3.c).toBe(0);
      expect(h3.ts).toBe(2000 * 1_000_000);
    });

    it("converts milliseconds to nanoseconds", () => {
      const clock = new HybridClock("node-1", { nowFn: () => 1000 });
      const h = clock.now();
      expect(h.ts).toBe(1000 * 1_000_000);
    });
  });

  describe("update()", () => {
    it("advances past remote when physical clock is ahead of both", () => {
      let time = 2000;
      const clock = new HybridClock("node-1", { nowFn: () => time });
      clock.now(); // set last to ts=2000*1e6

      time = 3000;
      const remote: HLC = { ts: 1000 * 1_000_000, c: 5, node: "node-2" };
      clock.update(remote);

      // Physical clock (3000ms=3e9ns) > both last (2e9ns) and remote (1e9ns)
      // So last should be set to physical, c=0
      const after = clock.now();
      // now() sees physicalNow (3e9) > last.ts (3e9 from update) => depends on timing
      // The key check: after is causally after both local and remote
      expect(hlcAfter(after, remote)).toBe(true);
    });

    it("merges when local and remote have same timestamp", () => {
      const clock = new HybridClock("node-1", { nowFn: () => 1000 });
      clock.now(); // last = {ts:1e9, c:0, node:"node-1"}

      const remote: HLC = { ts: 1000 * 1_000_000, c: 10, node: "node-2" };
      clock.update(remote);

      // Same ts => counter = max(0, 10) + 1 = 11
      const after = clock.now();
      expect(hlcAfter(after, remote)).toBe(true);
    });

    it("increments local counter when local is ahead of remote", () => {
      const clock = new HybridClock("node-1", { nowFn: () => 1000 });
      clock.now(); // last = {ts:1e9, c:0}
      clock.now(); // last = {ts:1e9, c:1}

      const remote: HLC = { ts: 500 * 1_000_000, c: 0, node: "node-2" };
      clock.update(remote);

      // local.ts (1e9) > remote.ts (5e8) => increment local counter
      const after = clock.now();
      expect(hlcAfter(after, remote)).toBe(true);
    });

    it("adopts remote timestamp when remote is ahead", () => {
      const clock = new HybridClock("node-1", { nowFn: () => 1000 });
      clock.now(); // last = {ts:1e9, c:0}

      const remote: HLC = {
        ts: 2000 * 1_000_000,
        c: 5,
        node: "node-2",
      };
      clock.update(remote);

      const after = clock.now();
      expect(hlcAfter(after, remote)).toBe(true);
    });

    it("ensures now() is causally after update(remote)", () => {
      const clock = new HybridClock("node-1");
      const local = clock.now();

      const remote: HLC = {
        ts: local.ts + 1_000_000,
        c: 5,
        node: "node-2",
      };
      clock.update(remote);

      const after = clock.now();
      expect(hlcAfter(after, remote)).toBe(true);
    });

    it("clamps remote timestamp to maxDrift", () => {
      const fixedTime = 1000; // 1000ms
      const clock = new HybridClock("node-1", {
        maxDriftMs: 1000, // 1 second
        nowFn: () => fixedTime,
      });

      // Remote clock far in the future (10 seconds ahead in ns)
      const remote: HLC = {
        ts: (fixedTime + 10000) * 1_000_000,
        c: 0,
        node: "node-2",
      };
      clock.update(remote);

      const after = clock.now();
      const maxAllowedNs = (fixedTime + 1000) * 1_000_000;
      // The timestamp should be clamped to maxDrift
      expect(after.ts).toBeLessThanOrEqual(maxAllowedNs);
    });

    it("handles remote with higher counter at same timestamp", () => {
      const clock = new HybridClock("node-1", { nowFn: () => 1000 });
      clock.now(); // last = {ts:1e9, c:0}

      const remote: HLC = {
        ts: 1000 * 1_000_000,
        c: 10,
        node: "node-2",
      };
      clock.update(remote);

      // Same ts => counter = max(0, 10) + 1 = 11
      const after = clock.now();
      // after should be at ts=1e9, c >= 12 (update set c=11, now() increments to 12)
      expect(after.c).toBeGreaterThanOrEqual(12);
    });
  });
});
