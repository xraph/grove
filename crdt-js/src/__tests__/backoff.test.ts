import { describe, it, expect } from "vitest";
import { Backoff } from "../backoff.js";

describe("Backoff", () => {
  it("grows geometrically without jitter", () => {
    const b = new Backoff({ initialDelay: 100, factor: 2, maxDelay: 10_000, jitter: false });
    expect(b.next()).toBe(100);
    expect(b.next()).toBe(200);
    expect(b.next()).toBe(400);
    expect(b.next()).toBe(800);
  });

  it("clamps at maxDelay", () => {
    const b = new Backoff({ initialDelay: 1000, factor: 10, maxDelay: 5000, jitter: false });
    b.next(); b.next();
    expect(b.next()).toBe(5000);
    expect(b.next()).toBe(5000);
  });

  it("full jitter keeps the delay within [0, ceiling]", () => {
    // random() === 1 yields the ceiling; random() === 0 yields zero.
    const hi = new Backoff({ initialDelay: 100, factor: 2, jitter: true, random: () => 1 });
    const lo = new Backoff({ initialDelay: 100, factor: 2, jitter: true, random: () => 0 });
    expect(hi.next()).toBe(100);
    expect(lo.next()).toBe(0);
  });

  it("reset returns to the initial delay", () => {
    const b = new Backoff({ initialDelay: 100, factor: 2, jitter: false });
    b.next(); b.next(); b.next();
    expect(b.attempt).toBe(3);
    b.reset();
    expect(b.attempt).toBe(0);
    expect(b.next()).toBe(100);
  });
});
