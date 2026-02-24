import { describe, it, expect, vi } from "vitest";
import { StaticAuthProvider } from "../auth.js";
import type { AuthProvider } from "../types.js";

// ---------------------------------------------------------------------------
// StaticAuthProvider
// ---------------------------------------------------------------------------

describe("StaticAuthProvider", () => {
  it("returns configured headers", () => {
    const auth = new StaticAuthProvider({ Authorization: "Bearer abc" });
    expect(auth.getHeaders()).toEqual({ Authorization: "Bearer abc" });
  });

  it("returns a copy each time (not the same reference)", () => {
    const auth = new StaticAuthProvider({ "X-Key": "val" });
    const a = auth.getHeaders();
    const b = auth.getHeaders();
    expect(a).toEqual(b);
    expect(a).not.toBe(b);
  });

  it("mutations to returned object do not affect provider", () => {
    const auth = new StaticAuthProvider({ "X-Key": "val" });
    const headers = auth.getHeaders();
    headers["X-Key"] = "mutated";
    expect(auth.getHeaders()["X-Key"]).toBe("val");
  });

  it("returns empty object when constructed with empty headers", () => {
    const auth = new StaticAuthProvider({});
    expect(auth.getHeaders()).toEqual({});
  });

  it("implements AuthProvider interface", () => {
    const auth: AuthProvider = new StaticAuthProvider({ key: "value" });
    expect(auth.getHeaders()).toEqual({ key: "value" });
  });
});

// ---------------------------------------------------------------------------
// Dynamic AuthProvider (mock)
// ---------------------------------------------------------------------------

describe("Dynamic AuthProvider", () => {
  it("supports sync getHeaders()", () => {
    const auth: AuthProvider = {
      getHeaders: () => ({ Authorization: "Bearer sync-token" }),
    };
    expect(auth.getHeaders()).toEqual({ Authorization: "Bearer sync-token" });
  });

  it("supports async getHeaders()", async () => {
    const auth: AuthProvider = {
      getHeaders: async () => ({ Authorization: "Bearer async-token" }),
    };
    const headers = await Promise.resolve(auth.getHeaders());
    expect(headers).toEqual({ Authorization: "Bearer async-token" });
  });

  it("can be called multiple times with vi.fn()", () => {
    let callCount = 0;
    const auth: AuthProvider = {
      getHeaders: vi.fn(() => {
        callCount++;
        return { "X-Request-ID": String(callCount) };
      }),
    };

    const h1 = auth.getHeaders();
    const h2 = auth.getHeaders();
    expect(h1).toEqual({ "X-Request-ID": "1" });
    expect(h2).toEqual({ "X-Request-ID": "2" });
    expect(auth.getHeaders).toHaveBeenCalledTimes(2);
  });

  it("supports token refresh pattern", async () => {
    let token = "initial";
    const auth: AuthProvider = {
      getHeaders: async () => {
        // Simulate token refresh.
        token = `refreshed-${Date.now()}`;
        return { Authorization: `Bearer ${token}` };
      },
    };

    const h1 = await Promise.resolve(auth.getHeaders());
    expect(h1.Authorization).toContain("Bearer refreshed-");
  });
});
