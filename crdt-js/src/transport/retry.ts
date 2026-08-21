/**
 * Retry decorator for any Transport.
 *
 * Retry lives here rather than inside HttpTransport so that a WebSocket
 * transport, or any transport a consumer plugs in, gets the same
 * resilience. Timeout stays in HttpTransport because real cancellation
 * needs AbortSignal, which is fetch-specific.
 */

import type {
  Transport, PullRequest, PullResponse,
  PushRequest, PushResponse, PresenceUpdate, PresenceState,
} from "../types.js";
import { CRDTError } from "../errors.js";
import { Backoff } from "../backoff.js";
import type { BackoffOptions } from "../backoff.js";

export interface RetryOptions {
  /** Retry attempts after the first try (default: 2). */
  retries?: number;
  /** Backoff schedule between attempts. */
  backoff?: BackoffOptions;
  /** Override which errors are worth retrying. */
  isRetryable?: (err: unknown) => boolean;
}

/**
 * Default policy: honor CRDTError.retryable; retry any other Error up to
 * the configured `retries` count (same schedule as everything else — not
 * a special one-shot case).
 */
function defaultIsRetryable(err: unknown): boolean {
  if (err instanceof CRDTError) return err.retryable;
  return err instanceof Error;
}

async function runWithRetry<T>(
  op: () => Promise<T>,
  retries: number,
  backoffOpts: BackoffOptions | undefined,
  isRetryable: (err: unknown) => boolean
): Promise<T> {
  const backoff = new Backoff(backoffOpts);
  let lastError: unknown;

  for (let attempt = 0; attempt <= retries; attempt++) {
    if (attempt > 0) {
      await new Promise((r) => setTimeout(r, backoff.next()));
    }
    try {
      return await op();
    } catch (err) {
      if (!isRetryable(err)) throw err;
      lastError = err;
    }
  }
  throw lastError;
}

/**
 * Wrap a transport so its pull/push (and presence, when supported) retry
 * on retryable failures. Streaming is delegated untouched — a live
 * subscription has its own reconnect loop, and retrying subscribe() would
 * fight it.
 *
 * Everything else on the transport is forwarded as-is, so a wrapped
 * transport keeps the members that are not part of the `Transport`
 * interface: `withRetry(ws).close()` reaches WebSocketTransport.close().
 * That is why the return type can honestly stay `T`.
 *
 * Note that `HttpTransport` already retries internally (its own `retries`
 * option, default 2). Wrapping one in `withRetry` stacks the two schedules
 * and multiplies the attempt count — 2 and 2 becomes up to 9 requests per
 * call — so set `retries: 0` on one of the layers unless you want that.
 */
export function withRetry<T extends Transport>(
  inner: T,
  opts?: RetryOptions
): T {
  const retries = opts?.retries ?? 2;
  const isRetryable = opts?.isRetryable ?? defaultIsRetryable;
  const run = <R>(op: () => Promise<R>) =>
    runWithRetry(op, retries, opts?.backoff, isRetryable);

  // Only these members are replaced; a Map (not an object literal) so an
  // inherited name like "toString" or "constructor" can never be mistaken
  // for an override.
  const overrides = new Map<PropertyKey, unknown>([
    ["pull", (req: PullRequest): Promise<PullResponse> => run(() => inner.pull(req))],
    ["push", (req: PushRequest): Promise<PushResponse> => run(() => inner.push(req))],
  ]);

  // Optional members must stay ABSENT when the inner transport lacks them:
  // CRDTClient tests `if (!this.transport.updatePresence)` to decide whether
  // presence is supported, so an always-present stub would claim support the
  // inner transport does not have. Proxying gives that for free — an absent
  // member on `inner` reads as undefined through the proxy — but the retry
  // wrappers themselves still have to be installed conditionally.
  if (inner.updatePresence) {
    overrides.set("updatePresence", (u: PresenceUpdate): Promise<void> =>
      run(() => inner.updatePresence!(u)));
  }
  if (inner.getPresence) {
    overrides.set("getPresence", (topic: string): Promise<PresenceState[]> =>
      run(() => inner.getPresence!(topic)));
  }

  // Bound forwards are cached so repeated reads of the same method return
  // the same function reference, the way a plain object would.
  const bound = new Map<PropertyKey, unknown>();

  return new Proxy(inner, {
    get(target, prop) {
      const override = overrides.get(prop);
      if (override !== undefined) return override;

      if (bound.has(prop)) return bound.get(prop);
      const value = Reflect.get(target, prop, target) as unknown;
      // Bind to the real instance rather than the proxy: methods that
      // touch private state (or `#`-private fields) must not see a proxy
      // as `this`. Streaming rides this path — `subscribe` is forwarded
      // untouched, so `isStreamTransport` still holds.
      if (typeof value === "function") {
        const fn = (value as (...args: unknown[]) => unknown).bind(target);
        bound.set(prop, fn);
        return fn;
      }
      return value;
    },
  });
}
