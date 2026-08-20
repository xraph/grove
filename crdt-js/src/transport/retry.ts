/**
 * Retry decorator for any Transport.
 *
 * Retry lives here rather than inside HttpTransport so that a WebSocket
 * transport, or any transport a consumer plugs in, gets the same
 * resilience. Timeout stays in HttpTransport because real cancellation
 * needs AbortSignal, which is fetch-specific.
 */

import type {
  Transport, StreamTransport, PullRequest, PullResponse,
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
 */
export function withRetry<T extends Transport>(
  inner: T,
  opts?: RetryOptions
): T {
  const retries = opts?.retries ?? 2;
  const isRetryable = opts?.isRetryable ?? defaultIsRetryable;
  const run = <R>(op: () => Promise<R>) =>
    runWithRetry(op, retries, opts?.backoff, isRetryable);

  const wrapped: Transport = {
    pull: (req: PullRequest): Promise<PullResponse> => run(() => inner.pull(req)),
    push: (req: PushRequest): Promise<PushResponse> => run(() => inner.push(req)),
  };

  // Optional members must stay ABSENT when the inner transport lacks them:
  // CRDTClient tests `if (!this.transport.updatePresence)` to decide whether
  // presence is supported, so an always-present stub would claim support the
  // inner transport does not have.
  if (inner.updatePresence) {
    wrapped.updatePresence = (u: PresenceUpdate): Promise<void> =>
      run(() => inner.updatePresence!(u));
  }
  if (inner.getPresence) {
    wrapped.getPresence = (topic: string): Promise<PresenceState[]> =>
      run(() => inner.getPresence!(topic));
  }

  const streaming = inner as unknown as StreamTransport;
  if (typeof streaming.subscribe === "function") {
    (wrapped as unknown as StreamTransport).subscribe = (config) =>
      streaming.subscribe(config);
  }

  return wrapped as T;
}
