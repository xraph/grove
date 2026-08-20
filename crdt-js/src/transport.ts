/**
 * Built-in HTTP transport for CRDT sync operations.
 *
 * Extracted from CRDTClient to allow pluggable transport backends.
 * HttpTransport handles pull/push over HTTP POST.
 * HttpStreamTransport extends it with SSE streaming via CRDTStream.
 */

import type {
  Transport,
  StreamTransport,
  StreamSubscription,
  StreamConfig,
  PullRequest,
  PullResponse,
  PushRequest,
  PushResponse,
  AuthProvider,
  PresenceUpdate,
  PresenceState,
  PresenceSnapshot,
} from "./types.js";
import { TransportError } from "./errors.js";
import { CRDTStream } from "./stream.js";
import { Backoff } from "./backoff.js";
import type { BackoffOptions } from "./backoff.js";

// Re-export TransportError for convenience.
export { TransportError } from "./errors.js";

/** Configuration for HttpTransport. */
export interface HttpTransportConfig {
  /** Base URL of the CRDT sync server. */
  baseURL: string;
  /** Custom fetch implementation (default: globalThis.fetch). */
  fetch?: typeof fetch;
  /** Static headers to include in all requests. */
  headers?: Record<string, string>;
  /** Auth provider for dynamic header injection. */
  auth?: AuthProvider;
  /** Per-request timeout in ms (default: 30000). 0 disables. */
  timeout?: number;
  /** Retry attempts for retryable failures (default: 2). */
  retries?: number;
  /** Backoff schedule between retries. */
  backoff?: BackoffOptions;
}

/**
 * HTTP transport for CRDT pull/push operations.
 *
 * Sends JSON POST requests to /pull and /push endpoints.
 * Supports static headers and dynamic auth via AuthProvider.
 */
export class HttpTransport implements Transport {
  protected baseURL: string;
  protected fetchImpl: typeof fetch;
  protected headers: Record<string, string>;
  protected auth?: AuthProvider;
  protected timeout: number;
  protected retries: number;
  protected backoffOpts: BackoffOptions;

  constructor(config: HttpTransportConfig) {
    this.baseURL = config.baseURL.replace(/\/+$/, "");
    this.fetchImpl = config.fetch ?? globalThis.fetch.bind(globalThis);
    this.headers = config.headers ?? {};
    this.auth = config.auth;
    this.timeout = config.timeout ?? 30_000;
    this.retries = config.retries ?? 2;
    this.backoffOpts = config.backoff ?? {};
  }

  async pull(req: PullRequest): Promise<PullResponse> {
    return this.request<PullResponse>("/pull", req);
  }

  async push(req: PushRequest): Promise<PushResponse> {
    return this.request<PushResponse>("/push", req);
  }

  async updatePresence(update: PresenceUpdate): Promise<void> {
    await this.request<unknown>("/presence", update);
  }

  async getPresence(topic: string): Promise<PresenceState[]> {
    const authHeaders = this.auth
      ? await Promise.resolve(this.auth.getHeaders())
      : {};

    const url = `${this.baseURL}/presence?topic=${encodeURIComponent(topic)}`;
    const response = await fetchWithTimeout(
      this.fetchImpl,
      url,
      {
        method: "GET",
        headers: {
          ...this.headers,
          ...authHeaders,
        },
      },
      this.timeout
    );

    if (!response.ok) {
      const text = await response.text().catch(() => "");
      throw new TransportError(
        `CRDT /presence returned ${response.status}: ${text}`,
        response.status
      );
    }

    const snapshot = await parseJsonOrEmpty<PresenceSnapshot>(response);
    return snapshot.states;
  }

  protected async request<T>(path: string, body: unknown): Promise<T> {
    const backoff = new Backoff(this.backoffOpts);
    let lastError: unknown;

    for (let attempt = 0; attempt <= this.retries; attempt++) {
      if (attempt > 0) {
        await new Promise((r) => setTimeout(r, backoff.next()));
      }

      const authHeaders = this.auth
        ? await Promise.resolve(this.auth.getHeaders())
        : {};

      try {
        const response = await fetchWithTimeout(
          this.fetchImpl,
          `${this.baseURL}${path}`,
          {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              Accept: "application/json",
              ...this.headers,
              ...authHeaders,
            },
            body: JSON.stringify(body),
          },
          this.timeout
        );

        if (!response.ok) {
          const text = await response.text().catch(() => "");
          const err = new TransportError(
            `CRDT ${path} returned ${response.status}: ${text}`,
            response.status
          );
          if (!isRetryableStatus(response.status)) throw err;
          lastError = err;
          continue;
        }

        return (await parseJsonOrEmpty<T>(response));
      } catch (err) {
        // A non-retryable TransportError was thrown deliberately above.
        if (err instanceof TransportError && !isRetryableStatus(err.statusCode ?? 0)) {
          throw err;
        }
        lastError = err;
      }
    }

    throw lastError instanceof Error
      ? lastError
      : new TransportError(`CRDT ${path} failed after ${this.retries + 1} attempts`);
  }
}

/** 5xx, 429, and 408 are worth retrying; other 4xx are the caller's fault. */
function isRetryableStatus(status: number): boolean {
  return status >= 500 || status === 429 || status === 408;
}

/** Fetch with an AbortSignal-backed timeout. */
async function fetchWithTimeout(
  fetchImpl: typeof fetch,
  url: string,
  init: RequestInit,
  timeout: number
): Promise<Response> {
  if (timeout <= 0) return fetchImpl(url, init);
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeout);
  try {
    return await fetchImpl(url, { ...init, signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Parse a JSON response, tolerating an empty body.
 *
 * The presence endpoint answers 204 No Content, and response.json() throws
 * on an empty body.
 */
async function parseJsonOrEmpty<T>(response: Response): Promise<T> {
  if (response.status === 204) return undefined as T;
  const text = await response.text();
  if (text.trim() === "") return undefined as T;
  return JSON.parse(text) as T;
}

/**
 * HTTP transport with SSE streaming support.
 *
 * Extends HttpTransport with subscribe() for real-time change propagation.
 */
export class HttpStreamTransport extends HttpTransport implements StreamTransport {
  subscribe(config: StreamConfig): StreamSubscription {
    return new CRDTStream(
      this.baseURL,
      config,
      this.headers,
      this.fetchImpl,
      this.auth
    );
  }
}

/**
 * Type guard to check if a Transport also supports streaming.
 */
export function isStreamTransport(
  transport: Transport | undefined
): transport is StreamTransport {
  return (
    transport !== undefined &&
    typeof (transport as StreamTransport).subscribe === "function"
  );
}
