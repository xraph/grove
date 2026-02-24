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
} from "./types.js";
import { TransportError } from "./errors.js";
import { CRDTStream } from "./stream.js";

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

  constructor(config: HttpTransportConfig) {
    this.baseURL = config.baseURL.replace(/\/+$/, "");
    this.fetchImpl = config.fetch ?? globalThis.fetch.bind(globalThis);
    this.headers = config.headers ?? {};
    this.auth = config.auth;
  }

  async pull(req: PullRequest): Promise<PullResponse> {
    return this.request<PullResponse>("/pull", req);
  }

  async push(req: PushRequest): Promise<PushResponse> {
    return this.request<PushResponse>("/push", req);
  }

  protected async request<T>(path: string, body: unknown): Promise<T> {
    const authHeaders = this.auth
      ? await Promise.resolve(this.auth.getHeaders())
      : {};

    const response = await this.fetchImpl(`${this.baseURL}${path}`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...this.headers,
        ...authHeaders,
      },
      body: JSON.stringify(body),
    });

    if (!response.ok) {
      const text = await response.text().catch(() => "");
      throw new TransportError(
        `CRDT ${path} returned ${response.status}: ${text}`,
        response.status
      );
    }

    return (await response.json()) as T;
  }
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
