/**
 * Default auth provider implementations.
 */

import type { AuthProvider } from "./types.js";

/**
 * Static auth provider that returns a fixed set of headers.
 *
 * @example
 * ```ts
 * const auth = new StaticAuthProvider({ Authorization: "Bearer my-token" });
 * const client = new CRDTClient({ baseURL: "/sync", nodeID: "n1", auth });
 * ```
 */
export class StaticAuthProvider implements AuthProvider {
  private headers: Record<string, string>;

  constructor(headers: Record<string, string>) {
    this.headers = headers;
  }

  getHeaders(): Record<string, string> {
    return { ...this.headers };
  }
}
