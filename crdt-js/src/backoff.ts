/**
 * Exponential backoff with full jitter.
 *
 * Full jitter (delay = random(0, ceiling)) rather than the raw geometric
 * delay: a fixed schedule makes every client that dropped during an outage
 * retry at the same instant, which is what knocks a recovering server back
 * over.
 */

export interface BackoffOptions {
  /** First delay in ms (default: 1000). */
  initialDelay?: number;
  /** Ceiling in ms (default: 30000). */
  maxDelay?: number;
  /** Growth multiplier (default: 2). */
  factor?: number;
  /** Apply full jitter (default: true). */
  jitter?: boolean;
  /** Randomness source, for tests (default: Math.random). */
  random?: () => number;
}

export class Backoff {
  private initialDelay: number;
  private maxDelay: number;
  private factor: number;
  private jitter: boolean;
  private random: () => number;
  private attempts = 0;

  constructor(opts?: BackoffOptions) {
    this.initialDelay = opts?.initialDelay ?? 1000;
    this.maxDelay = opts?.maxDelay ?? 30_000;
    this.factor = opts?.factor ?? 2;
    this.jitter = opts?.jitter ?? true;
    this.random = opts?.random ?? Math.random;
  }

  /** Number of delays issued since the last reset. */
  get attempt(): number {
    return this.attempts;
  }

  /** Next delay in ms, advancing the schedule. */
  next(): number {
    const ceiling = Math.min(
      this.maxDelay,
      this.initialDelay * Math.pow(this.factor, this.attempts)
    );
    this.attempts++;
    return this.jitter ? Math.floor(this.random() * ceiling) : ceiling;
  }

  /** Return to the initial delay. Call after a successful connection. */
  reset(): void {
    this.attempts = 0;
  }
}
