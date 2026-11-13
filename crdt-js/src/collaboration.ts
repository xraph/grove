import type { CRDTClient } from "./client.js";
import type { CursorPosition, ParticipantData } from "./room.js";
import type { StreamSubscription } from "./types.js";

export interface CollaborationOptions {
  /** Minimum interval between updates, in ms (default: 50, or 20 Hz). */
  updateInterval?: number;
  /** Clear typing after this much inactivity, in ms (default: 1500). */
  typingTimeout?: number;
  /** Observe an existing stream for reconnects. The session does not own it. */
  stream?: StreamSubscription;
  /** Receives failures from scheduled updates and reconnect refreshes. */
  onError?: (error: unknown) => void;
}

function positive(value: number | undefined, fallback: number): number {
  return value !== undefined && Number.isFinite(value) && value > 0 ? value : fallback;
}

// Presence is JSON data. Clone at the boundary so a caller cannot change a
// queued cursor by mutating an object after passing it to update().
function snapshot<T>(value: T): T {
  const copy = JSON.parse(JSON.stringify(value)) as T;
  const freeze = (item: unknown): void => {
    if (item && typeof item === "object") {
      for (const child of Object.values(item)) freeze(child);
      Object.freeze(item);
    }
  };
  freeze(copy);
  return copy;
}

/**
 * One local participant's collaboration lifecycle for one topic.
 *
 * Call start(), then updateCursor/update/setTyping as often as needed.
 * At most one write is in flight, with one replaceable pending snapshot.
 * flush() forces the latest state out; close() cancels pending movement,
 * waits for the in-flight write, and leaves without resurrecting presence.
 * Use one session per client/topic; inbound presence still flows through
 * client.applyPresenceEvent (as it does in CRDTProvider).
 */
export class CollaborationSession<T extends ParticipantData = ParticipantData> {
  private state: T;
  private listeners = new Set<() => void>();
  private active = false;
  private closed = false;
  private dirty = false;
  private lastSent = -Infinity;
  private inFlight: Promise<void> | null = null;
  private starting: Promise<void> | null = null;
  private closing: Promise<void> | null = null;
  private refreshing: Promise<void> | null = null;
  private timer: ReturnType<typeof setTimeout> | undefined;
  private typingTimer: ReturnType<typeof setTimeout> | undefined;
  private unwatch?: () => void;
  private interval: number;
  private typingTimeout: number;

  constructor(
    private client: CRDTClient,
    readonly topic: string,
    initialData: T,
    private options: CollaborationOptions = {},
  ) {
    this.state = snapshot(initialData);
    this.interval = positive(options.updateInterval, 50);
    this.typingTimeout = positive(options.typingTimeout, 1500);
  }

  /** Cached immutable local state, suitable for useSyncExternalStore. */
  getSnapshot = (): T => this.state;

  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => { this.listeners.delete(listener); };
  };

  /** Publish initial state and discover peers already present. Idempotent. */
  start(): Promise<void> {
    if (this.closed) return Promise.reject(new Error("Collaboration session is closed"));
    if (this.starting) return this.starting;
    this.active = true;
    this.dirty = true;
    this.unwatch = this.options.stream?.on(event => {
      if (event.type === "connected") void this.refresh().catch(error => this.report(error));
    });
    this.starting = this.refresh();
    return this.starting;
  }

  /** Shallow-merge local presence; network writes are paced and coalesced. */
  update(patch: Partial<T>): void {
    if (this.closed) return;
    this.state = snapshot({ ...this.state, ...patch });
    this.dirty = true;
    for (const listener of this.listeners) listener();
    this.schedule();
  }

  /** Publish a pointer or stable text selection; null hides the cursor. */
  updateCursor(cursor: CursorPosition | null): void {
    this.update({ cursor } as Partial<T>);
  }

  /** Repeated true calls extend the deadline, even without cursor movement. */
  setTyping(typing: boolean): void {
    if (this.closed) return;
    clearTimeout(this.typingTimer);
    this.update({ is_typing: typing } as Partial<T>);
    if (typing) this.typingTimer = setTimeout(() => this.setTyping(false), this.typingTimeout);
  }

  /** Republish and re-seed after reconnect, with only one refresh in flight. */
  refresh(): Promise<void> {
    if (!this.active || this.closed) return Promise.resolve();
    if (this.refreshing) return this.refreshing;
    this.dirty = true;
    const refresh = this.flush().then(() => {
      if (!this.closed) return this.client.refreshPresence(this.topic);
    });
    this.refreshing = refresh;
    const done = () => { if (this.refreshing === refresh) this.refreshing = null; };
    void refresh.then(done, done);
    return refresh;
  }

  /** Finish the current write and send the latest pending state. Movement
   * arriving during that final write remains scheduled, so continuous input
   * cannot prevent a join or reconnect snapshot from completing. */
  async flush(): Promise<void> {
    clearTimeout(this.timer);
    this.timer = undefined;
    if (this.inFlight) await this.inFlight;
    if (this.active && !this.closed) await this.send();
  }

  /** Stop timers and leave after outstanding writes. Safe to call twice. */
  close(): Promise<void> {
    if (this.closing) return this.closing;
    const wasActive = this.active;
    this.closed = true;
    this.active = false;
    this.dirty = false;
    clearTimeout(this.timer);
    clearTimeout(this.typingTimer);
    this.unwatch?.();
    // Enqueue leave immediately: CRDTClient serializes it after any write.
    // Deferring leave until a promise settles could remove a newly mounted
    // React session that has already joined the same topic.
    this.closing = wasActive ? this.client.leavePresence(this.topic) : Promise.resolve();
    return this.closing;
  }

  private schedule(): void {
    if (!this.active || this.closed || !this.dirty || this.inFlight || this.timer !== undefined) return;
    const delay = Math.max(0, this.interval - (Date.now() - this.lastSent));
    this.timer = setTimeout(() => {
      this.timer = undefined;
      void this.send().catch(error => this.report(error));
    }, delay);
  }

  private send(): Promise<void> {
    if (this.inFlight) return this.inFlight;
    if (!this.active || this.closed || !this.dirty) return Promise.resolve();
    this.dirty = false;
    this.lastSent = Date.now();
    const write = this.client.updatePresence(this.topic, this.state);
    this.inFlight = write;
    const done = () => {
      this.inFlight = null;
      this.schedule();
    };
    void write.then(done, done);
    return write;
  }

  private report(error: unknown): void {
    this.options.onError?.(error);
  }
}

export interface CursorPoint {
  x: number;
  y: number;
  /** Changing coordinate spaces snaps instead of animating across them. */
  field?: string;
}

interface CursorMotion {
  from: CursorPoint;
  to: CursorPoint;
  at: number;
}

/** Render remote pointers smoothly at the display's frame rate while sending
 * presence at a lower rate. Uses monotonic local arrival times, never remote
 * wall clocks. No extrapolation: stopped pointers settle without overshoot.
 * Call remove on peer leave; sample from your requestAnimationFrame loop. */
export class CursorSmoother {
  private motions = new Map<string, CursorMotion>();
  private duration: number;

  constructor(duration = 80) { this.duration = positive(duration, 80); }

  push(nodeID: string, point: CursorPoint, now = performance.now()): void {
    if (![point.x, point.y, now].every(Number.isFinite)) return;
    const old = this.motions.get(nodeID);
    if (old && now < old.at) return;
    const from = old?.to.field === point.field ? this.sample(nodeID, now) ?? point : point;
    this.motions.set(nodeID, { from: { ...from }, to: { ...point }, at: now });
  }

  sample(nodeID: string, now = performance.now()): CursorPoint | null {
    const motion = this.motions.get(nodeID);
    if (!motion) return null;
    const t = Number.isFinite(now) ? Math.min(1, Math.max(0, (now - motion.at) / this.duration)) : 1;
    // Smoothstep has zero velocity at both endpoints and never overshoots.
    const weight = t * t * (3 - 2 * t);
    return { ...motion.to,
      x: motion.from.x + (motion.to.x - motion.from.x) * weight,
      y: motion.from.y + (motion.to.y - motion.from.y) * weight,
    };
  }

  remove(nodeID: string): void { this.motions.delete(nodeID); }
  clear(): void { this.motions.clear(); }
}
