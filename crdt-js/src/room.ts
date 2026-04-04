/**
 * Room management, cursor tracking, and document collaboration.
 *
 * Provides a high-level RoomClient for interacting with the server's
 * room HTTP endpoints, plus helper types and utilities for
 * collaborative editing scenarios (cursors, typing indicators, etc.).
 */

import type { PresenceState } from "./types.js";

// --- Types ---

/** Cursor position within a document or field. */
export interface CursorPosition {
  x?: number;
  y?: number;
  offset?: number;
  line?: number;
  column?: number;
  selection_start?: number;
  selection_end?: number;
  field?: string;
}

/** Data associated with a participant in a room. */
export interface ParticipantData {
  name?: string;
  color?: string;
  avatar?: string;
  cursor?: CursorPosition;
  is_typing?: boolean;
  active_field?: string;
  status?: string;
  extra?: Record<string, unknown>;
}

/** A room record as returned by the server. */
export interface Room {
  id: string;
  type?: string;
  metadata?: unknown;
  max_participants?: number;
  created_at: string;
  created_by?: string;
}

/** A room with live participant information. */
export interface RoomInfo extends Room {
  participant_count: number;
  participants: PresenceState[];
}

// --- Configuration ---

/** Configuration for RoomClient. */
export interface RoomClientConfig {
  /** Base URL of the CRDT sync server (same as CRDTClient). */
  baseURL: string;
  /** Custom fetch implementation (default: globalThis.fetch). */
  fetch?: typeof fetch;
  /** Static headers to include in all room requests. */
  headers?: Record<string, string>;
}

// --- RoomClient ---

/**
 * RoomClient provides high-level room management APIs.
 *
 * Works with the server's room HTTP endpoints for CRUD operations,
 * cursor tracking, and typing indicators.
 *
 * @example
 * ```ts
 * const rooms = new RoomClient({ baseURL: "https://api.example.com/sync" });
 *
 * // Create and join a document room
 * const room = await rooms.joinDocumentRoom("documents", "doc-1", "node-1", {
 *   name: "Alice",
 *   color: randomColor(),
 * });
 *
 * // Update cursor
 * await rooms.updateCursor(room.id, "node-1", { line: 10, column: 5 });
 *
 * // Update typing status
 * await rooms.updateTyping(room.id, "node-1", true);
 * ```
 */
export class RoomClient {
  private baseURL: string;
  private fetchImpl: typeof fetch;
  private headers: Record<string, string>;

  constructor(config: RoomClientConfig) {
    this.baseURL = config.baseURL.replace(/\/+$/, "");
    this.fetchImpl = config.fetch ?? globalThis.fetch.bind(globalThis);
    this.headers = config.headers ?? {};
  }

  /**
   * List all rooms, optionally filtered by type.
   *
   * @param type - Optional room type filter (e.g., "document", "channel").
   * @returns Array of rooms with participant info.
   */
  async listRooms(type?: string): Promise<RoomInfo[]> {
    const params = type ? `?type=${encodeURIComponent(type)}` : "";
    return this.request<RoomInfo[]>("GET", `/rooms${params}`);
  }

  /**
   * Get a room by ID with live participant info.
   *
   * @param roomId - The room identifier.
   * @returns Room info, or null if not found.
   */
  async getRoom(roomId: string): Promise<RoomInfo | null> {
    try {
      return await this.request<RoomInfo>(
        "GET",
        `/rooms/${encodeURIComponent(roomId)}`
      );
    } catch {
      return null;
    }
  }

  /**
   * Create a room.
   *
   * @param id - Unique room identifier.
   * @param opts - Optional room configuration.
   * @returns The created room.
   */
  async createRoom(
    id: string,
    opts?: {
      type?: string;
      metadata?: unknown;
      max_participants?: number;
      created_by?: string;
    }
  ): Promise<Room> {
    return this.request<Room>("POST", "/rooms", { id, ...opts });
  }

  /**
   * Join a room with participant data.
   *
   * @param roomId - The room to join.
   * @param nodeId - The joining node's identifier.
   * @param data - Optional participant data (name, color, avatar, etc.).
   * @returns Updated room info with participants.
   */
  async joinRoom(
    roomId: string,
    nodeId: string,
    data?: ParticipantData
  ): Promise<RoomInfo> {
    return this.request<RoomInfo>(
      "POST",
      `/rooms/${encodeURIComponent(roomId)}/join`,
      { node_id: nodeId, data }
    );
  }

  /**
   * Leave a room.
   *
   * @param roomId - The room to leave.
   * @param nodeId - The leaving node's identifier.
   */
  async leaveRoom(roomId: string, nodeId: string): Promise<void> {
    await this.request<unknown>(
      "POST",
      `/rooms/${encodeURIComponent(roomId)}/leave`,
      { node_id: nodeId }
    );
  }

  /**
   * Update cursor position in a room.
   *
   * @param roomId - The room identifier.
   * @param nodeId - The node whose cursor is being updated.
   * @param cursor - The new cursor position.
   */
  async updateCursor(
    roomId: string,
    nodeId: string,
    cursor: CursorPosition
  ): Promise<void> {
    await this.request<unknown>(
      "POST",
      `/rooms/${encodeURIComponent(roomId)}/cursor`,
      { node_id: nodeId, cursor }
    );
  }

  /**
   * Update typing status in a room.
   *
   * @param roomId - The room identifier.
   * @param nodeId - The node whose typing status is being updated.
   * @param isTyping - Whether the node is currently typing.
   */
  async updateTyping(
    roomId: string,
    nodeId: string,
    isTyping: boolean
  ): Promise<void> {
    await this.request<unknown>(
      "POST",
      `/rooms/${encodeURIComponent(roomId)}/typing`,
      { node_id: nodeId, is_typing: isTyping }
    );
  }

  /**
   * Update room metadata.
   *
   * @param roomId - The room identifier.
   * @param metadata - The new metadata value.
   */
  async updateMetadata(roomId: string, metadata: unknown): Promise<void> {
    await this.request<unknown>(
      "POST",
      `/rooms/${encodeURIComponent(roomId)}/metadata`,
      { metadata }
    );
  }

  /**
   * Get room participants.
   *
   * @param roomId - The room identifier.
   * @returns Array of presence states for all participants.
   */
  async getParticipants(roomId: string): Promise<PresenceState[]> {
    return this.request<PresenceState[]>(
      "GET",
      `/rooms/${encodeURIComponent(roomId)}/participants`
    );
  }

  /**
   * Helper: create (if needed) and join a document room.
   *
   * Generates a standard room ID from table + pk, creates the room
   * if it doesn't exist, then joins it.
   *
   * @param table - The document table name.
   * @param pk - The document primary key.
   * @param nodeId - The joining node's identifier.
   * @param data - Optional participant data.
   * @returns Updated room info.
   */
  async joinDocumentRoom(
    table: string,
    pk: string,
    nodeId: string,
    data?: ParticipantData
  ): Promise<RoomInfo> {
    const roomId = documentRoomId(table, pk);

    // Create the room if it doesn't exist (ignore errors if it already exists).
    try {
      await this.createRoom(roomId, {
        type: "document",
        metadata: { table, pk },
      });
    } catch {
      // Room likely already exists — continue to join.
    }

    return this.joinRoom(roomId, nodeId, data);
  }

  /**
   * Helper: leave a document room.
   *
   * @param table - The document table name.
   * @param pk - The document primary key.
   * @param nodeId - The leaving node's identifier.
   */
  async leaveDocumentRoom(
    table: string,
    pk: string,
    nodeId: string
  ): Promise<void> {
    await this.leaveRoom(documentRoomId(table, pk), nodeId);
  }

  // --- Internal ---

  private async request<T>(
    method: string,
    path: string,
    body?: unknown
  ): Promise<T> {
    const url = `${this.baseURL}${path}`;

    const init: RequestInit = {
      method,
      headers: {
        "Content-Type": "application/json",
        ...this.headers,
      },
    };

    if (body !== undefined && method !== "GET") {
      init.body = JSON.stringify(body);
    }

    const response = await this.fetchImpl(url, init);

    if (!response.ok) {
      const text = await response.text().catch(() => "");
      throw new Error(
        `Room API error: ${response.status} ${response.statusText}${text ? ` — ${text}` : ""}`
      );
    }

    // Some endpoints return no body (204 or empty).
    const contentType = response.headers.get("content-type") ?? "";
    if (
      response.status === 204 ||
      !contentType.includes("application/json")
    ) {
      return undefined as unknown as T;
    }

    return response.json() as Promise<T>;
  }
}

// --- Helpers ---

/**
 * Generates the standard room ID for a document.
 *
 * Matches the Go server's `DocumentRoomID` format: "table:pk".
 *
 * @param table - The document table name.
 * @param pk - The document primary key.
 * @returns The room ID string.
 */
export function documentRoomId(table: string, pk: string): string {
  return `${table}:${pk}`;
}

/** Predefined collaboration colors. */
const COLLAB_COLORS = [
  "#e57373", // red
  "#81c784", // green
  "#64b5f6", // blue
  "#ffb74d", // orange
  "#ba68c8", // purple
  "#4dd0e1", // cyan
  "#fff176", // yellow
  "#f06292", // pink
  "#a1887f", // brown
  "#90a4ae", // blue-grey
  "#aed581", // light green
  "#7986cb", // indigo
  "#4db6ac", // teal
  "#ff8a65", // deep orange
  "#9575cd", // deep purple
  "#dce775", // lime
];

/**
 * Generates a random collaboration color from a curated palette.
 *
 * @returns A hex color string.
 */
export function randomColor(): string {
  return COLLAB_COLORS[Math.floor(Math.random() * COLLAB_COLORS.length)];
}
