/**
 * Undo/Redo manager for optimistic UI with rollback.
 *
 * Records field state snapshots before each local mutation so that
 * changes can be reverted (undo) or re-applied (redo). Works at the
 * field level: each entry captures a single ChangeRecord and the
 * FieldState that existed before the change was applied.
 */

import type { ChangeRecord, FieldState } from "./types.js";

/** A single undo/redo entry capturing a change and its pre-change state. */
export interface UndoEntry {
  /** The change that was applied. */
  change: ChangeRecord;
  /** Snapshot of the field state BEFORE the change was applied (null if field didn't exist). */
  previousState: FieldState | null;
  /** Timestamp of the operation (Date.now()). */
  timestamp: number;
}

/**
 * Manages undo/redo stacks for CRDT field mutations.
 *
 * Typical usage:
 * 1. Before applying a local change, snapshot the current field state.
 * 2. Call `record(change, previousState)` to push onto the undo stack.
 * 3. Call `undo()` to pop the last entry (caller restores the field state).
 * 4. Call `redo()` to re-apply a previously undone change.
 *
 * New mutations clear the redo stack (standard undo/redo semantics).
 */
export class UndoManager {
  private undoStack: UndoEntry[] = [];
  private redoStack: UndoEntry[] = [];
  private maxHistory: number;

  constructor(options?: { maxHistory?: number }) {
    this.maxHistory = options?.maxHistory ?? 100;
  }

  /**
   * Record a change and its pre-change state for potential undo.
   * Clears the redo stack (new mutation invalidates redo history).
   */
  record(change: ChangeRecord, previousState: FieldState | null): void {
    this.undoStack.push({
      change,
      previousState,
      timestamp: Date.now(),
    });

    // Trim oldest entries if we exceed maxHistory.
    if (this.undoStack.length > this.maxHistory) {
      this.undoStack.splice(0, this.undoStack.length - this.maxHistory);
    }

    // New mutation invalidates redo history.
    this.redoStack = [];
  }

  /** Check if undo is available. */
  get canUndo(): boolean {
    return this.undoStack.length > 0;
  }

  /** Check if redo is available. */
  get canRedo(): boolean {
    return this.redoStack.length > 0;
  }

  /** Get the number of undo entries. */
  get undoCount(): number {
    return this.undoStack.length;
  }

  /** Get the number of redo entries. */
  get redoCount(): number {
    return this.redoStack.length;
  }

  /**
   * Pop the last entry from the undo stack and push to redo.
   * Returns the entry so the caller can restore the previous field state.
   * Returns null if the undo stack is empty.
   */
  undo(): UndoEntry | null {
    const entry = this.undoStack.pop() ?? null;
    if (entry) {
      this.redoStack.push(entry);
    }
    return entry;
  }

  /**
   * Pop the last entry from the redo stack and push to undo.
   * Returns the entry so the caller can re-apply the change.
   * Returns null if the redo stack is empty.
   */
  redo(): UndoEntry | null {
    const entry = this.redoStack.pop() ?? null;
    if (entry) {
      this.undoStack.push(entry);
    }
    return entry;
  }

  /** Clear all undo and redo history. */
  clear(): void {
    this.undoStack = [];
    this.redoStack = [];
  }
}
