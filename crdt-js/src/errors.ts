/**
 * Error classes for CRDT operations.
 */

/** Error thrown by CRDT client operations. */
export class CRDTError extends Error {
  constructor(
    message: string,
    public readonly statusCode?: number
  ) {
    super(message);
    this.name = "CRDTError";
  }
}

/**
 * Error thrown by transport operations.
 * Extends CRDTError for backward compatibility — existing
 * `catch (e) { if (e instanceof CRDTError) }` patterns still work.
 */
export class TransportError extends CRDTError {
  constructor(message: string, statusCode?: number) {
    super(message, statusCode);
    this.name = "TransportError";
  }
}
