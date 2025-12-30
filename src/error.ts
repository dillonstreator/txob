/**
 * TxOBError can be thrown by an event handler to indicate that the event processing should be retried.
 * It allows handlers to specify a custom backoff time via the `backoffUntil` property.
 *
 * When multiple handlers throw TxOBError with different `backoffUntil` dates, the processor will use
 * the latest (maximum) backoff time among all handlers and the default backoff calculation.
 *
 * @example
 * ```typescript
 * // Throw an error with a custom backoff time
 * throw new TxOBError("Rate limit exceeded", {
 *   backoffUntil: new Date(Date.now() + 60000) // Retry after 1 minute
 * });
 *
 * // Throw an error with a cause
 * throw new TxOBError("Processing failed", {
 *   cause: originalError,
 *   backoffUntil: new Date(Date.now() + 30000) // Retry after 30 seconds
 * });
 * ```
 */
export class TxOBError extends Error {
  /**
   * Optional date indicating when the event should be retried.
   * If provided, this backoff time will be considered along with the default backoff calculation,
   * and the latest (maximum) backoff time will be used.
   */
  backoffUntil?: Date;

  constructor(
    message: string,
    options: { cause?: Error; backoffUntil?: Date } = {},
  ) {
    super(message);
    this.cause = options.cause;
    this.backoffUntil = options.backoffUntil;
  }
}

/**
 * ErrorUnprocessableEventHandler can be thrown by an event handler to indicate that the event handler is unprocessable.
 * It wraps the original error that caused the handler to be unprocessable.
 * This error will signal the processor to stop processing the event handler and mark the event handler as unprocessable.
 */
export class ErrorUnprocessableEventHandler extends Error {
  error: Error;

  constructor(error: Error) {
    const message = `unprocessable event handler: ${error.message}`;
    super(message);
    this.error = error;
  }
}
