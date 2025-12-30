import { getDate } from "./date.js";
import { sleep } from "./sleep.js";
import pLimit from "p-limit";
import { deepClone } from "./clone.js";
import PQueue from "p-queue";

type TxOBEventHandlerResult = {
  processed_at?: Date;
  unprocessable_at?: Date;
  errors?: { error: unknown; timestamp: Date }[];
};

export type TxOBEvent<TxOBEventType extends string> = {
  id: string;
  timestamp: Date;
  type: TxOBEventType;
  data: Record<string, unknown>;
  correlation_id: string;
  handler_results: Record<string, TxOBEventHandlerResult>;
  errors: number;
  backoff_until?: Date | null;
  processed_at?: Date;
};

type TxOBEventHandlerOpts = {
  signal?: AbortSignal;
};

export type TxOBEventHandler = <TxOBEventType extends string>(
  event: TxOBEvent<TxOBEventType>,
  opts: TxOBEventHandlerOpts,
) => Promise<void>;

export type TxOBEventHandlerMap<TxOBEventType extends string> = Record<
  TxOBEventType,
  {
    [key: string]: TxOBEventHandler;
  }
>;

export type TxOBProcessorClientOpts = {
  signal?: AbortSignal;
  maxErrors: number;
};

export interface TxOBProcessorClient<TxOBEventType extends string> {
  getEventsToProcess(
    opts: TxOBProcessorClientOpts,
  ): Promise<Pick<TxOBEvent<TxOBEventType>, "id" | "errors">[]>;
  transaction(
    fn: (
      txProcessorClient: TxOBTransactionProcessorClient<TxOBEventType>,
    ) => Promise<void>,
  ): Promise<void>;
}

export interface TxOBTransactionProcessorClient<TxOBEventType extends string> {
  getEventByIdForUpdateSkipLocked(
    eventId: TxOBEvent<TxOBEventType>["id"],
    opts: TxOBProcessorClientOpts,
  ): Promise<TxOBEvent<TxOBEventType> | null>;
  updateEvent(event: TxOBEvent<TxOBEventType>): Promise<void>;
  createEvent(
    event: Omit<TxOBEvent<TxOBEventType>, "processed_at" | "backoff_until">,
  ): Promise<void>;
}

export const defaultBackoff = (errorCount: number): Date => {
  const baseDelayMs = 1000;
  const maxDelayMs = 1000 * 60;
  const backoffMs = Math.min(baseDelayMs * 2 ** errorCount, maxDelayMs);
  const retryTimestamp = new Date(Date.now() + backoffMs);

  return retryTimestamp;
};
const defaultPollingIntervalMs = 5000;
const defaultMaxErrors = 5;
const defaultMaxEventConcurrency = 20;
const defaultMaxHandlerConcurrency = 10;

type TxOBProcessEventsOpts<TxOBEventType extends string> = {
  maxErrors: number;
  backoff: (count: number) => Date;
  signal?: AbortSignal;
  logger?: Logger;
  maxEventConcurrency?: number;
  maxHandlerConcurrency?: number;
  onEventMaxErrorsReached?: (opts: {
    event: Readonly<TxOBEvent<TxOBEventType>>;
    txClient: TxOBTransactionProcessorClient<TxOBEventType>;
    signal?: AbortSignal;
  }) => Promise<void>;
};

const processEvent = async <TxOBEventType extends string>({
  client,
  handlerMap,
  unlockedEvent,
  opts,
}: {
  client: TxOBProcessorClient<TxOBEventType>;
  handlerMap: TxOBEventHandlerMap<TxOBEventType>;
  unlockedEvent: Pick<TxOBEvent<TxOBEventType>, "id" | "errors">;
  opts?: Partial<TxOBProcessEventsOpts<TxOBEventType>>;
}) => {
  const {
    logger,
    maxErrors = defaultMaxErrors,
    signal,
    backoff = defaultBackoff,
    maxHandlerConcurrency = defaultMaxHandlerConcurrency,
    onEventMaxErrorsReached,
  } = opts ?? {};

  if (signal?.aborted) {
    return;
  }
  if (unlockedEvent.errors >= maxErrors) {
    // Potential issue with client configuration on finding unprocessed events
    // Events with maximum allowed errors should not be returned from `getEventsToProcess`
    logger?.warn(
      {
        eventId: unlockedEvent.id,
        errors: unlockedEvent.errors,
        maxErrors,
      },
      "unexpected event with max errors returned from `getEventsToProcess`",
    );
    return;
  }

  await client.transaction(async (txClient) => {
    const lockedEvent = await txClient.getEventByIdForUpdateSkipLocked(
      unlockedEvent.id,
      { signal, maxErrors },
    );
    if (!lockedEvent) {
      logger?.debug(
        {
          eventId: unlockedEvent.id,
        },
        "skipping locked or already processed event",
      );
      return;
    }

    // While unlikely, the following two conditions are possible if a concurrent processor finished processing this event or reaching maximum errors between the time
    // that this processor found the event with `getEventsToProcess` and called `getEventByIdForUpdateSkipLocked`
    // `getEventByIdForUpdateSkipLocked` should handle this in its query implementation and return null to save resources
    if (lockedEvent.processed_at) {
      logger?.debug(
        {
          eventId: lockedEvent.id,
          correlationId: lockedEvent.correlation_id,
        },
        "skipping already processed event",
      );
      return;
    }
    if (lockedEvent.errors >= maxErrors) {
      logger?.debug(
        {
          eventId: lockedEvent.id,
          correlationId: lockedEvent.correlation_id,
        },
        "skipping event with maximum errors",
      );
      return;
    }

    let errored = false;

    const eventHandlerMap = handlerMap[lockedEvent.type] ?? {};

    // Typescript should prevent the caller from passing a handler map that doesn't specify all event types but we'll check for it anyway
    // This is distinct from an empty handler map for an event type which is valid
    // We just want the caller to be explicit about the event types they are interested in handling and not accidentally skip events
    if (!(lockedEvent.type in handlerMap)) {
      logger?.warn(
        {
          eventId: lockedEvent.id,
          type: lockedEvent.type,
          correlationId: lockedEvent.correlation_id,
        },
        "missing event handler map",
      );
      errored = true;
      lockedEvent.errors = maxErrors;
    }

    logger?.debug(
      {
        eventId: lockedEvent.id,
        type: lockedEvent.type,
        correlationId: lockedEvent.correlation_id,
      },
      `processing event`,
    );

    const handlerLimit = pLimit(maxHandlerConcurrency);
    await Promise.allSettled(
      Object.entries(eventHandlerMap).map(([handlerName, handler]) =>
        handlerLimit(async (): Promise<void> => {
          const handlerResults = lockedEvent.handler_results[handlerName] ?? {};
          if (handlerResults.processed_at) {
            logger?.debug(
              {
                eventId: lockedEvent.id,
                type: lockedEvent.type,
                handlerName,
                correlationId: lockedEvent.correlation_id,
              },
              "handler already processed",
            );
            return;
          }
          if (handlerResults.unprocessable_at) {
            logger?.debug(
              {
                eventId: lockedEvent.id,
                type: lockedEvent.type,
                handlerName,
                correlationId: lockedEvent.correlation_id,
              },
              "handler unprocessable",
            );
            return;
          }

          handlerResults.errors ??= [];

          try {
            await handler(lockedEvent, { signal });
            handlerResults.processed_at = getDate();
            logger?.debug(
              {
                eventId: lockedEvent.id,
                type: lockedEvent.type,
                handlerName,
                correlationId: lockedEvent.correlation_id,
              },
              "handler succeeded",
            );
          } catch (error) {
            logger?.error(
              {
                eventId: lockedEvent.id,
                type: lockedEvent.type,
                handlerName,
                error,
                correlationId: lockedEvent.correlation_id,
              },
              "handler errored",
            );

            if (error instanceof ErrorUnprocessableEventHandler) {
              handlerResults.unprocessable_at = getDate();
              handlerResults.errors?.push({
                error: error.message ?? error,
                timestamp: getDate(),
              });
              errored = true;
            } else {
              errored = true;
              handlerResults.errors?.push({
                error: (error as Error)?.message ?? error,
                timestamp: getDate(),
              });
            }
          }

          lockedEvent.handler_results[handlerName] = handlerResults;
        }),
      ),
    );

    // Check if all remaining handlers (those that haven't succeeded) are unprocessable
    // If so, there's nothing left to retry, so set errors to maxErrors to stop processing
    const remainingHandlers = Object.entries(eventHandlerMap).filter(
      ([handlerName, _]) => {
        const result = lockedEvent.handler_results[handlerName];
        return !result?.processed_at;
      },
    );

    const allRemainingHandlersUnprocessable =
      remainingHandlers.length > 0 &&
      remainingHandlers.every(([handlerName, _]) => {
        const result = lockedEvent.handler_results[handlerName];
        return result?.unprocessable_at;
      });

    if (allRemainingHandlersUnprocessable) {
      lockedEvent.errors = maxErrors;
      errored = true;
    }

    if (errored) {
      lockedEvent.errors = Math.min(lockedEvent.errors + 1, maxErrors);
      lockedEvent.backoff_until = backoff(lockedEvent.errors);
      if (lockedEvent.errors === maxErrors) {
        lockedEvent.backoff_until = null;
        lockedEvent.processed_at = getDate();

        if (onEventMaxErrorsReached) {
          try {
            await onEventMaxErrorsReached({
              event: deepClone(lockedEvent),
              txClient,
              signal,
            });
          } catch (hookError) {
            logger?.error(
              {
                eventId: lockedEvent.id,
                error: hookError,
              },
              "error in onEventMaxErrorsReached hook",
            );

            throw hookError;
          }
        }
      }
    } else {
      lockedEvent.backoff_until = null;
      lockedEvent.processed_at = getDate();
    }

    logger?.debug(
      {
        eventId: lockedEvent.id,
        type: lockedEvent.type,
        lockedEvent,
        correlationId: lockedEvent.correlation_id,
        errored,
      },
      "updating event",
    );

    await txClient.updateEvent(lockedEvent);
  });
};

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

export interface Logger {
  debug(message?: unknown, ...optionalParams: unknown[]): void;
  info(message?: unknown, ...optionalParams: unknown[]): void;
  warn(message?: unknown, ...optionalParams: unknown[]): void;
  error(message?: unknown, ...optionalParams: unknown[]): void;
}

export class EventProcessor<TxOBEventType extends string> {
  private client: TxOBProcessorClient<TxOBEventType>;
  private handlerMap: TxOBEventHandlerMap<TxOBEventType>;
  private opts: Omit<TxOBProcessEventsOpts<TxOBEventType>, "signal"> & {
    pollingIntervalMs: number;
  };
  private abortController: AbortController;
  private queue: PQueue;
  private state: "stopped" | "started" | "stopping" = "stopped";

  constructor({
    client,
    handlerMap,
    ...opts
  }: Omit<Partial<TxOBProcessEventsOpts<TxOBEventType>>, "signal"> & {
    pollingIntervalMs?: number;
  } & {
    client: TxOBProcessorClient<TxOBEventType>;
    handlerMap: TxOBEventHandlerMap<TxOBEventType>;
  }) {
    const _opts = {
      pollingIntervalMs: defaultPollingIntervalMs,
      maxErrors: defaultMaxErrors,
      backoff: defaultBackoff,
      maxEventConcurrency: defaultMaxEventConcurrency,
      maxHandlerConcurrency: defaultMaxHandlerConcurrency,
      ...opts,
    };
    this.client = client;
    this.handlerMap = handlerMap;
    this.opts = _opts;
    this.abortController = new AbortController();
    this.queue = new PQueue({
      concurrency: _opts.maxEventConcurrency,
    });
  }

  start() {
    if (this.state !== "stopped") {
      this.opts.logger?.warn(`cannot start processor from '${this.state}'`);
      return;
    }
    this.state = "started";
    this.opts.logger?.debug("processor started");

    const queuedEventIds: Set<string> = new Set();

    (async () => {
      try {
        do {
          try {
            const events = await this.client.getEventsToProcess({
              ...this.opts,
              signal: this.abortController.signal,
            });

            const unqueuedEvents = events.filter(
              (event) => !queuedEventIds.has(event.id),
            );
            this.opts.logger?.debug(
              `found ${unqueuedEvents.length} events to process`,
            );

            for (const event of unqueuedEvents) {
              queuedEventIds.add(event.id);
              this.queue
                .add(
                  async () => {
                    try {
                      await processEvent<TxOBEventType>({
                        client: this.client,
                        handlerMap: this.handlerMap,
                        unlockedEvent: event,
                        opts: {
                          ...this.opts,
                          signal: this.abortController.signal,
                        },
                      });
                    } catch (error) {
                      this.opts.logger?.error(
                        {
                          eventId: event.id,
                          error,
                        },
                        "error processing event",
                      );
                    } finally {
                      queuedEventIds.delete(event.id);
                    }
                  },
                  { signal: this.abortController.signal },
                )
                .catch((error) => {
                  // Handle queue.add() rejections (e.g., when aborted)
                  // The event processing error is already logged in the task's catch block
                  queuedEventIds.delete(event.id);
                });
            }
          } catch (error) {
            this.opts.logger?.error(
              { error },
              "error polling for events, will retry",
            );
            // Continue polling even on error
          }

          await sleep(this.opts.pollingIntervalMs);
        } while (!this.abortController.signal.aborted);
      } catch (error) {
        this.opts.logger?.error({ error }, "polling loop error");
      }
    })();
  }

  async stop(opts?: { timeoutMs?: number }): Promise<void> {
    if (this.state !== "started") {
      this.opts.logger?.warn(`cannot stop processor from '${this.state}'`);
      return;
    }
    this.state = "stopping";
    this.opts.logger?.debug("processor stopping");

    const _stopOpts = {
      timeoutMs: 10000,
      ...opts,
    };

    this.abortController.abort();
    this.queue.pause();

    let caughtErr: unknown;
    try {
      // Wait for both the queue to be empty and the polling loop to complete
      await Promise.race([
        this.queue.onPendingZero(),
        sleep(_stopOpts.timeoutMs).then(() => {
          throw new Error(`shutdown timeout ${_stopOpts.timeoutMs}ms elapsed`);
        }),
      ]);
    } catch (error) {
      caughtErr = error;
    }

    this.state = "stopped";
    this.opts.logger?.debug("processor stopped");

    if (caughtErr) {
      this.opts.logger?.error({ error: caughtErr }, "shutdown error");
      throw caughtErr;
    }
  }
}
