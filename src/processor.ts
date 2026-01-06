import { getDate } from "./date.js";
import { sleep } from "./sleep.js";
import pLimit from "p-limit";
import { deepClone } from "./clone.js";
import PQueue from "p-queue";
import { ErrorUnprocessableEventHandler, TxOBError } from "./error.js";
import { throttle } from "throttle-debounce";

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

export interface WakeupEmitter {
  on(event: "wakeup", listener: () => void): void;
  off(event: "wakeup", listener: () => void): void;
  close(): Promise<void>;
}

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
const defaultPollingIntervalMs = 5_000;
const defaultMaxErrors = 5;
const defaultMaxEventConcurrency = 20;
const defaultMaxHandlerConcurrency = 10;
const defaultMaxQueuedEvents = 500;
const defaultWakeupTimeoutMs = 60_000;
const defaultWakeupThrottleMs = 1_000;

type TxOBProcessEventsOpts<TxOBEventType extends string> = {
  maxErrors: number;
  backoff: (count: number) => Date;
  signal?: AbortSignal;
  logger?: Logger;
  maxEventConcurrency?: number;
  maxHandlerConcurrency?: number;
  maxQueuedEvents?: number;
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
}): Promise<{ backoffUntil?: Date }> => {
  const {
    logger,
    maxErrors = defaultMaxErrors,
    signal,
    backoff = defaultBackoff,
    maxHandlerConcurrency = defaultMaxHandlerConcurrency,
    onEventMaxErrorsReached,
  } = opts ?? {};

  if (signal?.aborted) {
    return {};
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
    return {};
  }

  let backoffUntil: Date | undefined;

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

    const backoffs: Date[] = [];

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
              if (error instanceof TxOBError && error.backoffUntil) {
                backoffs.push(error.backoffUntil);
              }

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
      backoffs.push(backoff(lockedEvent.errors));
      const latestBackoff = backoffs.sort(
        (a, b) => b.getTime() - a.getTime(),
      )[0];
      lockedEvent.backoff_until = latestBackoff;
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

    backoffUntil = lockedEvent.backoff_until ?? undefined;

    await txClient.updateEvent(lockedEvent);
  });

  return { backoffUntil };
};

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
    maxQueuedEvents: number;
    wakeupTimeoutMs: number;
    wakeupThrottleMs: number;
  };
  private abortController: AbortController;
  private queue: PQueue;
  private state: "stopped" | "started" | "stopping" = "stopped";
  private wakeupEmitter?: WakeupEmitter;
  private wakeupListener?: () => void;
  private throttledPoll?: ReturnType<typeof throttle>;
  private lastWakeupTime: number = Date.now();
  private isPolling: boolean = false;

  constructor({
    client,
    handlerMap,
    wakeupEmitter,
    ...opts
  }: Omit<Partial<TxOBProcessEventsOpts<TxOBEventType>>, "signal"> & {
    pollingIntervalMs?: number;
    wakeupTimeoutMs?: number;
    wakeupThrottleMs?: number;
    wakeupEmitter?: WakeupEmitter;
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
      maxQueuedEvents: defaultMaxQueuedEvents,
      wakeupTimeoutMs: defaultWakeupTimeoutMs,
      wakeupThrottleMs: defaultWakeupThrottleMs,
      ...opts,
    };
    this.client = client;
    this.handlerMap = handlerMap;
    this.wakeupEmitter = wakeupEmitter;
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

    // Poll function that can be called from wakeup signals or polling loop
    const poll = async (): Promise<void> => {
      if (this.abortController.signal.aborted) {
        return;
      }

      // Prevent concurrent polls
      if (this.isPolling) {
        this.opts.logger?.debug("skipping poll - already polling");
        return;
      }

      this.isPolling = true;
      try {
        // Skip polling if we're at capacity to prevent memory leaks
        if (queuedEventIds.size >= this.opts.maxQueuedEvents) {
          this.opts.logger?.debug(
            {
              queuedCount: queuedEventIds.size,
              maxQueuedEvents: this.opts.maxQueuedEvents,
            },
            "skipping poll - queue at capacity",
          );
          return;
        }

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
                  const { backoffUntil } = await processEvent<TxOBEventType>({
                    client: this.client,
                    handlerMap: this.handlerMap,
                    unlockedEvent: event,
                    opts: {
                      ...this.opts,
                      signal: this.abortController.signal,
                    },
                  });

                  // Simulate a local wakeup signal after the backoff period
                  // to reduce latency on backed-off event reprocessing
                  if (backoffUntil) {
                    sleep(
                      backoffUntil.getTime() - Date.now(),
                      this.abortController.signal,
                    )
                      .then(() => {
                        this.throttledPoll?.();
                      })
                      .catch(() => {});
                  }
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
            .catch(() => {
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
      } finally {
        this.isPolling = false;
      }
    };

    if (this.wakeupEmitter) {
      // Setup wakeup signal listener with combined leading and trailing edge throttle
      // Using throttle-debounce library for robust throttling behavior
      // Leading edge: Poll immediately on the first signal
      // Trailing edge: If signals keep coming, also poll after a delay from the last signal
      // This provides both low latency (leading edge) and ensures we catch events
      // even if signals arrive in a continuous burst (trailing edge)
      const throttleMs = this.opts.wakeupThrottleMs;

      // Create throttled poll function with both leading and trailing edges
      // noLeading: false (default) = execute immediately on first signal (leading edge)
      // noTrailing: false (default) = also execute after delay from last signal (trailing edge)
      // This throttled poll is used by both wakeup signals and fallback polling to prevent conflicts
      this.throttledPoll = throttle(throttleMs, () => {
        if (this.abortController.signal.aborted) {
          return;
        }
        this.lastWakeupTime = Date.now();
        this.opts.logger?.debug("triggering throttled poll");
        void poll();
      });

      this.wakeupListener = () => {
        if (this.abortController.signal.aborted) {
          return;
        }
        this.opts.logger?.debug("received wakeup signal");
        this.throttledPoll?.();
      };

      this.wakeupEmitter.on("wakeup", this.wakeupListener);
      this.opts.logger?.debug("wakeup signal listener registered");

      // Initial poll
      this.throttledPoll?.();

      // Start fallback polling loop
      // This runs at a lower frequency and only polls if we haven't received a wakeup signal recently
      // Uses the same throttled poll function to prevent conflicts with wakeup-triggered polls
      (async () => {
        try {
          do {
            await sleep(
              this.opts.pollingIntervalMs,
              this.abortController.signal,
            ).catch(() => {});

            if (this.abortController.signal.aborted) {
              break;
            }

            // Check if we've received a wakeup signal recently
            const timeSinceLastWakeup = Date.now() - this.lastWakeupTime;
            if (timeSinceLastWakeup >= this.opts.wakeupTimeoutMs) {
              this.opts.logger?.debug(
                {
                  timeSinceLastWakeup,
                  wakeupTimeoutMs: this.opts.wakeupTimeoutMs,
                },
                "fallback poll triggered - no wakeup signal received",
              );
              this.throttledPoll?.();
            } else {
              this.opts.logger?.debug(
                {
                  timeSinceLastWakeup,
                  wakeupTimeoutMs: this.opts.wakeupTimeoutMs,
                },
                "skipping fallback poll - wakeup signal received recently",
              );
            }
          } while (!this.abortController.signal.aborted);
        } catch (error) {
          this.opts.logger?.error({ error }, "fallback polling loop error");
        }
      })();
    } else {
      // Standard polling loop
      (async () => {
        try {
          do {
            await poll();
            await sleep(
              this.opts.pollingIntervalMs,
              this.abortController.signal,
            ).catch(() => {});
          } while (!this.abortController.signal.aborted);
        } catch (error) {
          this.opts.logger?.error({ error }, "polling loop error");
        }
      })();
    }
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

    // Clean up wakeup listener
    if (this.wakeupListener && this.wakeupEmitter) {
      this.wakeupEmitter.off("wakeup", this.wakeupListener);
      this.wakeupListener = undefined;
    }

    // Cancel throttled poll (cleans up any pending timers)
    if (this.throttledPoll) {
      (this.throttledPoll as { cancel?: () => void }).cancel?.();
      this.throttledPoll = undefined;
    }

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
