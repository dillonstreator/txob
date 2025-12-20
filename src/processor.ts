import { retryable, RetryOpts } from "./retry.js";
import { getDate } from "./date.js";
import EventEmitter from "node:events";
import { sleep } from "./sleep.js";
import pLimit from "p-limit";

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
const defaultMaxErrors = 5;
const defaultMaxEventConcurrency = 5;
const defaultMaxHandlerConcurrency = 10;

export type EventProcessingFailedReason =
  | { type: "max_errors_reached" }
  | { type: "unprocessable_error"; handlerName: string; error: Error }
  | { type: "missing_handler_map" };

type TxOBProcessEventsOpts<TxOBEventType extends string> = {
  maxErrors: number;
  backoff: (count: number) => Date;
  retryOpts?: RetryOpts;
  signal?: AbortSignal;
  logger?: Logger;
  maxEventConcurrency?: number;
  maxHandlerConcurrency?: number;
  onEventProcessingFailed?: (opts: {
    failedEvent: TxOBEvent<TxOBEventType>;
    reason: EventProcessingFailedReason;
    txClient: TxOBTransactionProcessorClient<TxOBEventType>;
    signal?: AbortSignal;
  }) => Promise<void>;
};

export const processEvents = async <TxOBEventType extends string>(
  client: TxOBProcessorClient<TxOBEventType>,
  handlerMap: TxOBEventHandlerMap<TxOBEventType>,
  opts?: Partial<TxOBProcessEventsOpts<TxOBEventType>>,
): Promise<void> => {
  const {
    maxErrors = defaultMaxErrors,
    backoff = defaultBackoff,
    maxEventConcurrency = defaultMaxEventConcurrency,
    maxHandlerConcurrency = defaultMaxHandlerConcurrency,
    logger,
    signal,
    onEventProcessingFailed,
    retryOpts,
  } = opts ?? {};

  const events = await client.getEventsToProcess({ maxErrors, signal });
  logger?.debug(`found ${events.length} events to process`);

  const eventLimit = pLimit(maxEventConcurrency);
  await Promise.allSettled(
    events.map((unlockedEvent) =>
      eventLimit(async () => {
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

        try {
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

            let eventHandlerMap = handlerMap[lockedEvent.type];
            if (!eventHandlerMap) {
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
              eventHandlerMap = {};

              await onEventProcessingFailed?.({
                failedEvent: lockedEvent,
                reason: { type: "missing_handler_map" },
                txClient,
                signal,
              }).catch((hookError) => {
                logger?.error(
                  {
                    eventId: lockedEvent.id,
                    error: hookError,
                  },
                  "error in onEventProcessingFailed hook for missing handler map",
                );
              });
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
                  const handlerResults =
                    lockedEvent.handler_results[handlerName] ?? {};
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

                      await onEventProcessingFailed?.({
                        failedEvent: lockedEvent,
                        reason: {
                          type: "unprocessable_error",
                          handlerName,
                          error: error.error,
                        },
                        txClient,
                        signal,
                      }).catch((hookError) => {
                        logger?.error(
                          {
                            eventId: lockedEvent.id,
                            handlerName,
                            error: hookError,
                          },
                          "error in onEventProcessingFailed hook for unprocessable error",
                        );
                      });
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

            if (errored) {
              lockedEvent.errors = Math.min(lockedEvent.errors + 1, maxErrors);
              lockedEvent.backoff_until = backoff(lockedEvent.errors);
              if (lockedEvent.errors === maxErrors) {
                lockedEvent.backoff_until = null;

                await onEventProcessingFailed?.({
                  failedEvent: lockedEvent,
                  reason: { type: "max_errors_reached" },
                  txClient,
                  signal,
                }).catch((hookError) => {
                  logger?.error(
                    {
                      eventId: lockedEvent.id,
                      error: hookError,
                    },
                    "error in onEventProcessingFailed hook for max errors",
                  );
                });
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

            // The success of this update is crucial for the processor flow.
            // In the unlikely scenario of a failure to update the event, any handlers that have succeeded
            // during this iteration will be reinvoked in the subsequent processor tick.
            // This is why the processor is guaranteed for 'at least once' processing.
            await retryable(() => txClient.updateEvent(lockedEvent), {
              retries: 3,
              factor: 2,
              minTimeout: 100,
              maxTimeout: 2500,
              randomize: true,
              ...retryOpts,
            });
          });
        } catch (error) {
          logger?.error(
            {
              eventId: unlockedEvent.id,
              error,
            },
            "error processing event",
          );
        }
      }),
    ),
  );
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

export const EventProcessor = <TxOBEventType extends string>(
  client: TxOBProcessorClient<TxOBEventType>,
  handlerMap: TxOBEventHandlerMap<TxOBEventType>,
  opts?: Omit<Partial<TxOBProcessEventsOpts<TxOBEventType>>, "signal"> & {
    sleepTimeMs?: number;
  },
) => {
  return Processor(
    ({ signal }) => {
      return processEvents(client, handlerMap, {
        ...opts,
        signal,
      });
    },
    {
      sleepTimeMs: opts?.sleepTimeMs,
      logger: opts?.logger,
    },
  );
};

class SignalAbortedError extends Error {
  constructor() {
    super("signal aborted while awaiting next processor tick");
  }
}

export const Processor = (
  fn: ({ signal }: { signal: AbortSignal }) => Promise<void>,
  opts?: { sleepTimeMs?: number; logger?: Logger },
) => {
  let state: "started" | "stopped" | "stopping" = "stopped";
  const ee = new EventEmitter();
  const ac = new AbortController();
  let shutdownCompleteEmitted = false;
  const _opts = {
    sleepTimeMs: opts?.sleepTimeMs ?? 5000,
  };

  return {
    start: () => {
      if (state !== "stopped") {
        opts?.logger?.warn(`cannot start processor from '${state}'`);
        return;
      }
      state = "started";
      opts?.logger?.debug("processor started");

      let abortListener: ((this: AbortSignal, ev: Event) => unknown) | null =
        null;

      (async () => {
        while (true) {
          opts?.logger?.debug("tick");
          try {
            await fn({ signal: ac.signal });

            await Promise.race([
              sleep(_opts.sleepTimeMs),
              new Promise((_, reject) => {
                if (ac.signal.aborted) return reject(new SignalAbortedError());

                abortListener = () => reject(new SignalAbortedError());
                ac.signal.addEventListener("abort", abortListener);
              }),
            ]);
          } catch (error) {
            if (error instanceof SignalAbortedError) {
              opts?.logger?.debug(error.message);
              break;
            } else {
              opts?.logger?.error(error);
              await sleep(1000);
            }
          } finally {
            if (abortListener)
              ac.signal.removeEventListener("abort", abortListener);
          }
        }

        ee.emit("shutdownComplete");
        shutdownCompleteEmitted = true;
      })();
    },
    stop: async (stopOpts?: { timeoutMs?: number }) => {
      if (state !== "started") {
        opts?.logger?.warn(`cannot stop processor from '${state}'`);
        return;
      }
      state = "stopping";
      opts?.logger?.debug("processor stopping");

      const _stopOpts = {
        timeoutMs: 10000,
        ...stopOpts,
      };

      let caughtErr;
      try {
        await Promise.race([
          new Promise<void>((resolve) => {
            if (shutdownCompleteEmitted) {
              opts?.logger?.debug("shutdownCompleteEmitted caught in shutdown");
              return resolve();
            }

            ee.once("shutdownComplete", () => {
              opts?.logger?.debug("shutdownComplete event caught in shutdown");
              resolve();
            });
            opts?.logger?.debug("shutdown aborting AbortController");
            ac.abort();
          }),
          new Promise((_, reject) => {
            sleep(_stopOpts.timeoutMs).then(() => {
              reject(
                new Error(`shutdown timeout ${_stopOpts.timeoutMs}ms elapsed`),
              );
            });
          }),
        ]);
      } catch (error) {
        caughtErr = error;
      }

      ee.removeAllListeners("shutdownComplete");
      state = "stopped";
      opts?.logger?.debug("processor stopped");

      if (caughtErr) {
        opts?.logger?.debug({ error: caughtErr }, "shutdown error");
        throw caughtErr;
      }
    },
  };
};
