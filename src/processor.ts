import { retryable, RetryOpts } from "./retry";
import { getDate } from "./date";
import EventEmitter from "node:events";

type TxOBEventHandlerResult = {
  processed_at?: Date;
  errors?: { error: any; timestamp: Date }[];
};

export type TxOBEvent<TxOBEventType extends string> = {
  id: string;
  timestamp: Date;
  type: TxOBEventType;
  data: Record<string, any>;
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

type TxOBProcessorClientOpts = {
  signal?: AbortSignal;
};

export interface TxOBProcessorClient<TxOBEventType extends string> {
  getUnprocessedEvents(
    opts: TxOBProcessorClientOpts & { maxErrors: number },
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
}

const defaultBackoff = (errorCount: number): Date => {
  const baseDelayMs = 1000;
  const maxDelayMs = 1000 * 60;
  const backoffMs = Math.min(baseDelayMs * 2 ** errorCount, maxDelayMs);
  const retryTimestamp = new Date(Date.now() + backoffMs);

  return retryTimestamp;
};
const defaultMaxErrors = 5;

type TxOBProcessEventsOpts = {
  maxErrors: number;
  backoff: (count: number) => Date;
  retryOpts?: RetryOpts;
  signal?: AbortSignal;
  logger?: Logger;
};

export const processEvents = async <TxOBEventType extends string>(
  client: TxOBProcessorClient<TxOBEventType>,
  handlerMap: TxOBEventHandlerMap<TxOBEventType>,
  opts?: Partial<TxOBProcessEventsOpts>,
): Promise<void> => {
  const _opts: TxOBProcessEventsOpts = {
    maxErrors: defaultMaxErrors,
    backoff: defaultBackoff,
    ...opts,
  };

  const events = await client.getUnprocessedEvents(_opts);
  _opts.logger?.debug(`found ${events.length} events to process`);

  // TODO: consider concurrently processing events with max concurrency configuration
  for (const unlockedEvent of events) {
    if (_opts.signal?.aborted) {
      return;
    }
    if (unlockedEvent.errors >= _opts.maxErrors) {
      // Potential issue with client configuration on finding unprocessed events
      // Events with maximum allowed errors should not be returned from `getUnprocessedEvents`
      _opts.logger?.warn(
        "unexpected event with max errors returned from `getUnprocessedEvents`",
        {
          eventId: unlockedEvent.id,
          errors: unlockedEvent.errors,
          maxErrors: _opts.maxErrors,
        },
      );
      continue;
    }

    try {
      await client.transaction(async (txClient) => {
        const lockedEvent = await txClient.getEventByIdForUpdateSkipLocked(
          unlockedEvent.id,
          { signal: _opts.signal },
        );
        if (!lockedEvent) {
          _opts.logger?.debug("skipping locked event", {
            eventId: unlockedEvent.id,
          });
          return;
        }
        if (lockedEvent.processed_at) {
          // While unlikely, this is possible if a concurrent processor finished processing this event between the time
          // that this processor found the event with `getUnprocessedEvents` and called `getEventByIdForUpdateSkipLocked`
          _opts.logger?.debug("skipping already processed event", {
            eventId: lockedEvent.id,
          });
          return;
        }

        let errored = false;

        let eventHandlerMap = handlerMap[lockedEvent.type];
        if (!eventHandlerMap) {
          _opts.logger?.warn("missing event handler map", {
            type: lockedEvent.type,
          });
          errored = true;
          lockedEvent.errors = _opts.maxErrors;
          eventHandlerMap = {};
        }

        _opts.logger?.debug(`processing event`, { eventId: lockedEvent.id });

        // TODO: consider concurrently processing events handler with max concurrency configuration
        await Promise.allSettled(
          Object.entries(eventHandlerMap).map(
            async ([handlerName, handler]): Promise<void> => {
              const handlerResults =
                lockedEvent.handler_results[handlerName] ?? {};
              if (handlerResults.processed_at) {
                _opts.logger?.debug("handler already processed", {
                  eventId: lockedEvent.id,
                  handlerName,
                });
                return;
              }

              handlerResults.errors ??= [];

              try {
                await handler(lockedEvent, { signal: _opts.signal });
                handlerResults.processed_at = getDate();
                _opts.logger?.debug("handler succeeded", {
                  eventId: lockedEvent.id,
                  handlerName,
                });
              } catch (error) {
                _opts.logger?.error("handler errored", {
                  eventId: lockedEvent.id,
                  handlerName,
                  error,
                });
                errored = true;
                handlerResults.errors?.push({
                  error: (error as Error)?.message ?? error,
                  timestamp: getDate(),
                });
              }

              lockedEvent.handler_results[handlerName] = handlerResults;
            },
          ),
        );

        if (errored) {
          lockedEvent.errors = Math.min(
            lockedEvent.errors + 1,
            _opts.maxErrors,
          );
          lockedEvent.backoff_until = _opts.backoff(lockedEvent.errors);
          if (lockedEvent.errors === _opts.maxErrors) {
            lockedEvent.backoff_until = null;
          }
        } else {
          lockedEvent.backoff_until = null;
          lockedEvent.processed_at = getDate();
        }

        _opts.logger?.debug("updating event", { errored, lockedEvent });

        // The success of this update is crucial for the processor flow.
        // In the event of a failure, any handlers that have successfully executed
        // during this invokation will be reinvoked in the subsequent call.
        await retryable(() => txClient.updateEvent(lockedEvent), {
          retries: 3,
          factor: 2,
          minTimeout: 100,
          maxTimeout: 2500,
          randomize: true,
          ...(_opts.retryOpts ?? {}),
        });
      });
    } catch (error) {
      _opts.logger?.error("error processing event", {
        eventId: unlockedEvent.id,
        error,
      });
    }
  }
};

export interface Logger {
  debug(message?: any, ...optionalParams: any[]): void;
  info(message?: any, ...optionalParams: any[]): void;
  warn(message?: any, ...optionalParams: any[]): void;
  error(message?: any, ...optionalParams: any[]): void;
}

export const EventProcessor = <TxOBEventType extends string>(
  client: TxOBProcessorClient<TxOBEventType>,
  handlerMap: TxOBEventHandlerMap<TxOBEventType>,
  opts?: Omit<Partial<TxOBProcessEventsOpts>, "signal"> & {
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

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

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

      let abortListener: ((this: AbortSignal, ev: Event) => any) | null = null;

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
            } else {
              opts?.logger?.error(error);
            }

            break;
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
        opts?.logger?.debug("shutdown error", caughtErr);
        throw caughtErr;
      }
    },
  };
};
