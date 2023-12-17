import { retryable, RetryOpts } from './retry';
import { getDate } from './date';

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
    opts: TxOBEventHandlerOpts
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
        opts: TxOBProcessorClientOpts & { maxErrors: number }
    ): Promise<Pick<TxOBEvent<TxOBEventType>, 'id' | 'errors'>[]>;
    getEventByIdForUpdateSkipLocked(
        eventId: TxOBEvent<TxOBEventType>['id'],
        opts: TxOBProcessorClientOpts
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
};

export const processEvents = async <TxOBEventType extends string>(
    client: TxOBProcessorClient<TxOBEventType>,
    handlerMap: TxOBEventHandlerMap<TxOBEventType>,
    opts?: Partial<TxOBProcessEventsOpts>
): Promise<void> => {
    const _opts: TxOBProcessEventsOpts = {
        maxErrors: defaultMaxErrors,
        backoff: defaultBackoff,
        ...opts,
    };

    const events = await client.getUnprocessedEvents(_opts);
    if (events.length === 0) {
        return;
    }

    for (const unlockedEvent of events) {
        if (_opts.signal?.aborted) {
            return;
        }
        if (unlockedEvent.errors >= _opts.maxErrors) {
            // TODO: log potential issue with client configuration on finding unprocessed events
            // events that have reached the maximum allowed errors should not be returned from `getUnprocessedEvents`
            continue;
        }

        const lockedEvent = await client.getEventByIdForUpdateSkipLocked(
            unlockedEvent.id,
            _opts
        );
        if (!lockedEvent) {
            continue;
        }

        let errored = false;

        let eventHandlerMap = handlerMap[lockedEvent.type];
        if (!eventHandlerMap) {
            errored = true;
            lockedEvent.errors = _opts.maxErrors;
            eventHandlerMap = {};
        }

        await Promise.allSettled(
            Object.entries(eventHandlerMap).map(
                async ([handlerName, handler]): Promise<void> => {
                    const handlerResults =
                        lockedEvent.handler_results[handlerName] ?? {};
                    if (handlerResults.processed_at) {
                        return;
                    }

                    handlerResults.errors ??= [];

                    try {
                        await handler(lockedEvent, { signal: _opts.signal });
                        handlerResults.processed_at = getDate();
                    } catch (error) {
                        errored = true;
                        handlerResults.errors?.push({
                            error: (error as Error)?.message ?? error,
                            timestamp: getDate(),
                        });
                    }

                    lockedEvent.handler_results[handlerName] = handlerResults;
                }
            )
        );

        if (errored) {
            lockedEvent.errors = Math.min(
                lockedEvent.errors + 1,
                _opts.maxErrors
            );
            lockedEvent.backoff_until = _opts.backoff(lockedEvent.errors);
            if (lockedEvent.errors === _opts.maxErrors) {
                lockedEvent.backoff_until = null;
            }
        } else {
            lockedEvent.backoff_until = null;
            lockedEvent.processed_at = getDate();
        }

        // The success of this update is crucial for the processor flow.
        // In the event of a failure, any handlers that have successfully executed
        // during this processor tick will be reinvoked in the subsequent tick.
        await retryable(() => client.updateEvent(lockedEvent), {
            retries: 3,
            factor: 2,
            minTimeout: 100,
            maxTimeout: 2500,
            randomize: true,
            ...(_opts.retryOpts ?? {}),
        });
    }
};
