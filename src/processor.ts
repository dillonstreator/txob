import retry from 'retry';

type OutboxEventHandlerResult = {
    processed_at?: Date;
    errors?: { error: any; timestamp: Date }[];
};

export type OutboxEvent<OutboxEventType extends string> = {
    id: string;
    timestamp: Date;
    type: OutboxEventType;
    data: Record<string, any>;
    correlation_id: string;
    handler_results: Record<string, OutboxEventHandlerResult>;
    errors: number;
    backoff_until?: Date | null;
    processed_at?: Date;
};

type OutboxEventHandlerOpts = {
    signal?: AbortSignal;
};

export type OutboxEventHandler = <OutboxEventType extends string>(
    event: OutboxEvent<OutboxEventType>,
    opts: OutboxEventHandlerOpts
) => Promise<void>;

export type OutboxEventHandlerMap<OutboxEventType extends string> = Record<
    OutboxEventType,
    {
        [key: string]: OutboxEventHandler;
    }
>;

type OutboxProcessorClientOpts = {
    signal?: AbortSignal;
};

export interface OutboxProcessorClient<OutboxEventType extends string> {
    getUnprocessedEvents(
        opts: OutboxProcessorClientOpts & { maxErrors: number }
    ): Promise<OutboxEvent<OutboxEventType>[]>;
    getEventByIdForUpdateSkipLocked(
        eventId: OutboxEvent<OutboxEventType>['id'],
        opts: OutboxProcessorClientOpts
    ): Promise<OutboxEvent<OutboxEventType> | null>;
    updateEvent(event: OutboxEvent<OutboxEventType>): Promise<void>;
}

const defaultBackoff = (errorCount: number): Date => {
    const baseDelayMs = 1000;
    const maxDelayMs = 1000 * 60;
    const backoffMs = Math.min(baseDelayMs * 2 ** errorCount, maxDelayMs);
    const retryTimestamp = new Date(Date.now() + backoffMs);

    return retryTimestamp;
};
const defaultMaxErrors = 5;

type OutboxProcessorOpts = {
    signal?: AbortSignal;
    maxErrors: number;
    backoff: (count: number) => Date;
    retry?: retry.OperationOptions;
};

export const processEvents = async <OutboxEventType extends string>(
    client: OutboxProcessorClient<OutboxEventType>,
    handlerMap: OutboxEventHandlerMap<OutboxEventType>,
    opts: OutboxProcessorOpts = {
        maxErrors: defaultMaxErrors,
        backoff: defaultBackoff,
    }
): Promise<void> => {
    const events = await client.getUnprocessedEvents(opts);
    if (events.length === 0) {
        return;
    }

    for (const unlockedEvent of events) {
        if (opts.signal?.aborted) {
            return;
        }

        const lockedEvent = await client.getEventByIdForUpdateSkipLocked(
            unlockedEvent.id,
            opts
        );
        if (!lockedEvent) {
            continue;
        }

        let errored = false;

        let eventHandlerMap = handlerMap[lockedEvent.type];
        if (!eventHandlerMap) {
            errored = true;
            lockedEvent.errors = opts.maxErrors;
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
                        await handler(lockedEvent, opts);
                        handlerResults.processed_at = new Date();
                    } catch (error) {
                        errored = true;
                        handlerResults.errors?.push({
                            error: (error as Error)?.message ?? error,
                            timestamp: new Date(),
                        });
                    }

                    lockedEvent.handler_results[handlerName] = handlerResults;
                }
            )
        );

        if (errored) {
            lockedEvent.errors = Math.min(
                lockedEvent.errors + 1,
                opts.maxErrors
            );
            lockedEvent.backoff_until = opts.backoff(lockedEvent.errors);
            if (lockedEvent.errors === opts.maxErrors) {
                lockedEvent.backoff_until = null;
            }
        } else {
            lockedEvent.backoff_until = null;
            lockedEvent.processed_at = new Date();
        }

        await retryable(() => client.updateEvent(lockedEvent), {
            retries: 3,
            factor: 2,
            minTimeout: 250,
            maxTimeout: 2500,
            randomize: true,
            ...(opts.retry ?? {}),
        });
    }
};

export function retryable<T>(
    action: () => Promise<T>,
    options?: retry.OperationOptions
): Promise<T> {
    return new Promise((resolve, reject) => {
        const op = retry.operation(options);

        op.attempt(async () => {
            try {
                resolve(await action());
            } catch (err) {
                if (!op.retry(err as Error)) reject(err);
            }
        });
    });
}
