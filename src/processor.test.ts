import { describe, it, expect, vi, afterEach } from 'vitest';
import { OutboxEvent, processEvents } from './processor';

const mockClient = {
    getUnprocessedEvents: vi.fn(),
    getEventByIdForUpdateSkipLocked: vi.fn(),
    updateEvent: vi.fn(),
};

const now = new Date();
vi.mock('./date', async (getOg) => {
    const mod = await getOg();
    return {
        ...(mod as Object),
        getDate: vi.fn(() => now),
    };
});

afterEach(() => {
    vi.clearAllMocks();
});

describe('processEvents', () => {
    it('does nothing when no events to process', () => {
        const opts = {
            maxErrors: 5,
            backoff: () => now,
        };
        const handlerMap = {};
        mockClient.getUnprocessedEvents.mockImplementation(() => []);
        processEvents(mockClient, handlerMap, opts);
        expect(mockClient.getUnprocessedEvents).toHaveBeenCalledOnce();
        expect(mockClient.getUnprocessedEvents).toHaveBeenCalledWith(opts);
        expect(
            mockClient.getEventByIdForUpdateSkipLocked
        ).not.toHaveBeenCalled();
        expect(mockClient.updateEvent).not.toHaveBeenCalled();
    });

    it('handles handler results and updates', async () => {
        const opts = {
            maxErrors: 5,
            backoff: vi.fn(),
            retry: {
                minTimeout: 50,
                maxTimeout: 100,
            },
        };
        const err = new Error('some error');
        const handlerMap = {
            evtType1: {
                handler1: vi.fn(() => Promise.resolve()),
                handler2: vi.fn(() => Promise.reject(err)),
                handler3: vi.fn(() => Promise.resolve()),
            },
        };
        const evt1: OutboxEvent<keyof typeof handlerMap> = {
            type: 'evtType1',
            id: '1',
            timestamp: now,
            data: {},
            correlation_id: 'abc123',
            handler_results: {
                handler1: {
                    errors: [{ error: err.message, timestamp: now }],
                },
                handler3: {
                    processed_at: now,
                },
            },
            errors: 0,
        };
        const events = [evt1];
        mockClient.getUnprocessedEvents.mockImplementation(() => events);
        mockClient.getEventByIdForUpdateSkipLocked.mockImplementation((id) =>
            events.find((e) => e.id === id)
        );
        let updateEventCalls = 0;
        mockClient.updateEvent.mockImplementation(() => {
            updateEventCalls++;
            if (updateEventCalls <= 1) return Promise.reject('some error');
            else return Promise.resolve();
        });

        await processEvents(mockClient, handlerMap, opts);

        expect(mockClient.getUnprocessedEvents).toHaveBeenCalledOnce();
        expect(mockClient.getUnprocessedEvents).toHaveBeenCalledWith(opts);

        expect(handlerMap.evtType1.handler1).toHaveBeenCalledOnce();
        expect(handlerMap.evtType1.handler1).toHaveBeenCalledWith(evt1, {
            signal: undefined,
        });
        expect(handlerMap.evtType1.handler2).toHaveBeenCalledOnce();
        expect(handlerMap.evtType1.handler2).toHaveBeenCalledWith(evt1, {
            signal: undefined,
        });
        expect(handlerMap.evtType1.handler3).not.toHaveBeenCalled();

        expect(
            mockClient.getEventByIdForUpdateSkipLocked
        ).toHaveBeenCalledOnce();

        expect(opts.backoff).toHaveBeenCalledOnce();
        expect(opts.backoff).toHaveBeenCalledWith(1); // evt.errors + 1

        expect(mockClient.updateEvent).toHaveBeenCalledTimes(2);
        expect(mockClient.updateEvent).toHaveBeenCalledWith({
            backoff_until: undefined,
            correlation_id: 'abc123',
            data: {},
            errors: 1,
            handler_results: {
                handler1: {
                    errors: [
                        {
                            error: err.message,
                            timestamp: now,
                        },
                    ],
                    processed_at: now,
                },
                handler2: {
                    errors: [
                        {
                            error: err.message,
                            timestamp: now,
                        },
                    ],
                },
                handler3: {
                    processed_at: now,
                },
            },
            id: '1',
            timestamp: now,
            type: 'evtType1',
        });
    });
});
