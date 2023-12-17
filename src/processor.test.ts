import { describe, it, expect, vi, afterEach, beforeEach } from 'vitest';
import { OutboxEvent, processEvents } from './processor';

const mockClient = {
    getUnprocessedEvents: vi.fn(),
    getEventByIdForUpdateSkipLocked: vi.fn(),
    updateEvent: vi.fn(),
};

beforeEach(() => {
    vi.useFakeTimers();
});
afterEach(() => {
    vi.clearAllMocks();
});

describe('processEvents', () => {
    it('does nothing when no events to process', () => {
        const opts = {
            maxErrors: 5,
            backoff: () => new Date(),
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
        const date = new Date();
        vi.setSystemTime(date);

        const opts = {
            maxErrors: 5,
            backoff: vi.fn(),
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
            timestamp: new Date(),
            data: {},
            correlation_id: '',
            handler_results: {
                handler1: {
                    errors: [{ error: err.message, timestamp: new Date() }],
                },
                handler3: {
                    processed_at: new Date(),
                },
            },
            errors: 0,
        };
        const events = [evt1];
        mockClient.getUnprocessedEvents.mockImplementation(() => events);
        mockClient.getEventByIdForUpdateSkipLocked.mockImplementation((id) =>
            events.find((e) => e.id === id)
        );

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

        expect(mockClient.updateEvent).toHaveBeenCalledOnce();
        expect(mockClient.updateEvent).toHaveBeenCalledWith({
            backoff_until: undefined,
            correlation_id: '',
            data: {},
            errors: 1,
            handler_results: {
                handler1: {
                    errors: [
                        {
                            error: err.message,
                            timestamp: date,
                        },
                    ],
                    processed_at: date,
                },
                handler2: {
                    errors: [
                        {
                            error: err.message,
                            timestamp: date,
                        },
                    ],
                },
                handler3: {
                    processed_at: date,
                },
            },
            id: '1',
            timestamp: date,
            type: 'evtType1',
        });
    });
});
