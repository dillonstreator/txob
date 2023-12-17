import { vi, describe, it, expect } from 'vitest';
import { createEventProcessorClient } from './client';

describe('createEventProcessorClient', () => {
    it('should create a client with the correct functions', () => {
        const pgClient = {
            query: vi.fn(),
        };
        const client = createEventProcessorClient(pgClient);
        expect(typeof client.getUnprocessedEvents).toBe('function');
        expect(typeof client.getEventByIdForUpdateSkipLocked).toBe('function');
        expect(typeof client.updateEvent).toBe('function');
    });
});

describe('getUnprocessedEvents', () => {
    it('should execute the correct query', async () => {
        const rows = [1, 2, 3];
        const pgClient = {
            query: vi.fn<any, any>(() =>
                Promise.resolve({
                    rows,
                })
            ),
        };
        const opts = {
            maxErrors: 10,
        };
        const client = createEventProcessorClient(pgClient);
        const result = await client.getUnprocessedEvents(opts);
        expect(pgClient.query).toHaveBeenCalledOnce();
        expect(pgClient.query).toHaveBeenCalledWith(
            'SELECT id, errors FROM events WHERE processed_at IS NULL AND (backoff_until IS NULL OR backoff_until < NOW()) AND errors < $1',
            [opts.maxErrors]
        );
        expect(result).toBe(rows);
    });
});

describe('getEventByIdForUpdateSkipLocked', () => {
    it('should execute the correct query', async () => {
        const rows = [1, 2, 3];
        const pgClient = {
            query: vi.fn<any, any>(() =>
                Promise.resolve({
                    rows,
                    rowCount: rows.length,
                })
            ),
        };
        const eventId = '123';
        const client = createEventProcessorClient(pgClient);
        const result = await client.getEventByIdForUpdateSkipLocked(
            eventId,
            {}
        );
        expect(pgClient.query).toHaveBeenCalledOnce();
        expect(pgClient.query).toHaveBeenCalledWith(
            'SELECT * FROM events WHERE id = $1 FOR UPDATE SKIP LOCKED',
            [eventId]
        );
        expect(result).toBe(1);
    });

    it('should return null on no rows', async () => {
        const rows = [];
        const pgClient = {
            query: vi.fn<any, any>(() =>
                Promise.resolve({
                    rows,
                    rowCount: rows.length,
                })
            ),
        };
        const eventId = '123';
        const client = createEventProcessorClient(pgClient);
        const result = await client.getEventByIdForUpdateSkipLocked(
            eventId,
            {}
        );
        expect(pgClient.query).toHaveBeenCalledOnce();
        expect(pgClient.query).toHaveBeenCalledWith(
            'SELECT * FROM events WHERE id = $1 FOR UPDATE SKIP LOCKED',
            [eventId]
        );
        expect(result).toBeNull();
    });
});

describe('updateEvent', () => {
    it('should execute the correct query', () => {
        const rows = [1, 2, 3];
        const pgClient = {
            query: vi.fn<any, any>(() =>
                Promise.resolve({
                    rows,
                })
            ),
        };
        const event = {
            id: '1',
            handler_results: {},
            errors: 2,
            processed_at: new Date(),
            backoff_until: new Date(),
            timestamp: new Date(),
            type: 'type',
            data: {
                thing1: 'something',
            },
            correlation_id: 'abc123',
        };
        const client = createEventProcessorClient(pgClient);
        client.updateEvent(event);
        expect(pgClient.query).toHaveBeenCalledOnce();
        expect(pgClient.query).toHaveBeenCalledWith(
            'UPDATE events SET handler_results = $1, errors = $2, processed_at = $3, backoff_until = $4 WHERE id = $5',
            [
                event.handler_results,
                event.errors,
                event.processed_at,
                event.backoff_until,
                event.id,
            ]
        );
    });
});
