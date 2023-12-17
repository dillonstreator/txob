import { Client } from 'pg';
import { OutboxEvent, OutboxProcessorClient } from '../processor';

export const createEventProcessorClient = <EventType extends string>(
    pgClient: Client
): OutboxProcessorClient<EventType> => ({
    getUnprocessedEvents: async (opts): Promise<OutboxEvent<EventType>[]> => {
        const events = await pgClient.query<OutboxEvent<EventType>>(
            'SELECT * FROM events WHERE processed_at IS NULL AND (backoff_until IS NULL OR backoff_until < NOW()) AND errors < $1',
            [opts.maxErrors]
        );
        return events.rows;
    },
    getEventByIdForUpdateSkipLocked: async (
        eventId
    ): Promise<OutboxEvent<EventType> | null> => {
        const event = await pgClient.query<OutboxEvent<EventType>>(
            `SELECT * FROM events WHERE id = $1 FOR UPDATE SKIP LOCKED`,
            [eventId]
        );
        if (event.rowCount === 0) {
            return null;
        }

        return event.rows[0];
    },
    updateEvent: async (event: OutboxEvent<EventType>): Promise<void> => {
        await pgClient.query(
            `UPDATE events SET handler_results = $1, errors = $2, processed_at = $3, backoff_until = $4 WHERE id = $5`,
            [
                event.handler_results,
                event.errors,
                event.processed_at,
                event.backoff_until,
                event.id,
            ]
        );
    },
});
