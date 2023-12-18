import { Client } from "pg";
import { TxOBEvent, TxOBProcessorClient } from "../processor";

interface Querier {
  query: Client["query"];
}

export const createProcessorClient = <EventType extends string>(
  querier: Querier
): TxOBProcessorClient<EventType> => ({
  getUnprocessedEvents: async (opts) => {
    const events = await querier.query<
      Pick<TxOBEvent<EventType>, "id" | "errors">
    >(
      "SELECT id, errors FROM events WHERE processed_at IS NULL AND (backoff_until IS NULL OR backoff_until < NOW()) AND errors < $1",
      [opts.maxErrors]
    );
    return events.rows;
  },
  transaction: async (fn) => {
    try {
      await querier.query("BEGIN");
      await fn({
        getEventByIdForUpdateSkipLocked: async (eventId) => {
          const event = await querier.query<TxOBEvent<EventType>>(
            `SELECT * FROM events WHERE id = $1 FOR UPDATE SKIP LOCKED`,
            [eventId]
          );
          if (event.rowCount === 0) {
            return null;
          }

          return event.rows[0];
        },
        updateEvent: async (event) => {
          await querier.query(
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
      await querier.query("COMMIT");
    } catch (error) {
      await querier.query("ROLLBACK").catch(() => {});
      throw error;
    }
  },
});
