import { Client } from "pg";
import { TxOBEvent, TxOBProcessorClient } from "../processor";

interface Querier {
  query: Client["query"];
}

export const createProcessorClient = <EventType extends string>(
  querier: Querier,
  table: string = "events",
): TxOBProcessorClient<EventType> => ({
  getUnprocessedEvents: async (opts) => {
    const events = await querier.query<
      Pick<TxOBEvent<EventType>, "id" | "errors">
    >(
      "SELECT id, errors FROM $1 WHERE processed_at IS NULL AND (backoff_until IS NULL OR backoff_until < NOW()) AND errors < $2",
      [table, opts.maxErrors],
    );
    return events.rows;
  },
  transaction: async (fn) => {
    try {
      await querier.query("BEGIN");
      await fn({
        getEventByIdForUpdateSkipLocked: async (eventId) => {
          const event = await querier.query<TxOBEvent<EventType>>(
            `SELECT id, timestamp, type, data, correlation_id, handler_results, errors, backoff_until, processed_at FROM $1 WHERE id = $2 FOR UPDATE SKIP LOCKED`,
            [table, eventId],
          );
          if (event.rowCount === 0) {
            return null;
          }

          return event.rows[0];
        },
        updateEvent: async (event) => {
          await querier.query(
            `UPDATE $1 SET handler_results = $2, errors = $3, processed_at = $4, backoff_until = $5 WHERE id = $6`,
            [
              table,
              event.handler_results,
              event.errors,
              event.processed_at,
              event.backoff_until,
              event.id,
            ],
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
