import { Client, escapeIdentifier } from "pg";
import { TxOBEvent, TxOBProcessorClient } from "../processor";

interface Querier {
  query: Client["query"];
}

// TODO: leverage the signal option that comes in on options for `getEventsToProcess` and `getEventByIdForUpdateSkipLocked`
// to cancel queries if/when supported by `pg` https://github.com/brianc/node-postgres/issues/2774

export const createProcessorClient = <EventType extends string>(
  querier: Querier,
  table: string = "events",
): TxOBProcessorClient<EventType> => ({
  getEventsToProcess: async (opts) => {
    const events = await querier.query<
      Pick<TxOBEvent<EventType>, "id" | "errors">
    >(
      `SELECT id, errors FROM ${escapeIdentifier(table)} WHERE processed_at IS NULL AND (backoff_until IS NULL OR backoff_until < NOW()) AND errors < $1`,
      [opts.maxErrors],
    );
    return events.rows;
  },
  transaction: async (fn) => {
    try {
      await querier.query("BEGIN");
      await fn({
        getEventByIdForUpdateSkipLocked: async (eventId, opts) => {
          const event = await querier.query<TxOBEvent<EventType>>(
            `SELECT id, timestamp, type, data, correlation_id, handler_results, errors, backoff_until, processed_at FROM ${escapeIdentifier(table)} WHERE id = $1 AND processed_at IS NULL AND (backoff_until IS NULL OR backoff_until < NOW()) AND errors < $2 FOR UPDATE SKIP LOCKED`,
            [eventId, opts.maxErrors],
          );
          if (event.rowCount === 0) {
            return null;
          }

          return event.rows[0];
        },
        updateEvent: async (event) => {
          await querier.query(
            `UPDATE ${escapeIdentifier(table)} SET handler_results = $1, errors = $2, processed_at = $3, backoff_until = $4 WHERE id = $5`,
            [
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
      await querier.query("ROLLBACK").catch(() => { });
      throw error;
    }
  },
});
