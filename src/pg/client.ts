import { Client, escapeIdentifier } from "pg";
import type {
  TxOBEvent,
  TxOBProcessorClient,
  TxOBProcessorClientOpts,
  TxOBTransactionProcessorClient,
} from "../processor.js";

interface Querier {
  query: Client["query"];
}

// TODO: leverage the signal option that comes in on options for `getEventsToProcess` and `getEventByIdForUpdateSkipLocked`
// to cancel queries if/when supported by `pg` https://github.com/brianc/node-postgres/issues/2774

export const createProcessorClient = <EventType extends string>(
  querier: Querier,
  table: string = "events",
  limit: number = 100,
): TxOBProcessorClient<EventType> => {
  const getEventsToProcess = async (
    opts: TxOBProcessorClientOpts,
  ): Promise<Pick<TxOBEvent<EventType>, "id" | "errors">[]> => {
    const events = await querier.query<
      Pick<TxOBEvent<EventType>, "id" | "errors">
    >(
      `SELECT id, errors FROM ${escapeIdentifier(table)} WHERE processed_at IS NULL AND (backoff_until IS NULL OR backoff_until < NOW()) AND errors < $1 ORDER BY timestamp ASC LIMIT ${limit}`,
      [opts.maxErrors],
    );
    return events.rows;
  };

  const transaction: TxOBProcessorClient<EventType>["transaction"] = async (
    fn: (
      txProcessorClient: TxOBTransactionProcessorClient<EventType>,
    ) => Promise<void>,
  ): Promise<void> => {
    try {
      await querier.query("BEGIN");
      await fn({
        getEventByIdForUpdateSkipLocked: async (
          eventId: TxOBEvent<EventType>["id"],
          opts: TxOBProcessorClientOpts,
        ): Promise<TxOBEvent<EventType> | null> => {
          const event = await querier.query<TxOBEvent<EventType>>(
            `SELECT id, timestamp, type, data, correlation_id, handler_results, errors, backoff_until, processed_at FROM ${escapeIdentifier(table)} WHERE id = $1 AND processed_at IS NULL AND (backoff_until IS NULL OR backoff_until < NOW()) AND errors < $2 FOR UPDATE SKIP LOCKED`,
            [eventId, opts.maxErrors],
          );
          if (event.rowCount === 0) {
            return null;
          }

          return event.rows[0];
        },
        updateEvent: async (event: TxOBEvent<EventType>): Promise<void> => {
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
        createEvent: async (
          event: Omit<TxOBEvent<EventType>, "processed_at" | "backoff_until">,
        ): Promise<void> => {
          await querier.query(
            `INSERT INTO ${escapeIdentifier(table)} (id, timestamp, type, data, correlation_id, handler_results, errors) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
            [
              event.id,
              event.timestamp,
              event.type,
              event.data,
              event.correlation_id,
              event.handler_results,
              event.errors,
            ],
          );
        },
      });
      await querier.query("COMMIT");
    } catch (error) {
      try {
        await querier.query("ROLLBACK");
      } catch (rollbackError) {
        const message = error instanceof Error ? error.message : String(error);
        const rollbackMessage =
          rollbackError instanceof Error
            ? rollbackError.message
            : String(rollbackError);

        throw new Error(
          `Transaction failed: ${message} (rollback also failed: ${rollbackMessage})`,
          { cause: error },
        );
      }

      throw error;
    }
  };

  return {
    getEventsToProcess,
    transaction,
  };
};
