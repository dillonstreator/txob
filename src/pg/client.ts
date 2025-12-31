import {
  Client,
  escapeIdentifier,
  escapeLiteral,
  type ClientConfig,
  DatabaseError,
} from "pg";
import type {
  TxOBEvent,
  TxOBProcessorClient,
  TxOBProcessorClientOpts,
  TxOBTransactionProcessorClient,
  WakeupEmitter,
} from "../processor.js";
import { EventEmitter } from "node:events";

interface Querier {
  query: Client["query"];
}

// TODO: leverage the signal option that comes in on options for `getEventsToProcess` and `getEventByIdForUpdateSkipLocked`
// to cancel queries if/when supported by `pg` https://github.com/brianc/node-postgres/issues/2774

export type CreateProcessorClientOpts<EventType extends string> = {
  querier: Querier;
  table?: string;
  limit?: number;
};

export const createProcessorClient = <EventType extends string>(
  opts: CreateProcessorClientOpts<EventType>,
): TxOBProcessorClient<EventType> => {
  const { querier, table = "events", limit = 100 } = opts;
  const _table = table;
  const _limit = limit;
  const getEventsToProcess = async (
    opts: TxOBProcessorClientOpts,
  ): Promise<Pick<TxOBEvent<EventType>, "id" | "errors">[]> => {
    const events = await querier.query<
      Pick<TxOBEvent<EventType>, "id" | "errors">
    >(
      `SELECT id, errors FROM ${escapeIdentifier(_table)} WHERE processed_at IS NULL AND (backoff_until IS NULL OR backoff_until < NOW()) AND errors < $1 ORDER BY timestamp ASC LIMIT ${_limit}`,
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
            `SELECT id, timestamp, type, data, correlation_id, handler_results, errors, backoff_until, processed_at FROM ${escapeIdentifier(_table)} WHERE id = $1 AND processed_at IS NULL AND (backoff_until IS NULL OR backoff_until < NOW()) AND errors < $2 FOR UPDATE SKIP LOCKED`,
            [eventId, opts.maxErrors],
          );
          if (event.rowCount === 0) {
            return null;
          }

          return event.rows[0];
        },
        updateEvent: async (event: TxOBEvent<EventType>): Promise<void> => {
          await querier.query(
            `UPDATE ${escapeIdentifier(_table)} SET handler_results = $1, errors = $2, processed_at = $3, backoff_until = $4 WHERE id = $5`,
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
            `INSERT INTO ${escapeIdentifier(_table)} (id, timestamp, type, data, correlation_id, handler_results, errors) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
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

type CreateWakeupEmitterOpts =
  | {
      listenClientConfig: ClientConfig;
      channel?: string;
      createTrigger: true;
      table?: string;
      querier: Querier;
    }
  | {
      listenClientConfig: ClientConfig;
      channel?: string;
      createTrigger?: false;
      table?: string;
      querier?: Querier;
    };

/**
 * Creates a Postgres NOTIFY-based wakeup emitter for reducing polling frequency.
 * This uses a separate connection for LISTEN, as you cannot LISTEN on a connection
 * that's used for queries.
 *
 * @param opts - Options for the wakeup emitter. If `createTrigger` is `true`, `querier` is required.
 * @returns A WakeupEmitter that emits 'wakeup' events when Postgres NOTIFY is received
 */
export const createWakeupEmitter = async (
  opts: CreateWakeupEmitterOpts,
): Promise<WakeupEmitter> => {
  const { listenClientConfig, channel = "txob_events", table = "events" } = opts;
  const emitter = new EventEmitter();

  // Create a separate client for LISTEN
  const listenClient = new Client(listenClientConfig);
  await listenClient.connect();

  // Set up LISTEN - channel names are identifiers
  // Note: Postgres channel names are case-insensitive and converted to lowercase
  const listenChannel = channel.toLowerCase();
  await listenClient.query(`LISTEN ${escapeIdentifier(listenChannel)}`);

  // Handle notifications
  listenClient.on("notification", (msg) => {
    // msg.channel is already lowercase from Postgres
    if (msg.channel === listenChannel) {
      emitter.emit("wakeup");
    }
  });

  // Handle connection errors
  listenClient.on("error", (err) => {
    emitter.emit("error", err);
  });

  // Handle disconnection
  listenClient.on("end", () => {
    emitter.emit("error", new Error("Postgres LISTEN connection ended"));
  });

  // Create trigger if requested
  if (opts.createTrigger && opts.querier) {
    await createWakeupTrigger({ querier: opts.querier, table, channel: listenChannel });
  }

  // Return a WakeupEmitter that wraps the EventEmitter
  return {
    on: (event: "wakeup", listener: () => void) => {
      emitter.on(event, listener);
    },
    off: (event: "wakeup", listener: () => void) => {
      emitter.off(event, listener);
      // If no more listeners, we could optionally close the connection
      // But we'll leave it open for potential re-use
    },
    // Expose cleanup method (not part of interface but useful)
    close: async () => {
      await listenClient.query(`UNLISTEN ${escapeIdentifier(listenChannel)}`);
      await listenClient.end();
    },
  } as WakeupEmitter & { close: () => Promise<void> };
};

type CreateWakeupTriggerOpts = {
  querier: Querier;
  table?: string;
  channel?: string;
};

/**
 * Creates a Postgres trigger that sends NOTIFY when events are inserted.
 * Wakeup signals are primarily for new events - retries after backoff are handled
 * by fallback polling.
 *
 * This function is safe for concurrent execution - multiple processes can call it
 * simultaneously without errors. It gracefully handles cases where the trigger
 * already exists by catching and ignoring duplicate object errors.
 *
 * @param opts - Options for the wakeup trigger
 * @returns Promise that resolves when the trigger is created
 */
export const createWakeupTrigger = async (
  opts: CreateWakeupTriggerOpts,
): Promise<void> => {
  const { querier, table = "events", channel = "txob_events" } = opts;
  const triggerName = `txob_wakeup_trigger_${table}`;
  const functionName = `txob_wakeup_notify_${table}`;

  try {
    // Create the function that sends NOTIFY
    // CREATE OR REPLACE is safe for concurrent execution
    // Note: channel name is embedded in the function body using escapeLiteral for safety
    // pg_notify expects a text parameter, so we use escapeLiteral to safely embed the channel name
    const channelLiteral = escapeLiteral(channel);
    await querier.query(`
      CREATE OR REPLACE FUNCTION ${escapeIdentifier(functionName)}()
      RETURNS TRIGGER AS $$
      BEGIN
        -- Send NOTIFY when a new event is inserted
        -- Only trigger on INSERT - retries after backoff are handled by fallback polling
        IF TG_OP = 'INSERT' THEN
          PERFORM pg_notify(${channelLiteral}, '');
        END IF;
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;
    `);

    // Try to create the trigger
    // If it already exists, PostgreSQL will throw a duplicate_object error (42710)
    // which we'll catch and ignore below
    await querier.query(`
      CREATE TRIGGER ${escapeIdentifier(triggerName)}
      AFTER INSERT ON ${escapeIdentifier(table)}
      FOR EACH ROW
      EXECUTE FUNCTION ${escapeIdentifier(functionName)}();
    `);
  } catch (error: unknown) {
    // Handle errors that can occur during concurrent trigger creation
    // Note: pg errors may not always be DatabaseError instances, but they have a 'code' field
    const errorCode =
      typeof error === "object" &&
      error !== null &&
      "code" in error &&
      typeof error.code === "string"
        ? error.code
        : undefined;
    const errorMessage = error instanceof Error ? error.message : String(error);

    // 42710: duplicate_object - trigger already exists
    // This is safe to ignore - another process already created it
    if (errorCode === "42710") {
      return;
    }
    // XX000: internal_error - can include "tuple concurrently updated"
    // This happens when multiple processes try to modify system catalogs simultaneously
    // It's safe to ignore - one process will succeed, others will get this error
    if (
      errorCode === "XX000" &&
      errorMessage.includes("tuple concurrently updated")
    ) {
      return;
    }
    // Also check error message as fallback for duplicate trigger errors
    // Sometimes the error code might not be set correctly
    if (
      errorMessage.includes("already exists") &&
      (errorMessage.includes("trigger") || errorMessage.includes("relation"))
    ) {
      return;
    }
    // Re-throw other errors
    throw error;
  }
};
