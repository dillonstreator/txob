import pg from "pg";
import { randomUUID } from "node:crypto";
import {
  ErrorUnprocessableEventHandler,
  EventProcessor,
} from "../../src/processor.js";
import { createProcessorClient } from "../../src/pg/client.js";
import { migrate, type EventType, eventTypes } from "./server.js";
import dotenv from "dotenv";
import { sleep } from "../../src/sleep.js";
dotenv.config();

let processor: EventProcessor<EventType> | undefined = undefined;

(async () => {
  const client = new pg.Client({
    user: process.env.POSTGRES_USER,
    password: process.env.POSTGRES_PASSWORD,
    database: process.env.POSTGRES_DB,
    port: parseInt(process.env.POSTGRES_PORT || "5434"),
  });
  await client.connect();
  await migrate(client);

  processor = new EventProcessor<EventType>({
    client: createProcessorClient<EventType>(client),
    handlerMap: {
      ResourceSaved: {
        thing1: async (event) => {
          console.log(`${event.id} thing1 ${event.correlation_id}`);
          if (Math.random() > 0.9) throw new Error("some issue");

          return;
        },
        thing2: async (event) => {
          console.log(`${event.id} thing2 ${event.correlation_id}`);
          if (Math.random() > 0.9) throw new Error("some issue");
          if (Math.random() > 0.6)
            throw new ErrorUnprocessableEventHandler(new Error("parent error"));

          return;
        },
        thing3: async (event) => {
          await sleep(20_000);
          console.log(`${event.id} thing3 ${event.correlation_id}`);
          if (Math.random() > 0.75) throw new Error("some issue");

          return;
        },
      },
      EventMaxErrorsReached: {
        // Optional: add handlers for EventMaxErrorsReached events if needed
        // For example, you might want to send alerts or log to external systems
      },
    },
    pollingIntervalMs: 5000,
    logger: console,
    onEventMaxErrorsReached: async ({ event, txClient, signal }) => {
      // Transactionally persist an 'event max errors reached' event
      // This hook is called when:
      // - Maximum allowed errors are reached
      // - An unprocessable error is encountered
      // - Event handler map is missing for the event type

      // Use the abort signal for cleanup during graceful shutdown
      if (signal?.aborted) {
        return;
      }

      await txClient.createEvent({
        id: randomUUID(),
        timestamp: new Date(),
        type: eventTypes.EventMaxErrorsReached,
        data: {
          failedEventId: event.id,
          failedEventType: event.type,
          failedEventCorrelationId: event.correlation_id,
        },
        correlation_id: event.correlation_id,
        handler_results: {},
        errors: 0,
      });

      console.log("Event max errors reached event created", {
        failedEventId: event.id,
      });
    },
  });
  processor.start();
})();

const shutdown = (() => {
  let shutdownStarted = false;
  return () => {
    if (shutdownStarted) return;

    shutdownStarted = true;

    processor
      ?.stop()
      .then(() => {
        process.exit(0);
      })
      .catch((err) => {
        console.error(err);
        process.exit(1);
      });
  };
})();
process.once("SIGTERM", shutdown);
process.once("SIGINT", shutdown);
