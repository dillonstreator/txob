import pg from "pg";
import { randomUUID } from "node:crypto";
import {
  ErrorUnprocessableEventHandler,
  EventProcessor,
} from "../../src/processor.js";
import { createProcessorClient } from "../../src/pg/client.js";
import { migrate, type EventType, eventTypes } from "./server.js";
import dotenv from "dotenv";
dotenv.config();

let processor: ReturnType<typeof EventProcessor> | undefined = undefined;

(async () => {
  const client = new pg.Client({
    user: process.env.POSTGRES_USER,
    password: process.env.POSTGRES_PASSWORD,
    database: process.env.POSTGRES_DB,
    port: parseInt(process.env.POSTGRES_PORT || "5434"),
  });
  await client.connect();
  await migrate(client);

  processor = EventProcessor(
    createProcessorClient<EventType>(client),
    {
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
          console.log(`${event.id} thing3 ${event.correlation_id}`);
          if (Math.random() > 0.75) throw new Error("some issue");

          return;
        },
      },
      EventProcessingFailed: {
        // Optional: add handlers for EventProcessingFailed events if needed
        // For example, you might want to send alerts or log to external systems
      },
    },
    {
      sleepTimeMs: 5000,
      logger: console,
      onEventProcessingFailed: async ({
        event,
        reason,
        txClient,
        signal,
      }) => {
        // Transactionally persist an 'event processing failed' event
        // This hook is called when:
        // - Maximum allowed errors are reached
        // - An unprocessable error is encountered
        // - Event handler map is missing for the event type

        // Use the abort signal for cleanup during graceful shutdown
        if (signal?.aborted) {
          return;
        }

        const reasonData: Record<string, unknown> = {};
        if (reason.type === "max_errors_reached") {
          reasonData.reason = "max_errors_reached";
        } else if (reason.type === "unprocessable_error") {
          reasonData.reason = "unprocessable_error";
          reasonData.handlerName = reason.handlerName;
          reasonData.error = reason.error.message;
        } else if (reason.type === "missing_handler_map") {
          reasonData.reason = "missing_handler_map";
        }

        await txClient.createEvent({
          id: randomUUID(),
          timestamp: new Date(),
          type: eventTypes.EventProcessingFailed,
          data: {
            failedEventId: event.id,
            failedEventType: event.type,
            failedEventCorrelationId: event.correlation_id,
            ...reasonData,
          },
          correlation_id: event.correlation_id,
          handler_results: {},
          errors: 0,
        });

        console.log("Event processing failed event created", {
          failedEventId: event.id,
          reason: reason.type,
        });
      },
    },
  );
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
