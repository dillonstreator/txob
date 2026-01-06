import pg from "pg";
import { randomUUID } from "node:crypto";
import {
  ErrorUnprocessableEventHandler,
  EventProcessor,
  WakeupEmitter,
} from "../../src/index.js";
import {
  createProcessorClient,
  createWakeupEmitter,
} from "../../src/pg/client.js";
import { migrate, type EventType, eventTypes } from "./server.js";
import dotenv from "dotenv";
import { sleep } from "../../src/sleep.js";
dotenv.config();

let processor: EventProcessor<EventType> | undefined = undefined;
let wakeupEmitter: WakeupEmitter | undefined = undefined;

(async () => {
  const clientConfig: pg.ClientConfig = {
    user: process.env.POSTGRES_USER,
    password: process.env.POSTGRES_PASSWORD,
    database: process.env.POSTGRES_DB,
    port: parseInt(process.env.POSTGRES_PORT || "5434"),
  };
  const client = new pg.Client(clientConfig);
  await client.connect();
  await migrate(client);

  wakeupEmitter = await createWakeupEmitter({
    listenClientConfig: clientConfig,
    createTrigger: true,
    querier: client,
  });

  processor = new EventProcessor<EventType>({
    maxEventConcurrency: 50,
    client: createProcessorClient<EventType>({ querier: client }),
    wakeupEmitter,
    handlerMap: {
      ResourceSaved: {
        thing1: async (event) => {
          console.log(`${event.id} thing1 ${event.correlation_id}`);
          if (Math.random() > 0.99) throw new Error("some issue");

          return;
        },
        thing2: async (event) => {
          console.log(`${event.id} thing2 ${event.correlation_id}`);
          if (Math.random() > 0.96) throw new Error("some issue");

          return;
        },
        thing3: async (event) => {
          await sleep(Math.random() * 1_000);
          console.log(`${event.id} thing3 ${event.correlation_id}`);
          if (Math.random() > 0.8) throw new Error("some issue");

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
    onEventMaxErrorsReached: async ({ event, txClient }) => {
      // Transactionally persist an 'event max errors reached' event
      // This hook is called when:
      // - Maximum allowed errors are reached
      // - An unprocessable error is encountered
      // - Event handler map is missing for the event type

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
  return async () => {
    if (shutdownStarted) return;

    shutdownStarted = true;

    try {
      await wakeupEmitter?.close();
    } catch (err) {
      console.error(err);
      process.exit(1);
    }
    process.exit(0);
  };
})();
process.once("SIGTERM", shutdown);
process.once("SIGINT", shutdown);
