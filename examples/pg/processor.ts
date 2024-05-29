import { Client } from "pg";
import {
  ErrorUnprocessableEventHandler,
  EventProcessor,
} from "../../src/processor";
import { createProcessorClient } from "../../src/pg/client";
import { migrate, type EventType } from "./server";
import dotenv from "dotenv";
dotenv.config();

let processor: ReturnType<typeof EventProcessor> | undefined = undefined;

(async () => {
  const client = new Client({
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
    },
    { sleepTimeMs: 5000, logger: console },
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
