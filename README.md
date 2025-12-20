<h1 align="center">txob</h1>
<p align="center">A generic <a href="https://microservices.io/patterns/data/transactional-outbox.html">transactional outbox</a> event processor with graceful shutdown and horizontal scalability</p>
<p align="center">
  <a href="https://codecov.io/gh/dillonstreator/txob" >
    <img src="https://codecov.io/gh/dillonstreator/txob/graph/badge.svg?token=E9M7G67VLL"/>
  </a>
  <a aria-label="NPM version" href="https://www.npmjs.com/package/txob">
    <img alt="" src="https://badgen.net/npm/v/txob?v=0.0.22">
  </a>
  <a aria-label="License" href="https://github.com/dillonstreator/txob/blob/main/LICENSE">
    <img alt="" src="https://badgen.net/npm/license/txob">
  </a>
  <a aria-label="Typescript" href="https://github.com/dillonstreator/txob/blob/main/src/cache.ts">
    <img alt="" src="https://badgen.net/npm/types/txob">
  </a>
  <a aria-label="CodeFactor" href="https://www.codefactor.io/repository/github/dillonstreator/txob">
    <img alt="" src="https://www.codefactor.io/repository/github/dillonstreator/txob/badge">
  </a>
</p>

## Description

`txob` _does not_ prescribe a storage layer implementation.\
`txob` _does_ prescribe a base event storage data model that enables a high level of visibility into event handler processing outcomes.

- `id` string
- `timestamp` Date
- `type` (enum/string)
- `data` json
- `correlation_id` string
- `handler_results` json
- `errors` number
- `backoff_until` Date nullable
- `processed_at` Date nullable

`txob` exposes an optionally configurable interface into event processing with control over maximum allowed errors, backoff calculation on error, event update retrying, logging, and transactional event creation when processing fails.

### Event Processing Failed Hook

When an event fails to process (maximum errors reached, unprocessable error, or missing handler), you can optionally provide an `onEventProcessingFailed` hook that will be called transactionally. This allows you to persist a failure event within the same transaction, ensuring data consistency. The hook receives:

- `failedEvent`: The event that failed processing
- `reason`: An object indicating why processing failed:
  - `{ type: "max_errors_reached" }` - Event exceeded maximum retry attempts
  - `{ type: "unprocessable_error", handlerName: string, error: Error }` - Handler threw `ErrorUnprocessableEventHandler`
  - `{ type: "missing_handler_map" }` - No handler map exists for the event type
- `txClient`: The transactional client for creating events within the same transaction
- `signal`: Optional AbortSignal for graceful shutdown handling

As per the 'transactional outbox specification', you should ensure your events are transactionally persisted alongside their related data mutations.

The processor handles graceful shutdown and is horizontally scalable by default with the native client implementatations for [`pg`](./src/pg/client.ts) and [`mongodb`](./src/mongodb/client.ts).

## Installation

```sh
(npm|yarn) (install|add) txob
```

### Examples

Let's look at an example of an HTTP API that allows a user to be invited where an SMTP request must be sent as a side-effect of the user creation / invite.

```ts
import http from "node:http";
import { randomUUID } from "node:crypto";
import pg from "pg";
import gracefulShutdown from "http-graceful-shutdown";
import { EventProcessor, ErrorUnprocessableEventHandler } from "txob";
import { createProcessorClient } from "txob/pg";

const eventTypes = {
  UserCreated: "UserCreated",
  EventProcessingFailed: "EventProcessingFailed",
  // other event types
} as const;

type EventType = keyof typeof eventTypes;

const client = new pg.Client({
  user: process.env.POSTGRES_USER,
  password: process.env.POSTGRES_PASSWORD,
  database: process.env.POSTGRES_DB,
});
await client.connect();

const HTTP_PORT = process.env.PORT || 3000;

const processor = EventProcessor(
  createProcessorClient<EventType>(client),
  {
    UserCreated: {
      sendEmail: async (event, { signal }) => {
        // find user by event.data.userId to use relevant user data in email sending
        // email sending logic
        // use the AbortSignal `signal` (aborted when EventProcessor#stop is called) to perform quick cleanup
        // during graceful shutdown enabling the processor to
        // save handler result updates to the event ASAP
        // To mark a handler as unprocessable (will trigger onEventProcessingFailed hook):
        // throw new ErrorUnprocessableEventHandler(new Error("reason"));
      },
      publish: async (event) => {
        // publish to event bus
      },
      // other handler that should be executed when a `UserCreated` event is saved
    },
    EventProcessingFailed: {
      // Optional: add handlers for EventProcessingFailed events if needed
      // For example, you might want to send alerts or log to external systems
    },
    // other event types
  },
  {
    onEventProcessingFailed: async ({
      failedEvent,
      reason,
      txClient,
      signal,
    }) => {
      // Transactionally persist an 'event processing failed' event
      // This hook is called when:
      // - Maximum allowed errors are reached
      // - An unprocessable error is encountered (ErrorUnprocessableEventHandler)
      // - Event handler map is missing for the event type

      const reasonData: Record<string, unknown> = {
        failedEventId: failedEvent.id,
        failedEventType: failedEvent.type,
        reason: reason.type,
      };

      // Add additional context based on the failure reason
      if (reason.type === "unprocessable_error") {
        reasonData.handlerName = reason.handlerName;
        reasonData.error = reason.error.message;
      }

      await txClient.createEvent({
        id: randomUUID(),
        timestamp: new Date(),
        type: eventTypes.EventProcessingFailed,
        data: reasonData,
        correlation_id: failedEvent.correlation_id,
        handler_results: {},
        errors: 0,
      });
    },
  },
);
processor.start();

const server = http
  .createServer(async (req, res) => {
    if (req.url !== "/invite") return;

    // invite user endpoint

    const correlationId = randomUUID(); // or some value on the incoming request such as a request id / trace id

    try {
      await client.query("BEGIN");

      const userId = randomUUID();
      // save user with userId
      await client.query(`INSERT INTO users (id, email) VALUES ($1, $2)`, [
        userId,
        req.body.email,
      ]);

      // save event to `events` table
      await client.query(
        `INSERT INTO events (id, type, data, correlation_id) VALUES ($1, $2, $3, $4)`,
        [
          randomUUID(),
          eventTypes.UserCreated,
          { userId }, // other relevant data
          correlationId,
        ],
      );

      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
    }
  })
  .listen(HTTP_PORT, () => console.log(`listening on port ${HTTP_PORT}`));

gracefulShutdown(server, {
  onShutdown: async () => {
    // allow any actively running event handlers to finish
    // and the event processor to save the results
    await processor.stop();
  },
});
```

[other examples](./examples)
