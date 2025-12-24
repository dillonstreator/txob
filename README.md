<h1 align="center">txob</h1>
<p align="center">A generic <a href="https://microservices.io/patterns/data/transactional-outbox.html">transactional outbox</a> event processor with graceful shutdown and horizontal scalability</p>
<p align="center">
  <a href="https://codecov.io/gh/dillonstreator/txob" >
    <img src="https://codecov.io/gh/dillonstreator/txob/graph/badge.svg?token=E9M7G67VLL"/>
  </a>
  <a aria-label="NPM version" href="https://www.npmjs.com/package/txob">
    <img alt="" src="https://badgen.net/npm/v/txob?v=0.1.5">
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

**Important**: `txob` implements an **at-least-once delivery** mechanism. If event update operations fail after handlers have successfully executed, those handlers may be re-invoked. Handlers should be designed to be idempotent to handle potential duplicate executions gracefully.

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

### Event Processing Max Errors Reached Hook

When an event fails to process (maximum errors reached, unprocessable error, or missing handler), you can optionally provide an `onEventMaxErrorsReached` hook that will be called transactionally. This allows you to persist a failure event within the same transaction, ensuring data consistency. The hook receives:

- `event`: The event that failed processing
- `txClient`: The transactional client for creating events within the same transaction
- `signal`: Optional AbortSignal for graceful shutdown handling

As per the 'transactional outbox specification', you should ensure your events are transactionally persisted alongside their related data mutations.

The processor handles graceful shutdown and is horizontally scalable by default with the native client implementatations for [`pg`](./src/pg/client.ts) and [`mongodb`](./src/mongodb/client.ts).

### Database Index Recommendations (PostgreSQL)

For optimal performance when using PostgreSQL, create the following indexes on your events table:

```sql
-- Primary index for getEventsToProcess query (most critical)
-- This partial index only includes unprocessed events, keeping it small and fast
CREATE INDEX idx_events_processing ON events(processed_at, backoff_until, errors)
WHERE processed_at IS NULL;

-- Index for lookups by id (if id is not already the primary key)
CREATE UNIQUE INDEX idx_events_id ON events(id);

-- Optional: Index for correlation_id if you frequently query by correlation
CREATE INDEX idx_events_correlation_id ON events(correlation_id);
```

**Why these indexes?**

1. **`idx_events_processing`**: This is the most critical index. It's a partial index that only includes unprocessed events (`WHERE processed_at IS NULL`), which keeps the index small and efficient. It covers the main query pattern in `getEventsToProcess` which filters on:
   - `processed_at IS NULL`
   - `backoff_until IS NULL OR backoff_until < NOW()`
   - `errors < maxErrors`

2. **`idx_events_id`**: Ensures fast lookups when locking events by ID in `getEventByIdForUpdateSkipLocked`. If `id` is your primary key, this index already exists.

3. **`idx_events_correlation_id`**: Optional but recommended if you need to query events by correlation ID for debugging or tracing purposes.

**Note**: The partial index (`idx_events_processing`) is particularly important as it only indexes unprocessed events, making it much smaller and faster than a full table index. As events are processed and `processed_at` is set, they automatically drop out of the index.

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
  EventMaxErrorsReached: "EventMaxErrorsReached",
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
        // To mark a handler as unprocessable (will trigger onEventMaxErrorsReached hook):
        // throw new ErrorUnprocessableEventHandler(new Error("reason"));
      },
      publish: async (event) => {
        // publish to event bus
      },
      // other handler that should be executed when a `UserCreated` event is saved
    },
    EventMaxErrorsReached: {
      // Optional: add handlers for EventMaxErrorsReached events if needed
      // For example, you might want to send alerts or log to external systems
    },
    // other event types
  },
  {
    onEventMaxErrorsReached: async ({ event, txClient, signal }) => {
      // Transactionally persist an 'event processing failed' event
      // This hook is called when the maximum allowed errors are reached

      await txClient.createEvent({
        id: randomUUID(),
        timestamp: new Date(),
        type: eventTypes.EventProcessingFailed,
        data: {
          eventId: event.id,
          eventType: event.type,
        },
        correlation_id: event.correlation_id,
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
