<h1 align="center">txob</h1>
<p align="center">Reliably process side-effects in your Node.js applications without data loss</p>
<p align="center">
  <a href="https://codecov.io/gh/dillonstreator/txob" >
    <img src="https://codecov.io/gh/dillonstreator/txob/graph/badge.svg?token=E9M7G67VLL"/>
  </a>
  <a aria-label="NPM version" href="https://www.npmjs.com/package/txob">
    <img alt="" src="https://badgen.net/npm/v/txob?v=0.1.7">
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

---

## The Problem

When building applications, you often need to perform multiple operations together: update your database AND send an email, publish an event, trigger a webhook, or notify another service. This creates a critical challenge:

```typescript
// ❌ The problem: What if the email fails after the database commit?
await db.createUser(user);
await db.commit();
await emailService.sendWelcomeEmail(user.email); // 💥 Fails! User created but no email sent
```

```typescript
// ❌ Also problematic: What if the database fails after sending the email?
await emailService.sendWelcomeEmail(user.email); // ✅ Email sent
await db.createUser(user);
await db.commit(); // 💥 Fails! Email sent but no user record
```

```typescript
// ❌ What about using a message queue?
await db.createUser(user);
await messageQueue.publish("user.created", user); // ✅ Message queued
await db.commit(); // 💥 Fails! Message is in queue but no user record
// The queue and database are separate systems - you can't make them atomic!
```

**The Transactional Outbox Pattern** solves this by storing both the business data and events in a single database transaction, then processing events asynchronously with guaranteed delivery.

```typescript
// ✅ Solution: Save both user and event in the same transaction
await db.query("BEGIN");

// Save your business data
await db.query("INSERT INTO users (id, email, name) VALUES ($1, $2, $3)", [
  userId,
  email,
  name,
]);

// Save the event in the SAME transaction
await db.query(
  "INSERT INTO events (id, type, data, correlation_id, handler_results, errors) VALUES ($1, $2, $3, $4, $5, $6)",
  [randomUUID(), "UserCreated", { userId, email }, correlationId, {}, 0],
);

await db.query("COMMIT");
// ✅ Both user and event are saved atomically!
// If commit fails, neither is saved. If it succeeds, both are saved.
// The processor will pick up the event and send the email (and any other side effects that you register) asynchronously
```

## Features

- ✅ **At-least-once delivery** - Events are never lost, even during failures or crashes
- ✅ **Graceful shutdown** - Finish processing in-flight events before shutting down
- ✅ **Horizontal scalability** - Run multiple processors without conflicts using row-level locking
- ✅ **Database agnostic** - Built-in support for PostgreSQL and MongoDB, or implement your own
- ✅ **Configurable error handling** - Exponential backoff, max retries, and custom error hooks
- ✅ **TypeScript-first** - Full type safety and autocompletion
- ✅ **Handler result tracking** - Track the execution status of each handler independently
- ✅ **Minimal dependencies** - Only `p-limit` (plus your database driver)

## Quick Start

### Installation

```sh
# For PostgreSQL
npm install txob pg

# For MongoDB
npm install txob mongodb
```

### Basic Example (PostgreSQL)

**1. Create the events table:**

```sql
CREATE TABLE events (
  id UUID PRIMARY KEY,
  timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  type VARCHAR(255) NOT NULL,
  data JSONB,
  correlation_id UUID,
  handler_results JSONB DEFAULT '{}',
  errors INTEGER DEFAULT 0,
  backoff_until TIMESTAMPTZ,
  processed_at TIMESTAMPTZ
);

-- Critical index for performance
CREATE INDEX idx_events_processing ON events(processed_at, backoff_until, errors)
WHERE processed_at IS NULL;
```

**2. Set up the event processor:**

```typescript
import { EventProcessor } from "txob";
import { createProcessorClient } from "txob/pg";
import pg from "pg";

const client = new pg.Client({
  /* your config */
});
await client.connect();

const processor = EventProcessor(createProcessorClient(client), {
  UserCreated: {
    // Handlers are processed concurrently and independently with retries
    // If one handler fails, others continue processing
    sendWelcomeEmail: async (event, { signal }) => {
      await emailService.send({
        to: event.data.email,
        subject: "Welcome!",
        template: "welcome",
      });
    },
    createStripeCustomer: async (event, { signal }) => {
      await stripe.customers.create({
        email: event.data.email,
        metadata: { userId: event.data.userId },
      });
    },
  },
});

processor.start();

// Graceful shutdown
process.on("SIGTERM", () => processor.stop());
```

**3. Save events transactionally with your business logic:**

```typescript
import { randomUUID } from "crypto";

// Inside your application code
await client.query("BEGIN");

// Save your business data
await client.query("INSERT INTO users (id, email, name) VALUES ($1, $2, $3)", [
  userId,
  email,
  name,
]);

// Save the event in the SAME transaction
await client.query(
  "INSERT INTO events (id, type, data, correlation_id, handler_results, errors) VALUES ($1, $2, $3, $4, $5, $6)",
  [randomUUID(), "UserCreated", { userId, email }, correlationId, {}, 0],
);

await client.query("COMMIT");
// ✅ Both user and event are saved atomically!
// The processor will pick up the event and send the email
```

That's it! The processor will automatically poll for new events and execute your handlers.

## How It Works

```
┌─────────────────────────────────────────────────────────────────┐
│                     Your Application                             │
├─────────────────────────────────────────────────────────────────┤
│  BEGIN TRANSACTION                                               │
│    1. Insert/Update business data (users, orders, etc.)         │
│    2. Insert event record                                        │
│  COMMIT TRANSACTION                                              │
└─────────────────────────────────────────────────────────────────┘
                           │
                           │ Both saved atomically ✅
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Events Table                                  │
│  [id] [type] [data] [processed_at] [errors] [backoff_until]    │
└─────────────────────────────────────────────────────────────────┘
                           │
                           │ Polls every few seconds
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Event Processor (txob)                         │
├─────────────────────────────────────────────────────────────────┤
│  1. SELECT unprocessed events (FOR UPDATE SKIP LOCKED)          │
│  2. Execute handlers (send email, webhook, etc.)                │
│  3. UPDATE event with results and processed_at                  │
│  4. On failure: increment errors, set backoff_until             │
└─────────────────────────────────────────────────────────────────┘
```

**Key Points:**

- Events are saved in the **same transaction** as your business data
- If the transaction fails, neither the data nor event is saved
- The processor runs independently and guarantees **at-least-once delivery**
- Multiple processors can run concurrently using database row locking
- Failed events are retried with exponential backoff

## Core Concepts

### Event Structure

Every event in txob follows this structure:

```typescript
interface TxOBEvent<EventType extends string> {
  id: string; // Unique event identifier (UUID recommended)
  timestamp: Date; // When the event was created
  type: EventType; // Event type (e.g., "UserCreated", "OrderPlaced")
  data: Record<string, unknown>; // Event payload - your custom data
  correlation_id: string; // For tracing requests across services
  handler_results: Record<string, TxOBEventHandlerResult>; // Results from each handler
  errors: number; // Number of processing attempts
  backoff_until?: Date; // When to retry (null if not backing off)
  processed_at?: Date; // When fully processed (null if pending)
}
```

**Field Explanations:**

- **`handler_results`**: Tracks each handler's status independently. If one handler fails, others can still succeed
- **`errors`**: Global error count. When it reaches `maxErrors`, the event is marked as processed (failed)
- **`backoff_until`**: Prevents immediate retries. Set to future timestamp after failures
- **`correlation_id`**: Essential for distributed tracing and debugging

### Event Handlers

Handlers are async functions that execute your side-effects:

```typescript
type TxOBEventHandler = (
  event: TxOBEvent,
  opts: { signal?: AbortSignal },
) => Promise<void>;
```

**Important:** Handlers should be **idempotent** because they may be called multiple times for the same event (at-least-once delivery).

```typescript
// ✅ Good: Idempotent handler
const sendEmail: TxOBEventHandler = async (event) => {
  const alreadySent = await checkIfEmailSent(event.data.userId);
  if (alreadySent) return; // Safe to retry

  await emailService.send(event.data.email);
};

// ❌ Bad: Not idempotent
const incrementCounter: TxOBEventHandler = async (event) => {
  await db.query("UPDATE counters SET count = count + 1"); // Will increment multiple times!
};
```

### Handler Results

Each handler's execution is tracked independently:

```typescript
type TxOBEventHandlerResult = {
  processed_at?: Date; // When this handler succeeded
  unprocessable_at?: Date; // When this handler was marked unprocessable
  errors?: Array<{
    // Error history for this handler
    error: unknown;
    timestamp: Date;
  }>;
};
```

This means if you have 3 handlers and 1 fails, the other 2 won't be re-executed on retry.

### Delivery Guarantees

**txob implements at-least-once delivery:**

- ✅ Events are **never lost** even if the processor crashes
- ⚠️ Handlers **may be called multiple times** for the same event
- ⚠️ If `updateEvent` fails after handlers succeed, they will be re-invoked

**Why this matters:**

```typescript
// This handler will be called again if the event update fails
UserCreated: {
  sendEmail: async (event) => {
    await emailService.send(event.data.email); // ✅ Sent successfully
    // 💥 But if updateEvent() fails here, this will run again!
  };
}
```

**Solution:** Make your handlers idempotent (check if work was already done before doing it again).

### Error Handling

txob provides sophisticated error handling:

**1. Automatic Retries with Backoff**

```typescript
EventProcessor(client, handlers, {
  maxErrors: 5, // Retry up to 5 times
  backoff: (errorCount) => {
    // Custom backoff strategy
    const delayMs = 1000 * 2 ** errorCount; // Exponential: 1s, 2s, 4s, 8s, 16s
    return new Date(Date.now() + delayMs);
  },
});
```

**2. Unprocessable Events**

Sometimes an event cannot be processed (e.g., invalid data). Mark it as unprocessable to stop retrying:

```typescript
import { ErrorUnprocessableEventHandler } from "txob";

UserCreated: {
  sendEmail: async (event) => {
    if (!isValidEmail(event.data.email)) {
      throw new ErrorUnprocessableEventHandler(
        new Error("Invalid email address"),
      );
    }
    await emailService.send(event.data.email);
  };
}
```

**3. Max Errors Hook**

When an event reaches max errors, you can create a "dead letter" event:

```typescript
EventProcessor(client, handlers, {
  onEventMaxErrorsReached: async ({ event, txClient, signal }) => {
    // Save a failure event in the same transaction
    await txClient.createEvent({
      id: randomUUID(),
      type: "EventFailed",
      data: {
        originalEventId: event.id,
        originalEventType: event.type,
        reason: "Max errors reached",
      },
      correlation_id: event.correlation_id,
      handler_results: {},
      errors: 0,
    });

    // Send alert, log to monitoring system, etc.
  },
});
```

## Database Setup

### PostgreSQL

**1. Create the events table:**

```sql
CREATE TABLE events (
  id UUID PRIMARY KEY,
  timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  type VARCHAR(255) NOT NULL,
  data JSONB,
  correlation_id UUID,
  handler_results JSONB DEFAULT '{}',
  errors INTEGER DEFAULT 0,
  backoff_until TIMESTAMPTZ,
  processed_at TIMESTAMPTZ
);
```

**2. Create indexes for optimal performance:**

```sql
-- Critical: Partial index for unprocessed events (keeps index small and fast)
CREATE INDEX idx_events_processing ON events(processed_at, backoff_until, errors)
WHERE processed_at IS NULL;

-- Unique index on id (if not using id as PRIMARY KEY)
CREATE UNIQUE INDEX idx_events_id ON events(id);

-- Optional: For querying by correlation_id
CREATE INDEX idx_events_correlation_id ON events(correlation_id);
```

**Why these indexes?**

The `idx_events_processing` partial index is critical for performance. It:

- Only indexes unprocessed events (`WHERE processed_at IS NULL`)
- Stays small as events are processed
- Covers the main query pattern: `processed_at IS NULL AND (backoff_until IS NULL OR backoff_until < NOW()) AND errors < maxErrors`

**3. Use the PostgreSQL client:**

```typescript
import { createProcessorClient } from "txob/pg";
import pg from "pg";

const client = new pg.Client({
  user: process.env.POSTGRES_USER,
  password: process.env.POSTGRES_PASSWORD,
  database: process.env.POSTGRES_DB,
});
await client.connect();

const processorClient = createProcessorClient(
  client,
  "events", // Optional: table name (default: "events")
  100, // Optional: max events per poll (default: 100)
);
```

### MongoDB

**1. Create the events collection with indexes:**

```typescript
import { MongoClient } from "mongodb";

const mongoClient = new MongoClient(process.env.MONGO_URL);
await mongoClient.connect();
const db = mongoClient.db("myapp");

// Create collection
const eventsCollection = db.collection("events");

// Create indexes
await eventsCollection.createIndex(
  { processed_at: 1, backoff_until: 1, errors: 1 },
  { partialFilterExpression: { processed_at: null } },
);
await eventsCollection.createIndex({ id: 1 }, { unique: true });
await eventsCollection.createIndex({ correlation_id: 1 });
```

**2. Use the MongoDB client:**

```typescript
import { createProcessorClient } from "txob/mongodb";

const processorClient = createProcessorClient(
  db,
  "events", // Optional: collection name (default: "events")
  100, // Optional: max events per poll (default: 100)
);
```

**Note:** MongoDB transactions require a replica set or sharded cluster. [See MongoDB docs](https://www.mongodb.com/docs/manual/core/transactions/).

### Custom Database

Implement the `TxOBProcessorClient` interface:

```typescript
interface TxOBProcessorClient<EventType extends string> {
  getEventsToProcess(opts: {
    signal?: AbortSignal;
    maxErrors: number;
  }): Promise<Pick<TxOBEvent<EventType>, "id" | "errors">[]>;

  transaction(
    fn: (txClient: TxOBTransactionProcessorClient<EventType>) => Promise<void>,
  ): Promise<void>;
}

interface TxOBTransactionProcessorClient<EventType extends string> {
  getEventByIdForUpdateSkipLocked(
    eventId: string,
    opts: { signal?: AbortSignal; maxErrors: number },
  ): Promise<TxOBEvent<EventType> | null>;

  updateEvent(event: TxOBEvent<EventType>): Promise<void>;

  createEvent(
    event: Omit<TxOBEvent<EventType>, "processed_at" | "backoff_until">,
  ): Promise<void>;
}
```

See [src/pg/client.ts](./src/pg/client.ts) or [src/mongodb/client.ts](./src/mongodb/client.ts) for reference implementations.

## Configuration

### Processor Options

```typescript
EventProcessor(client, handlerMap, {
  // Polling interval in milliseconds (default: 5000)
  sleepTimeMs: 5000,

  // Maximum errors before marking event as processed/failed (default: 5)
  maxErrors: 5,

  // Backoff calculation function (default: exponential backoff)
  backoff: (errorCount: number): Date => {
    const baseDelayMs = 1000;
    const maxDelayMs = 60000;
    const backoffMs = Math.min(baseDelayMs * 2 ** errorCount, maxDelayMs);
    return new Date(Date.now() + backoffMs);
  },

  // Maximum concurrent events being processed (default: 5)
  maxEventConcurrency: 5,

  // Maximum concurrent handlers per event (default: 10)
  maxHandlerConcurrency: 10,

  // Custom logger (default: undefined)
  logger: {
    debug: (msg, ...args) => console.debug(msg, ...args),
    info: (msg, ...args) => console.info(msg, ...args),
    warn: (msg, ...args) => console.warn(msg, ...args),
    error: (msg, ...args) => console.error(msg, ...args),
  },

  // Hook called when max errors reached (default: undefined)
  onEventMaxErrorsReached: async ({ event, txClient, signal }) => {
    // Create a dead-letter event, send alerts, etc.
  },
});
```

### Configuration Reference

| Option                    | Type                      | Default     | Description                                 |
| ------------------------- | ------------------------- | ----------- | ------------------------------------------- |
| `sleepTimeMs`             | `number`                  | `5000`      | Milliseconds between polling cycles         |
| `maxErrors`               | `number`                  | `5`         | Max retry attempts before marking as failed |
| `backoff`                 | `(count: number) => Date` | Exponential | Calculate next retry time                   |
| `maxEventConcurrency`     | `number`                  | `5`         | Max events processed simultaneously         |
| `maxHandlerConcurrency`   | `number`                  | `10`        | Max handlers per event running concurrently |
| `logger`                  | `Logger`                  | `undefined` | Custom logger interface                     |
| `onEventMaxErrorsReached` | `function`                | `undefined` | Hook for max errors                         |

## Usage Examples

### Complete HTTP API Example

This example shows a complete HTTP API that creates users and sends welcome emails transactionally:

```typescript
import http from "node:http";
import { randomUUID } from "node:crypto";
import pg from "pg";
import gracefulShutdown from "http-graceful-shutdown";
import { EventProcessor, ErrorUnprocessableEventHandler } from "txob";
import { createProcessorClient } from "txob/pg";

// 1. Define your event types
const eventTypes = {
  UserCreated: "UserCreated",
  EventMaxErrorsReached: "EventMaxErrorsReached",
} as const;

type EventType = keyof typeof eventTypes;

// 2. Set up database connection
const client = new pg.Client({
  user: process.env.POSTGRES_USER,
  password: process.env.POSTGRES_PASSWORD,
  database: process.env.POSTGRES_DB,
});
await client.connect();

// 3. Create and start the processor
const processor = EventProcessor(
  createProcessorClient<EventType>(client),
  {
    UserCreated: {
      sendEmail: async (event, { signal }) => {
        // Check if email was already sent (idempotency)
        const sent = await checkEmailSent(event.data.userId);
        if (sent) return;

        // Send email
        await emailService.send({
          to: event.data.email,
          subject: "Welcome!",
          template: "welcome",
        });

        // Use signal for cleanup on shutdown
        signal?.addEventListener("abort", () => {
          emailService.cancelPending();
        });
      },

      publishToEventBus: async (event) => {
        await eventBus.publish("user.created", event.data);
      },
    },

    EventMaxErrorsReached: {
      alertOps: async (event) => {
        await slack.send({
          channel: "#alerts",
          text: `Event failed: ${event.data.eventType} (${event.data.eventId})`,
        });
      },
    },
  },
  {
    sleepTimeMs: 5000,
    maxErrors: 5,
    logger: console,
    onEventMaxErrorsReached: async ({ event, txClient }) => {
      await txClient.createEvent({
        id: randomUUID(),
        timestamp: new Date(),
        type: eventTypes.EventMaxErrorsReached,
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

// 4. Create HTTP server
const server = http.createServer(async (req, res) => {
  if (req.url !== "/users" || req.method !== "POST") {
    res.statusCode = 404;
    return res.end();
  }

  const correlationId = req.headers["x-correlation-id"] || randomUUID();

  try {
    const body = await getBody(req);
    const { email, name } = JSON.parse(body);

    // Start transaction
    await client.query("BEGIN");

    // Save user
    const userId = randomUUID();
    await client.query(
      "INSERT INTO users (id, email, name) VALUES ($1, $2, $3)",
      [userId, email, name],
    );

    // Save event IN THE SAME TRANSACTION
    await client.query(
      "INSERT INTO events (id, type, data, correlation_id, handler_results, errors) VALUES ($1, $2, $3, $4, $5, $6)",
      [
        randomUUID(),
        eventTypes.UserCreated,
        { userId, email, name },
        correlationId,
        {},
        0,
      ],
    );

    // Commit transaction
    await client.query("COMMIT");

    res.statusCode = 201;
    res.end(JSON.stringify({ userId }));
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    res.statusCode = 500;
    res.end(JSON.stringify({ error: "Internal server error" }));
  }
});

const HTTP_PORT = process.env.PORT || 3000;
server.listen(HTTP_PORT, () => console.log(`Server listening on ${HTTP_PORT}`));

// 5. Graceful shutdown
gracefulShutdown(server, {
  onShutdown: async () => {
    await processor.stop(); // Wait for in-flight events to complete
    await client.end();
  },
});
```

### Multiple Event Types

```typescript
const processor = EventProcessor(createProcessorClient(client), {
  UserCreated: {
    sendWelcomeEmail: async (event) => {
      /* ... */
    },
    createStripeCustomer: async (event) => {
      /* ... */
    },
  },

  OrderPlaced: {
    sendConfirmationEmail: async (event) => {
      /* ... */
    },
    updateInventory: async (event) => {
      /* ... */
    },
    notifyWarehouse: async (event) => {
      /* ... */
    },
  },

  PaymentFailed: {
    sendRetryEmail: async (event) => {
      /* ... */
    },
    logToAnalytics: async (event) => {
      /* ... */
    },
  },
});
```

### Custom Backoff Strategy

```typescript
// Linear backoff: 5s, 10s, 15s, 20s, 25s
const linearBackoff = (errorCount: number): Date => {
  const delayMs = 5000 * errorCount;
  return new Date(Date.now() + delayMs);
};

// Fixed delay: always 30s
const fixedBackoff = (): Date => {
  return new Date(Date.now() + 30000);
};

// Fibonacci backoff: 1s, 1s, 2s, 3s, 5s, 8s, 13s...
const fibonacciBackoff = (() => {
  const fib = (n: number): number => (n <= 1 ? 1 : fib(n - 1) + fib(n - 2));
  return (errorCount: number): Date => {
    const delayMs = fib(errorCount) * 1000;
    return new Date(Date.now() + delayMs);
  };
})();

EventProcessor(client, handlers, {
  backoff: linearBackoff, // or fixedBackoff, or fibonacciBackoff
});
```

### Using AbortSignal for Cleanup

```typescript
UserCreated: {
  sendEmail: async (event, { signal }) => {
    // Long-running operation
    const emailJob = emailService.sendLarge(event.data);

    // Listen for shutdown signal
    signal?.addEventListener("abort", () => {
      console.log("Shutdown requested, canceling email...");
      emailJob.cancel(); // Clean up quickly so event can be saved
    });

    await emailJob;
  };
}
```

### Using txob with Message Queues

Use txob to guarantee consistency between your database and queue, then let the queue handle low-latency distribution:

```typescript
import { EventProcessor } from "txob";
import { createProcessorClient } from "txob/pg";
import { Kafka } from "kafkajs";

const kafka = new Kafka({ brokers: ["localhost:9092"] });
const producer = kafka.producer();
await producer.connect();

const processor = EventProcessor(createProcessorClient(client), {
  UserCreated: {
    // Publish to Kafka with guaranteed consistency
    publishToKafka: async (event) => {
      // Kafka's idempotent producer handles deduplication
      // Using event.id as the key ensures retries are safe
      await producer.send({
        topic: "user-events",
        messages: [
          {
            key: event.id, // Use event.id for idempotency
            value: JSON.stringify({
              type: event.type,
              data: event.data,
              timestamp: event.timestamp,
            }),
          },
        ],
      });
    },

    // Also handle other side effects
    sendEmail: async (event) => {
      await emailService.send(event.data.email);
    },
  },
});

processor.start();
```

**Benefits of this approach:**

- ✅ Database and Kafka are **guaranteed consistent** (via txob's transactional guarantees)
- ✅ If Kafka publish fails, txob will retry automatically
- ✅ Downstream consumers get low-latency events from Kafka
- ✅ You can still handle other side effects (email, webhooks) in parallel
- ✅ Best of both worlds: consistency from txob + speed from Kafka

### Separate Processor Service

You can run the processor as a separate service from your API:

```typescript
// processor-service.ts
import { EventProcessor } from "txob";
import { createProcessorClient } from "txob/pg";
import pg from "pg";

const client = new pg.Client({
  /* config */
});
await client.connect();

const processor = EventProcessor(createProcessorClient(client), {
  // All your handlers...
});

processor.start();
console.log("Event processor started");

// Graceful shutdown
const shutdown = async () => {
  console.log("Shutting down...");
  await processor.stop();
  await client.end();
  process.exit(0);
};

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
```

Run multiple instances for horizontal scaling:

```bash
# Terminal 1
node processor-service.js

# Terminal 2
node processor-service.js

# Terminal 3
node processor-service.js
```

All three will coordinate using database row locking (`FOR UPDATE SKIP LOCKED`).

## API Reference

### `EventProcessor(client, handlerMap, options?)`

Creates and returns a processor instance with `start()` and `stop()` methods.

**Parameters:**

- `client`: `TxOBProcessorClient<EventType>` - Database client
- `handlerMap`: `TxOBEventHandlerMap<EventType>` - Map of event types to handlers
- `options?`: `ProcessorOptions` - Configuration options

**Returns:**

```typescript
{
  start: () => void;
  stop: (opts?: { timeoutMs?: number }) => Promise<void>;
}
```

**Example:**

```typescript
const processor = EventProcessor(client, handlers);
processor.start();
await processor.stop({ timeoutMs: 10000 }); // 10 second timeout
```

### `createProcessorClient` (PostgreSQL)

Creates a PostgreSQL processor client.

```typescript
import { createProcessorClient } from "txob/pg";

createProcessorClient<EventType>(
  client: pg.Client,
  tableName?: string,    // Default: "events"
  limit?: number         // Default: 100
): TxOBProcessorClient<EventType>
```

### `createProcessorClient` (MongoDB)

Creates a MongoDB processor client.

```typescript
import { createProcessorClient } from "txob/mongodb";

createProcessorClient<EventType>(
  db: mongodb.Db,
  collectionName?: string,  // Default: "events"
  limit?: number            // Default: 100
): TxOBProcessorClient<EventType>
```

### `ErrorUnprocessableEventHandler`

Error class to mark a handler as unprocessable (stops retrying).

```typescript
import { ErrorUnprocessableEventHandler } from "txob";

throw new ErrorUnprocessableEventHandler(new Error("Invalid data"));
```

### Types

```typescript
// Main event type
type TxOBEvent<EventType extends string> = {
  id: string;
  timestamp: Date;
  type: EventType;
  data: Record<string, unknown>;
  correlation_id: string;
  handler_results: Record<string, TxOBEventHandlerResult>;
  errors: number;
  backoff_until?: Date | null;
  processed_at?: Date;
};

// Handler function signature
type TxOBEventHandler = (
  event: TxOBEvent,
  opts: { signal?: AbortSignal },
) => Promise<void>;

// Handler map structure
type TxOBEventHandlerMap<EventType extends string> = Record<
  EventType,
  Record<string, TxOBEventHandler>
>;

// Handler result tracking
type TxOBEventHandlerResult = {
  processed_at?: Date;
  unprocessable_at?: Date;
  errors?: Array<{
    error: unknown;
    timestamp: Date;
  }>;
};

// Logger interface
interface Logger {
  debug(message?: unknown, ...optionalParams: unknown[]): void;
  info(message?: unknown, ...optionalParams: unknown[]): void;
  warn(message?: unknown, ...optionalParams: unknown[]): void;
  error(message?: unknown, ...optionalParams: unknown[]): void;
}
```

## Best Practices

### ✅ Do

- **Keep handlers focused** - Each handler should perform a single, independent side effect (send email, call webhook, etc.)
- **Make handlers idempotent** - Check if work was already done before doing it again
- **Use correlation IDs** - Essential for tracing and debugging distributed systems
- **Set appropriate maxErrors** - Balance between retry attempts and failure detection
- **Monitor handler performance** - Track execution time and error rates
- **Use AbortSignal** - Implement quick cleanup during graceful shutdown
- **Create indexes** - The partial index on `processed_at` is critical for performance
- **Validate event data** - Throw `ErrorUnprocessableEventHandler` for invalid data
- **Use transactions** - Always save events with business data in the same transaction
- **Test handlers** - Unit test handlers independently with mock events
- **Log with context** - Include `event.id` and `correlation_id` in all logs

### ❌ Don't

- **Don't assume exactly-once** - Handlers may be called multiple times
- **Don't require event ordering** - Handlers should be independent; if you need ordering, reconsider your design
- **Don't perform long operations without signal checks** - Delays shutdown
- **Don't ignore errors** - Handle them appropriately or let them propagate
- **Don't skip the indexes** - Performance will degrade rapidly
- **Don't save events outside transactions** - Defeats the purpose of the outbox pattern
- **Don't use for real-time processing** - Polling introduces latency (default 5s)
- **Don't modify events in handlers** - Event object is read-only
- **Don't share mutable state** - Handlers may run concurrently
- **Don't forget correlation IDs** - Makes debugging distributed issues very difficult

### Performance Tips

1. **Tune concurrency limits** - Adjust `maxEventConcurrency` and `maxHandlerConcurrency` based on your workload
2. **Reduce polling interval** - Lower `sleepTimeMs` for lower latency (at cost of more database queries)
3. **Batch operations** - If handlers can batch work, collect multiple events
4. **Monitor query performance** - Use `EXPLAIN ANALYZE` on the `getEventsToProcess` query
5. **Partition the events table** - For very high volume, partition by `processed_at` or `timestamp`
6. **Archive processed events** - Move old processed events to archive table to keep main table small

## Troubleshooting

### Events are not being processed

**Check:**

1. Is the processor started? `processor.start()` was called?
2. Is the database connection working?
3. Are events actually being saved? Query the events table
4. Is `processed_at` NULL on pending events?
5. Is `backoff_until` in the past (or NULL)?
6. Is `errors` less than `maxErrors`?

**Debug:**

```typescript
EventProcessor(client, handlers, {
  logger: console, // Enable logging
});
```

### High error rates

**Check:**

1. Are handlers throwing errors? Check logs
2. Is an external service down? (email, API, etc.)
3. Is event data invalid? Add validation
4. Are handlers timing out? Increase timeouts

**Solutions:**

- Use `ErrorUnprocessableEventHandler` for invalid data
- Implement circuit breakers for external services
- Add retries within handlers for transient failures
- Increase `maxErrors` if failures are expected

### Events stuck in "processing" state

This happens when:

1. Processor crashed after locking event but before updating
2. Transaction was rolled back

**Solution:** Events are never truly "stuck" - they're locked at the transaction level. Once the transaction ends (commit or rollback), the lock is released and another processor can pick it up.

If using `FOR UPDATE SKIP LOCKED` properly (which txob does), stuck events are not possible.

### Performance is slow

**Check:**

1. Do you have the recommended indexes?
2. How many unprocessed events are in the table?
3. What's your `maxEventConcurrency` setting?
4. Are handlers slow? Profile them

**Solutions:**

- Create the partial index on `processed_at`
- Archive or delete old processed events
- Increase `maxEventConcurrency`
- Optimize slow handlers
- Run multiple processor instances

### Memory usage is high

**Check:**

1. How many events are processed concurrently?
2. Are handlers leaking memory?
3. Is the events table huge?

**Solutions:**

- Lower `maxEventConcurrency`
- Profile handlers for memory leaks
- Archive old events
- Reduce `limit` in `createProcessorClient(client, table, limit)`

### Duplicate handler executions

This is expected behavior due to **at-least-once delivery**. It happens when:

- Event update fails after handler succeeds
- Processor crashes after handler succeeds but before updating event

**Solution:** Make handlers idempotent:

```typescript
// ✅ Idempotent: Check before doing work
const handler = async (event) => {
  const alreadyDone = await checkWorkStatus(event.id);
  if (alreadyDone) return;

  await doWork(event.data);
  await markWorkDone(event.id);
};
```

## Frequently Asked Questions

### How is this different from a message queue?

**The fundamental problem with message queues:**

Message queues (RabbitMQ, SQS, Kafka) are **separate systems** from your database. You cannot make both operations atomic:

```typescript
// ❌ This is NOT atomic - the queue and database are separate systems
await db.query("BEGIN");
await db.createUser(user);
await messageQueue.publish("user.created", user); // ✅ Succeeds
await db.query("COMMIT"); // 💥 Fails! Message is queued but user doesn't exist
```

Even if you publish after commit, you have the opposite problem:

```typescript
// ❌ Also NOT atomic
await db.query("BEGIN");
await db.createUser(user);
await db.query("COMMIT"); // ✅ Succeeds
await messageQueue.publish("user.created", user); // 💥 Fails! User exists but no message
```

**Message queues** require:

- Additional infrastructure to run and monitor
- Network calls to publish messages (can fail independently of database)
- Handling connection failures
- **No way to guarantee consistency** between database and queue
- Complex error recovery (replay, reconciliation, etc.)

**Transactional Outbox** with txob:

- Uses your existing database (no additional infrastructure)
- **Guaranteed consistency** - events saved in same transaction as data (atomicity via ACID)
- No network calls during transaction (everything is in one database)
- Simpler operational model
- If transaction fails, neither data nor events are saved
- If transaction succeeds, both data and events are saved

**Trade-offs:**

- Message queues: Lower latency (~10ms), higher throughput (10k+/s)
- txob: Higher latency (~5s default), moderate throughput (10-100/s per processor)

**Can I use txob WITH message queues?**

**Yes!** This is actually a great pattern. Use txob to guarantee consistency, then publish to your queue from a handler:

```typescript
UserCreated: {
  publishToKafka: async (event) => {
    // Now this is guaranteed to only run if user was created
    await kafka.publish("user.created", event.data);
    // If this fails, txob will retry it
  };
}
```

This gives you:

- ✅ Guaranteed consistency between database and queue (via txob)
- ✅ Low latency downstream (via message queue)
- ✅ Idempotent publishing (txob handles retries)

### Can I use this without microservices?

**Yes!** The transactional outbox pattern is useful in any application that needs reliable side-effects:

- Monoliths that send emails
- Single-service apps that call webhooks
- Any app that needs guaranteed event delivery

You don't need a microservices architecture to benefit from txob.

### What happens if the processor crashes?

The processor is designed to handle crashes gracefully:

1. **During handler execution:** The transaction hasn't committed yet, so the event remains unprocessed. Another processor (or restart) will pick it up.
2. **After handler but before update:** Same as above - event remains unprocessed.
3. **During event update:** Database transaction ensures atomicity. Either the update completes or it doesn't.

**Result:** Events are never lost. At worst, handlers are called again (which is why idempotency matters).

### How do I handle duplicate handler executions?

Make your handlers **idempotent** by checking if work was already done:

```typescript
// Pattern 1: Check external system
const sendEmail = async (event) => {
  const sent = await emailService.checkSent(event.id);
  if (sent) return; // Already sent

  await emailService.send(event.data.email);
};

// Pattern 2: Use unique constraints
const createStripeCustomer = async (event) => {
  try {
    await stripe.customers.create({
      id: event.data.userId, // Stripe will reject if already exists
      email: event.data.email,
    });
  } catch (err) {
    if (err.code === "resource_already_exists") return; // Already created
    throw err;
  }
};

// Pattern 3: Track in database
const processPayment = async (event) => {
  const processed = await db.query(
    "SELECT 1 FROM payment_events WHERE event_id = $1",
    [event.id],
  );
  if (processed.rowCount > 0) return;

  await processPayment(event.data);
  await db.query("INSERT INTO payment_events (event_id) VALUES ($1)", [
    event.id,
  ]);
};
```

### Can I process events in a specific order?

**Short answer: You generally shouldn't need to.**

Events are processed concurrently by default, and **handlers should contain single, independent side effects**. If you need ordering, it usually indicates a design issue.

**Why ordering is usually a design smell:**

- Each handler should represent one side effect (send email, call webhook, etc.)
- Side effects are typically independent and don't need ordering
- Ordering defeats the purpose of concurrent processing and reduces throughput
- If side effects must happen in sequence, they might belong in the same handler

**Better approaches:**

1. **Make side effects independent** (recommended):

```typescript
UserCreated: {
  sendEmail: async (event) => { /* sends welcome email */ },
  createStripeCustomer: async (event) => { /* creates customer */ },
  // These can run in any order or concurrently ✅
}
```

2. **If truly dependent, combine into one handler**:

```typescript
UserCreated: {
  completeOnboarding: async (event) => {
    // These MUST happen in order
    await createStripeCustomer(event.data);
    await sendWelcomeEmail(event.data);
    await enrollInTrial(event.data);
  };
}
```

3. **Use separate event types** for workflows:

```typescript
// Event 1 creates the customer
UserCreated: {
  createStripeCustomer: async (event) => {
    await stripe.customers.create(event.data);
    // Create next event when done
    await createEvent({ type: "StripeCustomerCreated", ... });
  }
}

// Event 2 sends the email
StripeCustomerCreated: {
  sendWelcomeEmail: async (event) => {
    await emailService.send(event.data);
  }
}
```

**If you absolutely must process events sequentially** (not recommended):

```typescript
EventProcessor(client, handlers, {
  maxEventConcurrency: 1, // Forces sequential processing
  // ⚠️ This sacrifices throughput and concurrency benefits
});
```

### How do I monitor event processing?

**1. Use the logger option:**

```typescript
EventProcessor(client, handlers, {
  logger: myLogger, // Logs all processing activity
});
```

**2. Query the events table:**

```sql
-- Pending events
SELECT COUNT(*) FROM events WHERE processed_at IS NULL;

-- Failed events (max errors reached)
SELECT * FROM events WHERE errors >= 5 AND processed_at IS NOT NULL;

-- Events by type
SELECT type, COUNT(*) FROM events GROUP BY type;

-- Average processing time (requires timestamp tracking)
SELECT type, AVG(processed_at - timestamp) as avg_duration
FROM events WHERE processed_at IS NOT NULL
GROUP BY type;
```

**3. Create monitoring events:**

```typescript
onEventMaxErrorsReached: async ({ event, txClient }) => {
  await txClient.createEvent({
    id: randomUUID(),
    type: "EventFailed",
    data: { originalEvent: event },
    correlation_id: event.correlation_id,
    handler_results: {},
    errors: 0,
  });

  // Send to monitoring service
  await monitoring.recordFailure(event);
};
```

### Can I use this with TypeScript?

Yes! txob is written in TypeScript and provides full type safety:

```typescript
// Define your event types
const eventTypes = {
  UserCreated: "UserCreated",
  OrderPlaced: "OrderPlaced",
} as const;

type EventType = keyof typeof eventTypes;

// TypeScript will enforce all event types have handlers
const processor = EventProcessor<EventType>(
  createProcessorClient<EventType>(client),
  {
    UserCreated: {
      /* handlers */
    },
    OrderPlaced: {
      /* handlers */
    },
    // Missing an event type? TypeScript error!
  },
);
```

### What's the performance impact?

**Database Impact:**

- One SELECT query per polling interval (default: every 5 seconds)
- One SELECT + UPDATE per event processed
- With proper indexes, queries are very fast (< 10ms typically)

**Processing Latency:**

- Average latency: `sleepTimeMs / 2` (default: 2.5 seconds)
- Worst case: `sleepTimeMs` (default: 5 seconds)

**Throughput:**

- Depends on handler speed and concurrency settings
- Single processor: 10-100 events/second typical
- Horizontally scalable: add more processors for higher throughput

**Optimization:**

- Lower `sleepTimeMs` for lower latency (at cost of more queries)
- Increase `maxEventConcurrency` for higher throughput
- Run multiple processors for horizontal scaling

### How does horizontal scaling work?

Run multiple processor instances (same code, different processes/machines):

```bash
# Machine 1
node processor.js

# Machine 2
node processor.js

# Machine 3
node processor.js
```

Each processor will:

1. Query for unprocessed events
2. Lock events using `FOR UPDATE SKIP LOCKED`
3. Process locked events
4. Release locks on commit/rollback

**Key mechanism:** `FOR UPDATE SKIP LOCKED` ensures each event is locked by only one processor. Other processors skip locked rows and process different events.

**No coordination needed** - processors don't need to know about each other. The database handles coordination.

### Can I prioritize certain events?

Yes, modify the query in your custom client:

```typescript
// Custom client with priority
const getEventsToProcess = async (opts) => {
  const events = await client.query(
    `SELECT id, errors FROM events
     WHERE processed_at IS NULL
     AND (backoff_until IS NULL OR backoff_until < NOW())
     AND errors < $1
     ORDER BY priority DESC, timestamp ASC  -- High priority first
     LIMIT 100`,
    [opts.maxErrors],
  );
  return events.rows;
};
```

Add a `priority` column to your events table.

## When to Use txob

### ✅ Use txob when:

- You need **guaranteed event delivery** (can't lose events)
- You want to avoid **distributed transactions** (2PC, Saga)
- You're already using **PostgreSQL or MongoDB**
- You need **at-least-once delivery** semantics
- You can make handlers **idempotent**
- You're building **reliable background processing**
- You want **simple infrastructure** (no separate message queue)
- You need **horizontal scalability** without coordination

### ⚠️ Consider alternatives when:

- You need **exactly-once semantics** (use Kafka with transactions)
- You need **real-time processing** (< 1 second latency) - use message queue
- You need **high throughput** (> 10k events/second) - use message queue
- You already have **message queue infrastructure** you're happy with
- You can't make handlers **idempotent**
- You need **complex routing** or pub/sub patterns - use message broker

### Comparison with alternatives:

| Feature                | txob                   | RabbitMQ         | Kafka            | AWS SQS         |
| ---------------------- | ---------------------- | ---------------- | ---------------- | --------------- |
| Infrastructure         | Database only          | Separate service | Separate cluster | Managed service |
| Consistency            | Strong (ACID)          | Eventual         | Eventual         | Eventual        |
| Latency                | ~5s default            | ~10ms            | ~10ms            | ~1s             |
| Throughput             | 10-100/s per processor | 10k+/s           | 100k+/s          | 3k/s            |
| Horizontal scaling     | ✅ Yes                 | ✅ Yes           | ✅ Yes           | ✅ Yes          |
| Exactly-once           | ❌ No                  | ❌ No            | ✅ Yes           | ❌ No           |
| Operational complexity | Low                    | Medium           | High             | Low             |
| Cost                   | DB storage             | Self-hosted      | Self-hosted      | Pay per request |

## Contributing

Contributions are welcome! To contribute:

1. **Fork the repository**
2. **Create a feature branch:** `git checkout -b feature/my-feature`
3. **Make your changes** with tests
4. **Run tests:** `npm test`
5. **Run linting:** `npm run format`
6. **Commit your changes:** `git commit -m "Add my feature"`
7. **Push to your fork:** `git push origin feature/my-feature`
8. **Open a Pull Request**

**Guidelines:**

- Add tests for new features
- Update documentation for API changes
- Follow existing code style
- Keep PRs focused on a single concern

## Examples

See the [examples](./examples) directory for complete working examples:

- **[PostgreSQL example](./examples/pg)** - HTTP API with user creation and email sending
- More examples coming soon!

## Support & Community

- 📖 **Documentation:** You're reading it!
- 🐛 **Bug Reports:** [GitHub Issues](https://github.com/dillonstreator/txob/issues)

## Learn More

- [Transactional Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html) - Detailed explanation of the pattern
- [Implementing the Outbox Pattern](https://debezium.io/blog/2019/02/19/reliable-microservices-data-exchange-with-the-outbox-pattern/) - Debezium blog post
- [Event-Driven Architecture](https://martinfowler.com/articles/201701-event-driven.html) - Martin Fowler

## License

MIT © [Dillon Streator](https://github.com/dillonstreator)

## Acknowledgments

Implements the [Transactional Outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html) microservices patterns.
