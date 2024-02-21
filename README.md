<h1 align="center">txob</h1>
<p align="center">A generic <a href="https://microservices.io/patterns/data/transactional-outbox.html">transactional outbox</a> event processor with graceful shutdown and horizontal scalability</p>
<p align="center">
  <a href="https://codecov.io/gh/dillonstreator/txob" >
    <img src="https://codecov.io/gh/dillonstreator/txob/graph/badge.svg?token=E9M7G67VLL"/>
  </a>
  <a aria-label="NPM version" href="https://www.npmjs.com/package/txob">
    <img alt="" src="https://badgen.net/npm/v/txob?v=0.0.19">
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

`txob` exposes an optionally configurable interface into event processing with control over maximum allowed errors, backoff calculation on error, event update retrying, and logging.

As per the 'transactional outbox specification', you should ensure your events are transactionally persisted alongside their related data mutations.

The processor handles graceful shutdown and is horizontally scalable by default with the native client implementatations for [`pg`](./src/pg/client.ts) and [`mongodb`](./src/mongodb/client.ts).

## Installation

```sh
(npm|yarn) (install|add) txob
```

### Examples

Let's look at an example of an HTTP API that allows a user to be invited where a email must be sent along with the invite.

```ts
import http from "node:http";
import { randomUUID } from "node:crypto";
import { Client } from "pg";
import gracefulShutdown from "http-graceful-shutdown";
import { EventProcessor } from "txob";
import { createProcessorClient } from "txob/pg";

const eventTypes = {
  UserInvited: "UserInvited",
  // other event types
} as const;

type EventType = keyof typeof eventTypes;

const client = new Client({
  user: process.env.POSTGRES_USER,
  password: process.env.POSTGRES_PASSWORD,
  database: process.env.POSTGRES_DB,
});
await client.connect();

const HTTP_PORT = process.env.PORT || 3000;

const processor = EventProcessor(
  createProcessorClient<EventType>(client),
  {
    UserInvited: {
      sendEmail: async (event, { signal }) => {
        // find user by event.data.userId to use relevant user data in email sending

        // email sending logic

        // use the AbortSignal `signal` to perform quick cleanup
        // during graceful shutdown enabling the processor to
        // save handler result updates to the event ASAP
      },
      publish: async (event) => {
        // publish to event bus
      },
      // other handler that should be executed when a `UserInvited` event is saved
    },
    // other event types
  }
)
processor.start();

const server = http.createServer(async (req, res) => {
  if (req.url  !== "/invite") return;

  // invite user endpoint

  const correlationId = randomUUID(); // or some value on the incoming request such as a request id

  try {
    await client.query("BEGIN");

    const userId = randomUUID();
    // save/create user with userId

    // save event to `events` table
    await client.query(
      `INSERT INTO events (id, type, data, correlation_id) VALUES ( $1, $2, $3, $4 )`,
      [
        randomUUID(),
        eventTypes.UserInvited,
        { userId }, // other relevant data
        correlationId,
      ],
    );

    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
  }
}).listen(HTTP_PORT, () => console.log(`listening on port ${HTTP_PORT}`));

gracefulShutdown(server, {
  onShutdown: async () => {
    // allow any actively running event handlers to finish
    // and the event processor to save the results
    await processor.stop();
  }
});
```

[other examples](./examples)
