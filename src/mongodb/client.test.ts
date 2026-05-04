import { vi, describe, it, expect, beforeEach } from "vitest";
import { ObjectId } from "mongodb";
import type { TxOBEvent } from "../processor.js";
import { createProcessorClient, createWakeupEmitter } from "./client.js";

const fixedNow = new Date("2024-06-01T12:00:00.000Z");

vi.mock("../date.js", () => ({
  getDate: () => fixedNow,
}));

const eventSchemas = {
  TestEvent: {
    "~standard": {
      version: 1 as const,
      vendor: "test",
      validate: (value: unknown) => ({
        value:
          typeof value === "object" && value !== null
            ? (value as Record<string, unknown>)
            : {},
      }),
    },
  },
};

function createMongoMocks() {
  const toArray = vi.fn();
  const findChain = {
    project: vi.fn().mockReturnThis(),
    limit: vi.fn().mockReturnThis(),
    sort: vi.fn().mockReturnThis(),
    toArray,
  };
  const findOneAndUpdate = vi.fn();
  const updateOne = vi.fn();
  const insertOne = vi.fn();
  const watch = vi.fn();

  const collection = vi.fn(() => ({
    find: vi.fn(() => findChain),
    findOneAndUpdate,
    updateOne,
    insertOne,
    watch,
  }));

  const db = vi.fn(() => ({
    collection,
  }));

  const session = {
    withTransaction: vi.fn(async (fn: () => Promise<void>) => {
      await fn();
    }),
  };

  const mongo = {
    db,
    withSession: vi.fn(async (fn: (s: typeof session) => Promise<void>) => {
      await fn(session);
    }),
  };

  return {
    mongo: mongo as any,
    toArray,
    findOneAndUpdate,
    updateOne,
    insertOne,
    watch,
    collection,
    session,
  };
}

beforeEach(() => {
  vi.clearAllMocks();
});

describe("createProcessorClient (MongoDB)", () => {
  it("getEventsToProcess queries with ready-to-process filter", async () => {
    const rows = [{ id: "e1", errors: 0 }];
    const { mongo, toArray, collection } = createMongoMocks();
    toArray.mockResolvedValue(rows);

    const client = createProcessorClient({
      mongo,
      db: "app",
      collection: "outbox",
      limit: 50,
      eventSchemas,
    });

    const result = await client.getEventsToProcess({ maxErrors: 3 });

    expect(mongo.db).toHaveBeenCalledWith("app");
    expect(collection).toHaveBeenCalledWith("outbox");
    expect(result).toEqual(rows);

    const coll = collection.mock.results[0].value;
    expect(coll.find).toHaveBeenCalledWith({
      processed_at: null,
      $and: [
        {
          $or: [
            { backoff_until: null },
            { backoff_until: { $lt: fixedNow } },
          ],
        },
        {
          $or: [{ lock: null }, { lock: { $exists: false } }],
        },
      ],
      errors: { $lt: 3 },
    });
    const findReturn = coll.find.mock.results[0].value;
    expect(findReturn.project).toHaveBeenCalledWith({ id: 1, errors: 1 });
    expect(findReturn.limit).toHaveBeenCalledWith(50);
    expect(findReturn.sort).toHaveBeenCalledWith("timestamp", "asc");
  });

  it("transaction runs getEventByIdForUpdateSkipLocked and returns row", async () => {
    const { mongo, findOneAndUpdate } = createMongoMocks();
    const doc = {
      id: "1",
      timestamp: fixedNow,
      type: "TestEvent",
      data: {},
      correlation_id: "c",
      handler_results: {},
      errors: 0,
      backoff_until: null,
      processed_at: null,
    };
    findOneAndUpdate.mockResolvedValue({ value: doc });

    const client = createProcessorClient({
      mongo,
      db: "app",
      eventSchemas,
    });

    let loaded: (typeof doc) | null = null;
    await client.transaction(async (tx) => {
      loaded = (await tx.getEventByIdForUpdateSkipLocked("1", {
        maxErrors: 5,
      })) as (typeof doc) | null;
    });

    expect(loaded).toEqual(doc);
    expect(findOneAndUpdate).toHaveBeenCalledOnce();
    const [filter, update, opts] = findOneAndUpdate.mock.calls[0];
    expect(filter).toMatchObject({ id: "1" });
    expect(update.$set.lock).toBeInstanceOf(ObjectId);
    expect(opts.returnDocument).toBe("after");
    expect(opts.session).toBeDefined();
  });

  it("getEventByIdForUpdateSkipLocked returns null when no document", async () => {
    const { mongo, findOneAndUpdate } = createMongoMocks();
    findOneAndUpdate.mockResolvedValue(null);

    const client = createProcessorClient({
      mongo,
      db: "app",
      eventSchemas,
    });

    let loaded: unknown;
    await client.transaction(async (tx) => {
      loaded = await tx.getEventByIdForUpdateSkipLocked("missing", {
        maxErrors: 1,
      });
    });

    expect(loaded).toBeNull();
  });

  it("updateEvent and createEvent call driver with expected payloads", async () => {
    const { mongo, updateOne, insertOne } = createMongoMocks();

    const client = createProcessorClient({
      mongo,
      db: "app",
      eventSchemas,
    });

    const event = {
      id: "e1",
      timestamp: fixedNow,
      type: "TestEvent" as const,
      data: { x: 1 },
      correlation_id: "corr",
      handler_results: { h: {} },
      errors: 1,
      backoff_until: null,
      processed_at: null,
    } as unknown as TxOBEvent<"TestEvent", Record<string, unknown>>;

    await client.transaction(async (tx) => {
      await tx.updateEvent(event);
      await tx.createEvent({
        id: "e2",
        timestamp: fixedNow,
        type: "TestEvent",
        data: {},
        correlation_id: "c2",
        handler_results: {},
        errors: 0,
      });
    });

    expect(updateOne).toHaveBeenCalledWith(
      { id: "e1" },
      {
        $set: {
          handler_results: event.handler_results,
          errors: 1,
          processed_at: null,
          backoff_until: null,
          lock: null,
        },
      },
      expect.objectContaining({ session: expect.anything() }),
    );

    expect(insertOne).toHaveBeenCalledWith(
      {
        id: "e2",
        timestamp: fixedNow,
        type: "TestEvent",
        data: {},
        correlation_id: "c2",
        handler_results: {},
        errors: 0,
        processed_at: null,
        backoff_until: null,
        lock: null,
      },
      expect.objectContaining({ session: expect.anything() }),
    );
  });
});

describe("createWakeupEmitter (MongoDB)", () => {
  it("emits wakeup on insert change and closes the stream", async () => {
    const handlers: Record<string, ((...args: unknown[]) => void)[]> = {};
    const changeStream = {
      on: vi.fn((ev: string, fn: (...args: unknown[]) => void) => {
        (handlers[ev] ??= []).push(fn);
      }),
      close: vi.fn().mockResolvedValue(undefined),
    };

    const collection = vi.fn(() => ({
      watch: vi.fn(() => changeStream),
    }));

    const mongo = {
      db: vi.fn(() => ({ collection })),
    } as any;

    const emitter = await createWakeupEmitter({
      mongo,
      db: "app",
      collection: "events",
    });

    const wakeups: number[] = [];
    emitter.on("wakeup", () => wakeups.push(1));

    for (const fn of handlers["change"] ?? []) {
      fn({});
    }
    expect(wakeups).toHaveLength(1);

    await emitter.close();
    expect(changeStream.close).toHaveBeenCalledOnce();
  });

  it("forwards change stream errors to error listeners", async () => {
    const handlers: Record<string, ((...args: unknown[]) => void)[]> = {};
    const changeStream = {
      on: vi.fn((ev: string, fn: (...args: unknown[]) => void) => {
        (handlers[ev] ??= []).push(fn);
      }),
      close: vi.fn().mockResolvedValue(undefined),
    };

    const mongo = {
      db: vi.fn(() => ({
        collection: vi.fn(() => ({
          watch: vi.fn(() => changeStream),
        })),
      })),
    } as any;

    const emitter = await createWakeupEmitter({ mongo, db: "app" });
    const errors: unknown[] = [];
    const ee = emitter as unknown as import("node:events").EventEmitter;
    ee.on("error", (e: unknown) => errors.push(e));

    const streamErr = new Error("replica set required");
    for (const fn of handlers["error"] ?? []) {
      fn(streamErr);
    }
    expect(errors).toEqual([streamErr]);
  });

  it("emits error when change stream closes", async () => {
    const handlers: Record<string, ((...args: unknown[]) => void)[]> = {};
    const changeStream = {
      on: vi.fn((ev: string, fn: (...args: unknown[]) => void) => {
        (handlers[ev] ??= []).push(fn);
      }),
      close: vi.fn().mockResolvedValue(undefined),
    };

    const mongo = {
      db: vi.fn(() => ({
        collection: vi.fn(() => ({
          watch: vi.fn(() => changeStream),
        })),
      })),
    } as any;

    const emitter = await createWakeupEmitter({ mongo, db: "app" });
    const errors: unknown[] = [];
    const ee = emitter as unknown as import("node:events").EventEmitter;
    ee.on("error", (e: unknown) => errors.push(e));

    for (const fn of handlers["close"] ?? []) {
      fn();
    }
    expect(errors).toHaveLength(1);
    expect((errors[0] as Error).message).toBe("MongoDB Change Stream closed");
  });

  it("allows unregistering wakeup listener via off", async () => {
    const changeStream = {
      on: vi.fn(),
      close: vi.fn().mockResolvedValue(undefined),
    };
    const mongo = {
      db: vi.fn(() => ({
        collection: vi.fn(() => ({
          watch: vi.fn(() => changeStream),
        })),
      })),
    } as any;

    const emitter = await createWakeupEmitter({ mongo, db: "app" });
    const fn = () => {};
    emitter.on("wakeup", fn);
    emitter.off("wakeup", fn);
  });
});
