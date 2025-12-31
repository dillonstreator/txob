import { vi, describe, it, expect } from "vitest";
import { createProcessorClient } from "./client.js";

describe("createProcessorClient", () => {
  it("should create a client with the correct functions", async () => {
    const pgClient = {
      query: vi.fn(),
    } as any;
    const client = createProcessorClient({ querier: pgClient });
    expect(typeof client.getEventsToProcess).toBe("function");
    expect(typeof client.transaction).toBe("function");
  });
});

describe("getEventsToProcess", () => {
  it("should execute the correct query", async () => {
    const rows = [1, 2, 3];
    const pgClient = {
      query: vi.fn<any>(() =>
        Promise.resolve({
          rows,
        }),
      ),
    } as any;
    const opts = {
      maxErrors: 10,
    };
    const client = createProcessorClient({ querier: pgClient });
    const result = await client.getEventsToProcess(opts);
    expect(pgClient.query).toHaveBeenCalledOnce();
    expect(pgClient.query).toHaveBeenCalledWith(
      'SELECT id, errors FROM "events" WHERE processed_at IS NULL AND (backoff_until IS NULL OR backoff_until < NOW()) AND errors < $1 ORDER BY timestamp ASC LIMIT 100',
      [opts.maxErrors],
    );
    expect(result).toBe(rows);
  });
});

describe("transaction", () => {
  it("should begin and commit", async () => {
    const pgClient = {
      query: vi.fn<any>(() => Promise.resolve()),
    } as any;
    const client = createProcessorClient({ querier: pgClient });
    await client.transaction(async () => {});
    expect(pgClient.query).toHaveBeenCalledTimes(2);
    expect(pgClient.query).toHaveBeenNthCalledWith(1, "BEGIN");
    expect(pgClient.query).toHaveBeenNthCalledWith(2, "COMMIT");
  });
  it("should begin and rollback", async () => {
    const pgClient = {
      query: vi.fn<any>(() => Promise.resolve()),
    } as any;
    const client = createProcessorClient({ querier: pgClient });
    await client
      .transaction(async () => {
        throw new Error("error");
      })
      .catch(() => {});
    expect(pgClient.query).toHaveBeenCalledTimes(2);
    expect(pgClient.query).toHaveBeenNthCalledWith(1, "BEGIN");
    expect(pgClient.query).toHaveBeenNthCalledWith(2, "ROLLBACK");
  });

  describe("getEventByIdForUpdateSkipLocked", () => {
    it("should execute the correct query", async () => {
      const rows = [1, 2, 3];
      const pgClient = {
        query: vi.fn<any>(() =>
          Promise.resolve({
            rows,
            rowCount: rows.length,
          }),
        ),
      } as any;
      const eventId = "123";
      const client = createProcessorClient({ querier: pgClient });
      let result: any;
      await client.transaction(async (txClient) => {
        result = await txClient.getEventByIdForUpdateSkipLocked(eventId, {
          maxErrors: 6,
        });
      });

      expect(pgClient.query).toHaveBeenCalledTimes(3);
      expect(pgClient.query).toHaveBeenCalledWith(
        'SELECT id, timestamp, type, data, correlation_id, handler_results, errors, backoff_until, processed_at FROM "events" WHERE id = $1 AND processed_at IS NULL AND (backoff_until IS NULL OR backoff_until < NOW()) AND errors < $2 FOR UPDATE SKIP LOCKED',
        [eventId, 6],
      );
      expect(result).toBe(1);
    });

    it("should return null on no rows", async () => {
      const rows = [];
      const pgClient = {
        query: vi.fn<any>(() =>
          Promise.resolve({
            rows,
            rowCount: rows.length,
          }),
        ),
      } as any;
      const eventId = "123";
      const client = createProcessorClient({ querier: pgClient });
      let result: any;
      await client.transaction(async (txClient) => {
        result = await txClient.getEventByIdForUpdateSkipLocked(eventId, {
          maxErrors: 5,
        });
      });

      expect(pgClient.query).toHaveBeenCalledTimes(3);
      expect(pgClient.query).toHaveBeenCalledWith(
        'SELECT id, timestamp, type, data, correlation_id, handler_results, errors, backoff_until, processed_at FROM "events" WHERE id = $1 AND processed_at IS NULL AND (backoff_until IS NULL OR backoff_until < NOW()) AND errors < $2 FOR UPDATE SKIP LOCKED',
        [eventId, 5],
      );
      expect(result).toBeNull();
    });
  });

  describe("updateEvent", () => {
    it("should execute the correct query", async () => {
      const rows = [1, 2, 3];
      const pgClient = {
        query: vi.fn<any>(() =>
          Promise.resolve({
            rows,
          }),
        ),
      } as any;
      const event = {
        id: "1",
        handler_results: {},
        errors: 2,
        processed_at: new Date(),
        backoff_until: new Date(),
        timestamp: new Date(),
        type: "type",
        data: {
          thing1: "something",
        },
        correlation_id: "abc123",
      };
      const client = createProcessorClient({ querier: pgClient });
      await client.transaction(async (txClient) => {
        await txClient.updateEvent(event);
      });

      expect(pgClient.query).toHaveBeenCalledTimes(3);
      expect(pgClient.query).toHaveBeenCalledWith(
        'UPDATE "events" SET handler_results = $1, errors = $2, processed_at = $3, backoff_until = $4 WHERE id = $5',
        [
          event.handler_results,
          event.errors,
          event.processed_at,
          event.backoff_until,
          event.id,
        ],
      );
    });
  });

  describe("createEvent", () => {
    it("should execute the correct query", async () => {
      const pgClient = {
        query: vi.fn<any>(() => Promise.resolve()),
      } as any;
      const event = {
        id: "1",
        timestamp: new Date(),
        type: "test_event",
        data: {
          thing1: "something",
        },
        correlation_id: "abc123",
        handler_results: {},
        errors: 0,
      };
      const client = createProcessorClient({ querier: pgClient });
      await client.transaction(async (txClient) => {
        await txClient.createEvent(event);
      });

      expect(pgClient.query).toHaveBeenCalledTimes(3);
      expect(pgClient.query).toHaveBeenCalledWith(
        'INSERT INTO "events" (id, timestamp, type, data, correlation_id, handler_results, errors) VALUES ($1, $2, $3, $4, $5, $6, $7)',
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
    });
  });

  describe("rollback failure", () => {
    it("should throw combined error when both transaction and rollback fail", async () => {
      const transactionError = new Error("transaction failed");
      const rollbackError = new Error("rollback failed");
      let queryCount = 0;

      const pgClient = {
        query: vi.fn<any>((sql: string) => {
          queryCount++;
          if (sql === "BEGIN") {
            // BEGIN succeeds
            return Promise.resolve();
          } else if (sql === "ROLLBACK") {
            // ROLLBACK fails
            return Promise.reject(rollbackError);
          } else {
            // COMMIT or other operations
            return Promise.resolve();
          }
        }),
      } as any;

      const client = createProcessorClient({ querier: pgClient });

      try {
        await client.transaction(async () => {
          throw transactionError;
        });
        expect.fail("should have thrown an error");
      } catch (error: any) {
        expect(error.message).toBe(
          "Transaction failed: transaction failed (rollback also failed: rollback failed)",
        );
        expect(error.cause).toBe(transactionError);
      }

      expect(pgClient.query).toHaveBeenCalledTimes(2);
      expect(pgClient.query).toHaveBeenNthCalledWith(1, "BEGIN");
      expect(pgClient.query).toHaveBeenNthCalledWith(2, "ROLLBACK");
    });
  });
});
