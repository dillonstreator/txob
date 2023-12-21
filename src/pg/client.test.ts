import { vi, describe, it, expect } from "vitest";
import { createProcessorClient } from "./client";

describe("createProcessorClient", () => {
  it("should create a client with the correct functions", async () => {
    const pgClient = {
      query: vi.fn(),
    };
    const client = createProcessorClient(pgClient);
    expect(typeof client.getUnprocessedEvents).toBe("function");
    expect(typeof client.transaction).toBe("function");
  });
});

describe("getUnprocessedEvents", () => {
  it("should execute the correct query", async () => {
    const rows = [1, 2, 3];
    const pgClient = {
      query: vi.fn<any, any>(() =>
        Promise.resolve({
          rows,
        }),
      ),
    };
    const opts = {
      maxErrors: 10,
    };
    const client = createProcessorClient(pgClient);
    const result = await client.getUnprocessedEvents(opts);
    expect(pgClient.query).toHaveBeenCalledOnce();
    expect(pgClient.query).toHaveBeenCalledWith(
      "SELECT id, errors FROM $1 WHERE processed_at IS NULL AND (backoff_until IS NULL OR backoff_until < NOW()) AND errors < $2",
      ["events", opts.maxErrors],
    );
    expect(result).toBe(rows);
  });
});

describe("transaction", () => {
  it("should begin and commit", async () => {
    const pgClient = {
      query: vi.fn<any, any>(() => Promise.resolve()),
    };
    const client = createProcessorClient(pgClient);
    await client.transaction(async () => {});
    expect(pgClient.query).toHaveBeenCalledTimes(2);
    expect(pgClient.query).toHaveBeenNthCalledWith(1, "BEGIN");
    expect(pgClient.query).toHaveBeenNthCalledWith(2, "COMMIT");
  });
  it("should begin and rollback", async () => {
    const pgClient = {
      query: vi.fn<any, any>(() => Promise.resolve()),
    };
    const client = createProcessorClient(pgClient);
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
        query: vi.fn<any, any>(() =>
          Promise.resolve({
            rows,
            rowCount: rows.length,
          }),
        ),
      };
      const eventId = "123";
      const client = createProcessorClient(pgClient);
      let result: any;
      await client.transaction(async (txClient) => {
        result = await txClient.getEventByIdForUpdateSkipLocked(eventId, {});
      });

      expect(pgClient.query).toHaveBeenCalledTimes(3);
      expect(pgClient.query).toHaveBeenCalledWith(
        "SELECT id, timestamp, type, data, correlation_id, handler_results, errors, backoff_until, processed_at FROM $1 WHERE id = $2 FOR UPDATE SKIP LOCKED",
        ["events", eventId],
      );
      expect(result).toBe(1);
    });

    it("should return null on no rows", async () => {
      const rows = [];
      const pgClient = {
        query: vi.fn<any, any>(() =>
          Promise.resolve({
            rows,
            rowCount: rows.length,
          }),
        ),
      };
      const eventId = "123";
      const client = createProcessorClient(pgClient);
      let result: any;
      await client.transaction(async (txClient) => {
        result = await txClient.getEventByIdForUpdateSkipLocked(eventId, {});
      });

      expect(pgClient.query).toHaveBeenCalledTimes(3);
      expect(pgClient.query).toHaveBeenCalledWith(
        "SELECT id, timestamp, type, data, correlation_id, handler_results, errors, backoff_until, processed_at FROM $1 WHERE id = $2 FOR UPDATE SKIP LOCKED",
        ["events", eventId],
      );
      expect(result).toBeNull();
    });
  });

  describe("updateEvent", () => {
    it("should execute the correct query", async () => {
      const rows = [1, 2, 3];
      const pgClient = {
        query: vi.fn<any, any>(() =>
          Promise.resolve({
            rows,
          }),
        ),
      };
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
      const client = createProcessorClient(pgClient);
      await client.transaction(async (txClient) => {
        await txClient.updateEvent(event);
      });

      expect(pgClient.query).toHaveBeenCalledTimes(3);
      expect(pgClient.query).toHaveBeenCalledWith(
        "UPDATE $1 SET handler_results = $2, errors = $3, processed_at = $4, backoff_until = $5 WHERE id = $6",
        [
          "events",
          event.handler_results,
          event.errors,
          event.processed_at,
          event.backoff_until,
          event.id,
        ],
      );
    });
  });
});
