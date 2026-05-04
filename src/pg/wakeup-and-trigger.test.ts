import { vi, describe, it, expect, beforeEach, afterEach } from "vitest";
import { EventEmitter } from "node:events";
import { createWakeupEmitter, createWakeupTrigger } from "./client.js";

const pgMocks = vi.hoisted(() => {
  const listenClients: Array<{
    query: ReturnType<typeof vi.fn>;
    connect: ReturnType<typeof vi.fn>;
    end: ReturnType<typeof vi.fn>;
    on: EventEmitter["on"];
    emit: EventEmitter["emit"];
  }> = [];
  class MockListenClient {
    query = vi.fn<any>(() => Promise.resolve());
    connect = vi.fn<any>(() => Promise.resolve());
    end = vi.fn<any>(() => Promise.resolve());
    private readonly emitter = new EventEmitter();
    on = this.emitter.on.bind(this.emitter);
    emit = this.emitter.emit.bind(this.emitter);
    constructor(public config?: unknown) {
      listenClients.push(this);
    }
  }
  return { listenClients, MockListenClient };
});

vi.mock("pg", async (importActual) => {
  const actual = await importActual<typeof import("pg")>();
  return {
    ...actual,
    Client: pgMocks.MockListenClient as unknown as typeof actual.Client,
  };
});

beforeEach(() => {
  pgMocks.listenClients.length = 0;
});

afterEach(() => {
  vi.clearAllMocks();
});

describe("createWakeupTrigger", () => {
  it("runs CREATE FUNCTION and CREATE TRIGGER SQL", async () => {
    const querier = { query: vi.fn<any>(() => Promise.resolve()) } as any;
    await createWakeupTrigger({ querier, table: "events", channel: "txob_events" });
    expect(querier.query).toHaveBeenCalledTimes(2);
    expect(String(querier.query.mock.calls[0][0])).toContain(
      "CREATE OR REPLACE FUNCTION",
    );
    expect(String(querier.query.mock.calls[1][0])).toContain("CREATE TRIGGER");
  });

  it("ignores duplicate_object (42710)", async () => {
    const querier = {
      query: vi.fn<any>(() =>
        Promise.reject(Object.assign(new Error("exists"), { code: "42710" })),
      ),
    } as any;
    await expect(
      createWakeupTrigger({ querier, table: "events" }),
    ).resolves.toBeUndefined();
  });

  it("ignores XX000 tuple concurrently updated", async () => {
    const querier = {
      query: vi.fn<any>(() =>
        Promise.reject(
          Object.assign(new Error("tuple concurrently updated"), {
            code: "XX000",
          }),
        ),
      ),
    } as any;
    await expect(createWakeupTrigger({ querier })).resolves.toBeUndefined();
  });

  it("ignores already exists trigger/relation message without code", async () => {
    const querier = {
      query: vi.fn<any>(() =>
        Promise.reject(
          new Error('trigger "txob_wakeup_trigger_events" already exists'),
        ),
      ),
    } as any;
    await expect(createWakeupTrigger({ querier })).resolves.toBeUndefined();
  });

  it("ignores already exists message when thrown value is not an Error", async () => {
    const querier = {
      query: vi.fn<any>(() =>
        Promise.reject("relation for trigger already exists"),
      ),
    } as any;
    await expect(createWakeupTrigger({ querier })).resolves.toBeUndefined();
  });

  it("ignores duplicate relation message that mentions relation but not trigger", async () => {
    const querier = {
      query: vi.fn<any>(() =>
        Promise.reject(
          new Error('relation "public"."events" already exists'),
        ),
      ),
    } as any;
    await expect(createWakeupTrigger({ querier })).resolves.toBeUndefined();
  });

  it("rethrows unexpected errors", async () => {
    const boom = new Error("connection refused");
    const querier = { query: vi.fn<any>(() => Promise.reject(boom)) } as any;
    await expect(createWakeupTrigger({ querier })).rejects.toBe(boom);
  });
});

describe("createWakeupEmitter", () => {
  it("LISTENs on lowercased channel and emits wakeup for matching notifications", async () => {
    const querier = { query: vi.fn<any>(() => Promise.resolve()) } as any;
    const emitter = await createWakeupEmitter({
      listenClientConfig: { connectionString: "postgres://localhost/test" },
      channel: "TxOb_Events",
      createTrigger: true,
      querier,
    });

    const client = pgMocks.listenClients[0];
    expect(client.connect).toHaveBeenCalledOnce();
    expect(client.query).toHaveBeenCalledWith('LISTEN "txob_events"');

    const wakeups: number[] = [];
    emitter.on("wakeup", () => wakeups.push(1));

    client.emit("notification", { channel: "txob_events" });
    client.emit("notification", { channel: "other" });

    expect(wakeups).toHaveLength(1);

    const extended = emitter as typeof emitter & { close: () => Promise<void> };
    await extended.close();
    expect(client.query).toHaveBeenCalledWith('UNLISTEN "txob_events"');
    expect(client.end).toHaveBeenCalledOnce();

    expect(querier.query).toHaveBeenCalled();
  });

  it("emits error when LISTEN connection ends", async () => {
    const emitter = await createWakeupEmitter({
      listenClientConfig: {},
      createTrigger: false,
    });
    const client = pgMocks.listenClients[0];
    const errs: unknown[] = [];
    (emitter as unknown as EventEmitter).on("error", (e: unknown) =>
      errs.push(e),
    );
    client.emit("end");
    expect(errs[0]).toBeInstanceOf(Error);
    expect((errs[0] as Error).message).toBe(
      "Postgres LISTEN connection ended",
    );
  });

  it("forwards listen client errors", async () => {
    const emitter = await createWakeupEmitter({
      listenClientConfig: {},
      createTrigger: false,
    });
    const client = pgMocks.listenClients[0];
    const errs: unknown[] = [];
    (emitter as unknown as EventEmitter).on("error", (e: unknown) =>
      errs.push(e),
    );
    const err = new Error("econnreset");
    client.emit("error", err);
    expect(errs).toEqual([err]);
  });

  it("allows unregistering wakeup listener via off", async () => {
    const emitter = await createWakeupEmitter({
      listenClientConfig: {},
      createTrigger: false,
    });
    const fn = () => {};
    emitter.on("wakeup", fn);
    emitter.off("wakeup", fn);
  });
});
