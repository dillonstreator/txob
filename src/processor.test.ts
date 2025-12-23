import { describe, it, expect, vi, afterEach } from "vitest";
import {
  EventProcessor,
  Processor,
  TxOBEvent,
  ErrorUnprocessableEventHandler,
  defaultBackoff,
  processEvents,
} from "./processor.js";
import { sleep } from "./sleep.js";

const mockTxClient = {
  getEventByIdForUpdateSkipLocked: vi.fn(),
  updateEvent: vi.fn(),
  createEvent: vi.fn(),
};
const mockClient = {
  getEventsToProcess: vi.fn(),
  transaction: vi.fn(async (fn) => fn(mockTxClient)),
};

const now = new Date();
vi.mock("./date", async (getOg) => {
  const mod = await getOg();
  return {
    ...(mod as Object),
    getDate: vi.fn(() => now),
  };
});

afterEach(() => {
  vi.clearAllMocks();
});

describe("processEvents", () => {
  it("does nothing when no events to process", () => {
    const opts = {
      maxErrors: 5,
      backoff: () => now,
    };
    const handlerMap = {};
    mockClient.getEventsToProcess.mockImplementation(() => []);
    processEvents(mockClient, handlerMap, opts);
    expect(mockClient.getEventsToProcess).toHaveBeenCalledOnce();
    expect(mockClient.getEventsToProcess).toHaveBeenCalledWith({
      maxErrors: opts.maxErrors,
      signal: undefined,
    });
    expect(mockClient.transaction).not.toHaveBeenCalled();
    expect(mockTxClient.getEventByIdForUpdateSkipLocked).not.toHaveBeenCalled();
    expect(mockTxClient.updateEvent).not.toHaveBeenCalled();
  });

  it("handles handler results and updates", async () => {
    const opts = {
      maxErrors: 5,
      backoff: vi.fn(),
      retry: {
        minTimeout: 50,
        maxTimeout: 100,
      },
    };
    const err = new Error("some error");
    const handlerMap = {
      evtType1: {
        handler1: vi.fn(() => Promise.resolve()),
        handler2: vi.fn(() => Promise.reject(err)),
        handler3: vi.fn(() => Promise.resolve()),
      },
      evtType2: {
        handler1: vi.fn(() => Promise.resolve()),
      },
    };
    const evt1: TxOBEvent<keyof typeof handlerMap> = {
      type: "evtType1",
      id: "1",
      timestamp: now,
      data: {},
      correlation_id: "abc123",
      handler_results: {
        handler1: {
          errors: [{ error: err.message, timestamp: now }],
        },
        handler3: {
          processed_at: now,
        },
      },
      errors: opts.maxErrors - 1,
    };
    // should skip evt2 due to max errors
    const evt2: TxOBEvent<keyof typeof handlerMap> = {
      type: "evtType1",
      id: "2",
      timestamp: now,
      data: {},
      correlation_id: "abc456",
      handler_results: {},
      errors: opts.maxErrors,
    };
    const evt3: TxOBEvent<keyof typeof handlerMap> = {
      type: "evtType1",
      id: "3",
      timestamp: now,
      data: {},
      correlation_id: "abc789",
      handler_results: {},
      errors: 0,
    };
    // skip processed
    const evt4: TxOBEvent<keyof typeof handlerMap> = {
      type: "evtType1",
      id: "4",
      timestamp: now,
      data: {},
      correlation_id: "xyz123",
      handler_results: {},
      errors: 0,
      processed_at: now,
    };
    const events = [evt1, evt2, evt3, evt4];
    mockClient.getEventsToProcess.mockImplementation(() => events);
    mockTxClient.getEventByIdForUpdateSkipLocked.mockImplementation((id) => {
      if (id === evt3.id) return null;

      return events.find((e) => e.id === id);
    });
    let updateEventCalls = 0;
    mockTxClient.updateEvent.mockImplementation(() => {
      updateEventCalls++;
      if (updateEventCalls <= 1) return Promise.reject("some error");
      else return Promise.resolve();
    });

    await processEvents(mockClient, handlerMap, opts);

    expect(mockClient.getEventsToProcess).toHaveBeenCalledOnce();
    expect(mockClient.getEventsToProcess).toHaveBeenCalledWith({
      maxErrors: opts.maxErrors,
      signal: undefined,
    });

    expect(mockClient.transaction).toHaveBeenCalledTimes(3);

    expect(handlerMap.evtType1.handler1).toHaveBeenCalledOnce();
    expect(handlerMap.evtType1.handler1).toHaveBeenCalledWith(evt1, {
      signal: undefined,
    });
    expect(handlerMap.evtType1.handler2).toHaveBeenCalledOnce();
    expect(handlerMap.evtType1.handler2).toHaveBeenCalledWith(evt1, {
      signal: undefined,
    });
    expect(handlerMap.evtType1.handler3).not.toHaveBeenCalled();

    expect(mockTxClient.getEventByIdForUpdateSkipLocked).toHaveBeenCalledTimes(
      3,
    );

    expect(opts.backoff).toHaveBeenCalledOnce();
    expect(opts.backoff).toHaveBeenCalledWith(5); // evt.errors + 1

    expect(mockTxClient.updateEvent).toHaveBeenCalledTimes(2);
    expect(mockTxClient.updateEvent).toHaveBeenCalledWith({
      processed_at: now,
      backoff_until: null,
      correlation_id: "abc123",
      data: {},
      errors: 5,
      handler_results: {
        handler1: {
          errors: [
            {
              error: err.message,
              timestamp: now,
            },
          ],
          processed_at: now,
        },
        handler2: {
          errors: [
            {
              error: err.message,
              timestamp: now,
            },
          ],
        },
        handler3: {
          processed_at: now,
        },
      },
      id: "1",
      timestamp: now,
      type: "evtType1",
    });
  });

  it("process event if no handler errors encountered", async () => {
    const opts = {
      maxErrors: 5,
      backoff: vi.fn(),
    };
    const err = new Error("some error");
    const handlerMap = {
      evtType1: {
        handler1: vi.fn(() => Promise.resolve()),
        handler2: vi.fn(() => Promise.resolve()),
      },
    };
    const evt1: TxOBEvent<keyof typeof handlerMap> = {
      type: "evtType1",
      id: "1",
      timestamp: now,
      data: {},
      correlation_id: "abc123",
      handler_results: {
        handler1: {
          errors: [{ error: err.message, timestamp: now }],
        },
        handler2: {
          processed_at: now,
        },
      },
      errors: 1,
    };
    const events = [evt1];
    mockClient.getEventsToProcess.mockImplementation(() => events);
    mockTxClient.getEventByIdForUpdateSkipLocked.mockImplementation((id) => {
      return events.find((e) => e.id === id);
    });
    mockTxClient.updateEvent.mockImplementation(() => {
      return Promise.resolve();
    });

    await processEvents(mockClient, handlerMap, opts);

    expect(mockClient.getEventsToProcess).toHaveBeenCalledOnce();
    expect(mockClient.getEventsToProcess).toHaveBeenCalledWith({
      maxErrors: opts.maxErrors,
      signal: undefined,
    });

    expect(mockClient.transaction).toHaveBeenCalledTimes(1);

    expect(handlerMap.evtType1.handler1).toHaveBeenCalledOnce();
    expect(handlerMap.evtType1.handler1).toHaveBeenCalledWith(evt1, {
      signal: undefined,
    });
    expect(mockTxClient.getEventByIdForUpdateSkipLocked).toHaveBeenCalledTimes(
      1,
    );

    expect(mockTxClient.updateEvent).toHaveBeenCalledTimes(1);
    expect(mockTxClient.updateEvent).toHaveBeenCalledWith({
      backoff_until: null,
      correlation_id: "abc123",
      data: {},
      errors: 1,
      handler_results: {
        handler1: {
          errors: [
            {
              error: err.message,
              timestamp: now,
            },
          ],
          processed_at: now,
        },
        handler2: {
          processed_at: now,
        },
      },
      id: "1",
      timestamp: now,
      type: "evtType1",
      processed_at: now,
    });
  });

  it("respects ErrorUnprocessableEventHandler sentinel error to stop handler processing", async () => {
    const opts = {
      maxErrors: 5,
      backoff: vi.fn(),
    };
    const err = new Error("some error");
    const errUnprocessable = new ErrorUnprocessableEventHandler(
      new Error("err1"),
    );
    const handlerMap = {
      evtType1: {
        handler1: vi.fn(() => Promise.reject(errUnprocessable)),
        handler2: vi.fn(() => Promise.reject(errUnprocessable)),
      },
    };
    const evt1: TxOBEvent<keyof typeof handlerMap> = {
      type: "evtType1",
      id: "1",
      timestamp: now,
      data: {},
      correlation_id: "abc123",
      handler_results: {
        handler1: {
          errors: [{ error: err.message, timestamp: now }],
        },
        handler2: {
          processed_at: now,
        },
        handler3: {
          unprocessable_at: now,
          errors: [
            {
              error: errUnprocessable.message,
              timestamp: now,
            },
          ],
        },
      },
      errors: 1,
    };
    const events = [evt1];
    mockClient.getEventsToProcess.mockImplementation(() => events);
    mockTxClient.getEventByIdForUpdateSkipLocked.mockImplementation((id) => {
      return events.find((e) => e.id === id);
    });
    mockTxClient.updateEvent.mockImplementation(() => {
      return Promise.resolve();
    });

    await processEvents(mockClient, handlerMap, opts);

    expect(mockClient.getEventsToProcess).toHaveBeenCalledOnce();
    expect(mockClient.getEventsToProcess).toHaveBeenCalledWith({
      maxErrors: opts.maxErrors,
      signal: undefined,
    });

    expect(mockClient.transaction).toHaveBeenCalledTimes(1);

    expect(handlerMap.evtType1.handler1).toHaveBeenCalledOnce();
    expect(handlerMap.evtType1.handler1).toHaveBeenCalledWith(evt1, {
      signal: undefined,
    });
    expect(mockTxClient.getEventByIdForUpdateSkipLocked).toHaveBeenCalledTimes(
      1,
    );

    expect(mockTxClient.updateEvent).toHaveBeenCalledTimes(1);
    expect(mockTxClient.updateEvent).toHaveBeenCalledWith({
      backoff_until: null,
      correlation_id: "abc123",
      data: {},
      errors: 1,
      handler_results: {
        handler1: {
          unprocessable_at: now,
          errors: [
            { error: err.message, timestamp: now },
            { error: errUnprocessable.message, timestamp: now },
          ],
        },
        handler2: {
          processed_at: now,
        },
        handler3: {
          unprocessable_at: now,
          errors: [
            {
              error: errUnprocessable.message,
              timestamp: now,
            },
          ],
        },
      },
      id: "1",
      timestamp: now,
      type: "evtType1",
      processed_at: now,
    });
  });
});

describe("defaultBackoff", () => {
  it("should calculate a backoff", () => {
    const backoff = defaultBackoff(3);
    const actual = backoff.getTime();
    const expected = Date.now() + 1000 * 2 ** 3;
    const diff = Math.abs(actual - expected);

    expect(diff).lessThanOrEqual(1);
  });
});

describe("Processor", () => {
  it("should shutdown gracefully", async () => {
    let calls = 0;
    let aborted = false;
    const processor = Processor(
      ({ signal }) => {
        calls++;
        return new Promise((r) => {
          signal.addEventListener("abort", () => {
            aborted = true;
            r();
          });
        });
      },
      { sleepTimeMs: 0 },
    );
    processor.start();

    await processor.stop();

    expect(calls).toBe(1);
    expect(aborted).toBe(true);
  });
  it("should respect shutdown timeout and throw", async () => {
    const processor = Processor(() => sleep(100), { sleepTimeMs: 0 });
    processor.start();

    const start = Date.now();
    try {
      await processor.stop({ timeoutMs: 10 });
    } catch (error) {
      expect(error.message).toBe("shutdown timeout 10ms elapsed");
    }
    const diff = Date.now() - start;
    expect(diff).toBeLessThan(50);
  });
});

describe("EventProcessor", () => {
  it("should call into processEvents", async () => {
    const opts = {
      maxErrors: 5,
      backoff: () => now,
    };
    const handlerMap = {};
    mockClient.getEventsToProcess.mockImplementation(() => []);
    const processor = EventProcessor(mockClient, handlerMap, opts);
    processor.start();

    await processor.stop();

    expect(mockClient.getEventsToProcess).toHaveBeenCalledOnce();
    expect(mockClient.transaction).not.toHaveBeenCalled();
    expect(mockTxClient.getEventByIdForUpdateSkipLocked).not.toHaveBeenCalled();
    expect(mockTxClient.updateEvent).not.toHaveBeenCalled();
  });
});
