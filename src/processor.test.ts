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
    mockTxClient.updateEvent.mockImplementation(() => {
      return Promise.resolve();
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

    expect(mockTxClient.updateEvent).toHaveBeenCalledTimes(1);
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
      onEventMaxErrorsReached: vi.fn(),
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

    // Should call onEventMaxErrorsReached since all remaining handlers are unprocessable
    expect(opts.onEventMaxErrorsReached).toHaveBeenCalledOnce();
    expect(opts.onEventMaxErrorsReached).toHaveBeenCalledWith({
      event: expect.objectContaining({
        id: "1",
        errors: opts.maxErrors,
      }),
      txClient: mockTxClient,
      signal: undefined,
    });

    expect(mockTxClient.updateEvent).toHaveBeenCalledTimes(1);
    expect(mockTxClient.updateEvent).toHaveBeenCalledWith({
      backoff_until: null,
      correlation_id: "abc123",
      data: {},
      errors: opts.maxErrors,
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

  it("sets errors to maxErrors when all handlers are unprocessable and calls onEventMaxErrorsReached", async () => {
    const opts = {
      maxErrors: 5,
      backoff: vi.fn(),
      onEventMaxErrorsReached: vi.fn(),
    };
    const errUnprocessable1 = new ErrorUnprocessableEventHandler(
      new Error("err1"),
    );
    const errUnprocessable2 = new ErrorUnprocessableEventHandler(
      new Error("err2"),
    );
    const handlerMap = {
      evtType1: {
        handler1: vi.fn(() => Promise.reject(errUnprocessable1)),
        handler2: vi.fn(() => Promise.reject(errUnprocessable2)),
      },
    };
    const evt1: TxOBEvent<keyof typeof handlerMap> = {
      type: "evtType1",
      id: "1",
      timestamp: now,
      data: {},
      correlation_id: "abc123",
      handler_results: {},
      errors: 0,
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
    expect(mockClient.transaction).toHaveBeenCalledTimes(1);
    expect(handlerMap.evtType1.handler1).toHaveBeenCalledOnce();
    expect(handlerMap.evtType1.handler2).toHaveBeenCalledOnce();

    // Backoff is called even when reaching maxErrors (then backoff_until is nulled)
    expect(opts.backoff).toHaveBeenCalledOnce();
    expect(opts.backoff).toHaveBeenCalledWith(opts.maxErrors);

    // Should call the maxErrors callback
    expect(opts.onEventMaxErrorsReached).toHaveBeenCalledOnce();
    expect(opts.onEventMaxErrorsReached).toHaveBeenCalledWith({
      event: expect.objectContaining({
        id: "1",
        errors: opts.maxErrors,
      }),
      txClient: mockTxClient,
      signal: undefined,
    });

    expect(mockTxClient.updateEvent).toHaveBeenCalledWith({
      backoff_until: null,
      correlation_id: "abc123",
      data: {},
      errors: opts.maxErrors,
      handler_results: {
        handler1: {
          unprocessable_at: now,
          errors: [
            {
              error: errUnprocessable1.message,
              timestamp: now,
            },
          ],
        },
        handler2: {
          unprocessable_at: now,
          errors: [
            {
              error: errUnprocessable2.message,
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

  it("sets errors to maxErrors when some handlers succeed and remaining are unprocessable", async () => {
    const opts = {
      maxErrors: 5,
      backoff: vi.fn(),
      onEventMaxErrorsReached: vi.fn(),
    };
    const errUnprocessable = new ErrorUnprocessableEventHandler(
      new Error("err1"),
    );
    const handlerMap = {
      evtType1: {
        handler1: vi.fn(() => Promise.resolve()),
        handler2: vi.fn(() => Promise.reject(errUnprocessable)),
      },
    };
    const evt1: TxOBEvent<keyof typeof handlerMap> = {
      type: "evtType1",
      id: "1",
      timestamp: now,
      data: {},
      correlation_id: "abc123",
      handler_results: {},
      errors: 0,
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
    expect(mockClient.transaction).toHaveBeenCalledTimes(1);
    expect(handlerMap.evtType1.handler1).toHaveBeenCalledOnce();
    expect(handlerMap.evtType1.handler2).toHaveBeenCalledOnce();

    // Should call backoff even when jumping to maxErrors
    expect(opts.backoff).toHaveBeenCalledWith(opts.maxErrors);

    // Should call the maxErrors callback since all remaining handlers are unprocessable
    expect(opts.onEventMaxErrorsReached).toHaveBeenCalledOnce();

    expect(mockTxClient.updateEvent).toHaveBeenCalledWith({
      backoff_until: null,
      correlation_id: "abc123",
      data: {},
      errors: opts.maxErrors,
      handler_results: {
        handler1: {
          errors: [],
          processed_at: now,
        },
        handler2: {
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

  it("does not set errors to maxErrors when remaining handlers have retryable errors", async () => {
    const opts = {
      maxErrors: 5,
      backoff: vi.fn(() => now),
      onEventMaxErrorsReached: vi.fn(),
    };
    const retryableError = new Error("temporary failure");
    const errUnprocessable = new ErrorUnprocessableEventHandler(
      new Error("err1"),
    );
    const handlerMap = {
      evtType1: {
        handler1: vi.fn(() => Promise.resolve()),
        handler2: vi.fn(() => Promise.reject(errUnprocessable)),
        handler3: vi.fn(() => Promise.reject(retryableError)),
      },
    };
    const evt1: TxOBEvent<keyof typeof handlerMap> = {
      type: "evtType1",
      id: "1",
      timestamp: now,
      data: {},
      correlation_id: "abc123",
      handler_results: {},
      errors: 0,
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
    expect(mockClient.transaction).toHaveBeenCalledTimes(1);
    expect(handlerMap.evtType1.handler1).toHaveBeenCalledOnce();
    expect(handlerMap.evtType1.handler2).toHaveBeenCalledOnce();
    expect(handlerMap.evtType1.handler3).toHaveBeenCalledOnce();

    // Should call backoff normally since handler3 has retryable error
    expect(opts.backoff).toHaveBeenCalledWith(1);

    // Should NOT call the maxErrors callback since errors < maxErrors
    expect(opts.onEventMaxErrorsReached).not.toHaveBeenCalled();

    expect(mockTxClient.updateEvent).toHaveBeenCalledWith({
      backoff_until: expect.any(Date),
      correlation_id: "abc123",
      data: {},
      errors: 1,
      handler_results: {
        handler1: {
          errors: [],
          processed_at: now,
        },
        handler2: {
          unprocessable_at: now,
          errors: [
            {
              error: errUnprocessable.message,
              timestamp: now,
            },
          ],
        },
        handler3: {
          errors: [
            {
              error: retryableError.message,
              timestamp: now,
            },
          ],
        },
      },
      id: "1",
      timestamp: now,
      type: "evtType1",
    });
  });

  it("should throw and log when onEventMaxErrorsReached hook throws an error", async () => {
    const hookError = new Error("hook error");
    const logger = {
      debug: vi.fn(),
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
    };
    const opts = {
      maxErrors: 5,
      backoff: vi.fn(),
      onEventMaxErrorsReached: vi.fn(() => {
        throw hookError;
      }),
      logger,
    };
    const errUnprocessable = new ErrorUnprocessableEventHandler(
      new Error("err1"),
    );
    const handlerMap = {
      evtType1: {
        handler1: vi.fn(() => Promise.reject(errUnprocessable)),
      },
    };
    const evt1: TxOBEvent<keyof typeof handlerMap> = {
      type: "evtType1",
      id: "1",
      timestamp: now,
      data: {},
      correlation_id: "abc123",
      handler_results: {},
      errors: 0,
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

    expect(opts.onEventMaxErrorsReached).toHaveBeenCalledOnce();
    expect(logger.error).toHaveBeenCalledWith(
      {
        eventId: "1",
        error: hookError,
      },
      "error in onEventMaxErrorsReached hook",
    );
    expect(logger.error).toHaveBeenCalledWith(
      {
        eventId: "1",
        error: hookError,
      },
      "error processing event",
    );
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

  it("should cap backoff at maxDelayMs for large error counts", () => {
    const maxDelayMs = 1000 * 60; // 60 seconds
    const backoff = defaultBackoff(20); // Large error count that would exceed max
    const actual = backoff.getTime();
    const expected = Date.now() + maxDelayMs;
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
  it("should warn when stopping a processor that is not started", async () => {
    const logger = {
      debug: vi.fn(),
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
    };
    const processor = Processor(() => sleep(1), { sleepTimeMs: 0, logger });

    await processor.stop();

    expect(logger.warn).toHaveBeenCalledWith(
      "cannot stop processor from 'stopped'",
    );
  });
  it("should handle shutdown when processor completes before stop is called", async () => {
    let resolve: (() => void) | null = null;
    const promise = new Promise<void>((r) => {
      resolve = r;
    });

    const processor = Processor(
      () => {
        return promise;
      },
      { sleepTimeMs: 0 },
    );
    processor.start();

    // Complete the processor's work
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    resolve!();

    // Wait a bit for the processor to emit shutdownComplete
    await sleep(10);

    // Now stop should handle the already-completed case
    await processor.stop();
  });
  it("should warn when starting a processor that is already started", () => {
    const logger = {
      debug: vi.fn(),
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
    };
    const processor = Processor(() => sleep(1000), { sleepTimeMs: 0, logger });

    processor.start();
    processor.start(); // Try to start again

    expect(logger.warn).toHaveBeenCalledWith(
      "cannot start processor from 'started'",
    );
  });
  it("should handle non-abort errors and continue processing", async () => {
    let calls = 0;
    const logger = {
      debug: vi.fn(),
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
    };
    const error = new Error("processing error");

    const processor = Processor(
      () => {
        calls++;
        if (calls === 1) {
          throw error;
        }
        return Promise.resolve();
      },
      { sleepTimeMs: 0, logger },
    );
    processor.start();

    // Wait for the error to be logged and processing to continue
    await sleep(1100);

    await processor.stop();

    expect(logger.error).toHaveBeenCalledWith(error);
    expect(calls).toBeGreaterThan(1); // Should continue processing after error
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
