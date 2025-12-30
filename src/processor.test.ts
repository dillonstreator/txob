import { describe, it, expect, vi, afterEach } from "vitest";
import { EventProcessor, TxOBEvent, defaultBackoff } from "./processor.js";
import { TxobError, ErrorUnprocessableEventHandler } from "./error.js";
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

describe("EventProcessor - processEvents", () => {
  it("does nothing when no events to process", async () => {
    const opts = {
      maxErrors: 5,
      backoff: () => now,
      pollingIntervalMs: 10,
    };
    const handlerMap = {};
    mockClient.getEventsToProcess.mockImplementation(() => Promise.resolve([]));
    const processor = new EventProcessor({
      client: mockClient,
      handlerMap,
      ...opts,
    });
    processor.start();
    await sleep(50); // Wait for polling
    await processor.stop();

    expect(mockClient.getEventsToProcess).toHaveBeenCalled();
    expect(mockClient.transaction).not.toHaveBeenCalled();
    expect(mockTxClient.getEventByIdForUpdateSkipLocked).not.toHaveBeenCalled();
    expect(mockTxClient.updateEvent).not.toHaveBeenCalled();
  });

  it("handles handler results and updates", async () => {
    const opts = {
      maxErrors: 5,
      backoff: vi.fn(),
      pollingIntervalMs: 10,
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
    let callCount = 0;
    mockClient.getEventsToProcess.mockImplementation(() => {
      callCount++;
      // Return events only on first call, then empty array
      return Promise.resolve(callCount === 1 ? events : []);
    });
    mockTxClient.getEventByIdForUpdateSkipLocked.mockImplementation((id) => {
      if (id === evt3.id) return Promise.resolve(null);

      return Promise.resolve(events.find((e) => e.id === id) ?? null);
    });
    mockTxClient.updateEvent.mockImplementation(() => {
      return Promise.resolve();
    });

    const processor = new EventProcessor({
      client: mockClient,
      handlerMap,
      ...opts,
    });
    processor.start();
    await sleep(50); // Wait for processing
    await processor.stop();

    expect(mockClient.getEventsToProcess).toHaveBeenCalled();

    expect(mockClient.transaction).toHaveBeenCalledTimes(3);

    expect(handlerMap.evtType1.handler1).toHaveBeenCalledOnce();
    expect(handlerMap.evtType1.handler1).toHaveBeenCalledWith(evt1, {
      signal: expect.any(AbortSignal),
    });
    expect(handlerMap.evtType1.handler2).toHaveBeenCalledOnce();
    expect(handlerMap.evtType1.handler2).toHaveBeenCalledWith(evt1, {
      signal: expect.any(AbortSignal),
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
      pollingIntervalMs: 10,
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
    let callCount = 0;
    mockClient.getEventsToProcess.mockImplementation(() => {
      callCount++;
      return Promise.resolve(callCount === 1 ? events : []);
    });
    mockTxClient.getEventByIdForUpdateSkipLocked.mockImplementation((id) => {
      return Promise.resolve(events.find((e) => e.id === id) ?? null);
    });
    mockTxClient.updateEvent.mockImplementation(() => {
      return Promise.resolve();
    });

    const processor = new EventProcessor({
      client: mockClient,
      handlerMap,
      ...opts,
    });
    processor.start();
    await sleep(50); // Wait for processing
    await processor.stop();

    expect(mockClient.getEventsToProcess).toHaveBeenCalled();

    expect(mockClient.transaction).toHaveBeenCalledTimes(1);

    expect(handlerMap.evtType1.handler1).toHaveBeenCalledOnce();
    expect(handlerMap.evtType1.handler1).toHaveBeenCalledWith(evt1, {
      signal: expect.any(AbortSignal),
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
      pollingIntervalMs: 10,
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
    let callCount = 0;
    mockClient.getEventsToProcess.mockImplementation(() => {
      callCount++;
      return Promise.resolve(callCount === 1 ? events : []);
    });
    mockTxClient.getEventByIdForUpdateSkipLocked.mockImplementation((id) => {
      return Promise.resolve(events.find((e) => e.id === id) ?? null);
    });
    mockTxClient.updateEvent.mockImplementation(() => {
      return Promise.resolve();
    });

    const processor = new EventProcessor({
      client: mockClient,
      handlerMap,
      ...opts,
    });
    processor.start();
    await sleep(50); // Wait for processing
    await processor.stop();

    expect(mockClient.getEventsToProcess).toHaveBeenCalled();

    expect(mockClient.transaction).toHaveBeenCalledTimes(1);

    expect(handlerMap.evtType1.handler1).toHaveBeenCalledOnce();
    expect(handlerMap.evtType1.handler1).toHaveBeenCalledWith(evt1, {
      signal: expect.any(AbortSignal),
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
      signal: expect.any(AbortSignal),
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
      pollingIntervalMs: 10,
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
    let callCount = 0;
    mockClient.getEventsToProcess.mockImplementation(() => {
      callCount++;
      return Promise.resolve(callCount === 1 ? events : []);
    });
    mockTxClient.getEventByIdForUpdateSkipLocked.mockImplementation((id) => {
      return Promise.resolve(events.find((e) => e.id === id) ?? null);
    });
    mockTxClient.updateEvent.mockImplementation(() => {
      return Promise.resolve();
    });

    const processor = new EventProcessor({
      client: mockClient,
      handlerMap,
      ...opts,
    });
    processor.start();
    await sleep(50); // Wait for processing
    await processor.stop();

    expect(mockClient.getEventsToProcess).toHaveBeenCalled();
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
      signal: expect.any(AbortSignal),
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
      pollingIntervalMs: 10,
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
    let callCount = 0;
    mockClient.getEventsToProcess.mockImplementation(() => {
      callCount++;
      return Promise.resolve(callCount === 1 ? events : []);
    });
    mockTxClient.getEventByIdForUpdateSkipLocked.mockImplementation((id) => {
      return Promise.resolve(events.find((e) => e.id === id) ?? null);
    });
    mockTxClient.updateEvent.mockImplementation(() => {
      return Promise.resolve();
    });

    const processor = new EventProcessor({
      client: mockClient,
      handlerMap,
      ...opts,
    });
    processor.start();
    await sleep(50); // Wait for processing
    await processor.stop();

    expect(mockClient.getEventsToProcess).toHaveBeenCalled();
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
      pollingIntervalMs: 10,
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
    let callCount = 0;
    mockClient.getEventsToProcess.mockImplementation(() => {
      callCount++;
      return Promise.resolve(callCount === 1 ? events : []);
    });
    mockTxClient.getEventByIdForUpdateSkipLocked.mockImplementation((id) => {
      return Promise.resolve(events.find((e) => e.id === id) ?? null);
    });
    mockTxClient.updateEvent.mockImplementation(() => {
      return Promise.resolve();
    });

    const processor = new EventProcessor({
      client: mockClient,
      handlerMap,
      ...opts,
    });
    processor.start();
    await sleep(50); // Wait for processing
    await processor.stop();

    expect(mockClient.getEventsToProcess).toHaveBeenCalled();
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
      pollingIntervalMs: 10,
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
    let callCount = 0;
    mockClient.getEventsToProcess.mockImplementation(() => {
      callCount++;
      return Promise.resolve(callCount === 1 ? events : []);
    });
    mockTxClient.getEventByIdForUpdateSkipLocked.mockImplementation((id) => {
      return Promise.resolve(events.find((e) => e.id === id) ?? null);
    });
    mockTxClient.updateEvent.mockImplementation(() => {
      return Promise.resolve();
    });

    const processor = new EventProcessor({
      client: mockClient,
      handlerMap,
      ...opts,
    });
    processor.start();
    await sleep(50); // Wait for processing
    await processor.stop();

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

  it("should use the latest backoff when multiple TxobErrors have different backoffUntil dates", async () => {
    const opts = {
      maxErrors: 5,
      backoff: vi.fn(() => new Date(now.getTime() + 5000)), // Default backoff: 5 seconds
      pollingIntervalMs: 10,
    };

    // Create different backoff times
    const backoff1 = new Date(now.getTime() + 10000); // 10 seconds
    const backoff2 = new Date(now.getTime() + 20000); // 20 seconds (latest)
    const backoff3 = new Date(now.getTime() + 15000); // 15 seconds

    const error1 = new TxobError("error 1", { backoffUntil: backoff1 });
    const error2 = new TxobError("error 2", { backoffUntil: backoff2 });
    const error3 = new TxobError("error 3", { backoffUntil: backoff3 });

    const handlerMap = {
      evtType1: {
        handler1: vi.fn(() => Promise.reject(error1)),
        handler2: vi.fn(() => Promise.reject(error2)),
        handler3: vi.fn(() => Promise.reject(error3)),
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
    let callCount = 0;
    mockClient.getEventsToProcess.mockImplementation(() => {
      callCount++;
      return Promise.resolve(callCount === 1 ? events : []);
    });
    mockTxClient.getEventByIdForUpdateSkipLocked.mockImplementation((id) => {
      return Promise.resolve(events.find((e) => e.id === id) ?? null);
    });
    mockTxClient.updateEvent.mockImplementation(() => {
      return Promise.resolve();
    });

    const processor = new EventProcessor({
      client: mockClient,
      handlerMap,
      ...opts,
    });
    processor.start();
    await sleep(50); // Wait for processing
    await processor.stop();

    expect(mockClient.getEventsToProcess).toHaveBeenCalled();
    expect(mockClient.transaction).toHaveBeenCalledTimes(1);

    // All handlers should have been called
    expect(handlerMap.evtType1.handler1).toHaveBeenCalledOnce();
    expect(handlerMap.evtType1.handler2).toHaveBeenCalledOnce();
    expect(handlerMap.evtType1.handler3).toHaveBeenCalledOnce();

    // Default backoff should also be called
    expect(opts.backoff).toHaveBeenCalledWith(1);

    // The latest backoff (backoff2 = 20 seconds) should be used
    expect(mockTxClient.updateEvent).toHaveBeenCalledTimes(1);
    const updateCall = mockTxClient.updateEvent.mock.calls[0][0];
    expect(updateCall.backoff_until).toEqual(backoff2);
    expect(updateCall.errors).toBe(1);
  });

  it("should use the latest backoff when TxobError backoff is later than default backoff", async () => {
    const laterBackoff = new Date(now.getTime() + 30000); // 30 seconds
    const defaultBackoffTime = new Date(now.getTime() + 5000); // 5 seconds

    const opts = {
      maxErrors: 5,
      backoff: vi.fn(() => defaultBackoffTime),
      pollingIntervalMs: 10,
    };

    const error = new TxobError("error with backoff", {
      backoffUntil: laterBackoff,
    });

    const handlerMap = {
      evtType1: {
        handler1: vi.fn(() => Promise.reject(error)),
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
    let callCount = 0;
    mockClient.getEventsToProcess.mockImplementation(() => {
      callCount++;
      return Promise.resolve(callCount === 1 ? events : []);
    });
    mockTxClient.getEventByIdForUpdateSkipLocked.mockImplementation((id) => {
      return Promise.resolve(events.find((e) => e.id === id) ?? null);
    });
    mockTxClient.updateEvent.mockImplementation(() => {
      return Promise.resolve();
    });

    const processor = new EventProcessor({
      client: mockClient,
      handlerMap,
      ...opts,
    });
    processor.start();
    await sleep(50); // Wait for processing
    await processor.stop();

    expect(mockClient.transaction).toHaveBeenCalledTimes(1);
    expect(opts.backoff).toHaveBeenCalledWith(1);

    // The latest backoff (laterBackoff = 30 seconds) should be used, not the default (5 seconds)
    const updateCall = mockTxClient.updateEvent.mock.calls[0][0];
    expect(updateCall.backoff_until).toEqual(laterBackoff);
    expect(updateCall.backoff_until?.getTime()).toBeGreaterThan(
      defaultBackoffTime.getTime(),
    );
  });

  it("should use default backoff when TxobError backoff is earlier than default backoff", async () => {
    const earlierBackoff = new Date(now.getTime() + 2000); // 2 seconds
    const defaultBackoffTime = new Date(now.getTime() + 5000); // 5 seconds

    const opts = {
      maxErrors: 5,
      backoff: vi.fn(() => defaultBackoffTime),
      pollingIntervalMs: 10,
    };

    const error = new TxobError("error with backoff", {
      backoffUntil: earlierBackoff,
    });

    const handlerMap = {
      evtType1: {
        handler1: vi.fn(() => Promise.reject(error)),
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
    let callCount = 0;
    mockClient.getEventsToProcess.mockImplementation(() => {
      callCount++;
      return Promise.resolve(callCount === 1 ? events : []);
    });
    mockTxClient.getEventByIdForUpdateSkipLocked.mockImplementation((id) => {
      return Promise.resolve(events.find((e) => e.id === id) ?? null);
    });
    mockTxClient.updateEvent.mockImplementation(() => {
      return Promise.resolve();
    });

    const processor = new EventProcessor({
      client: mockClient,
      handlerMap,
      ...opts,
    });
    processor.start();
    await sleep(50); // Wait for processing
    await processor.stop();

    expect(mockClient.transaction).toHaveBeenCalledTimes(1);
    expect(opts.backoff).toHaveBeenCalledWith(1);

    // The latest backoff (defaultBackoffTime = 5 seconds) should be used, not the earlier one (2 seconds)
    const updateCall = mockTxClient.updateEvent.mock.calls[0][0];
    expect(updateCall.backoff_until).toEqual(defaultBackoffTime);
    expect(updateCall.backoff_until?.getTime()).toBeGreaterThan(
      earlierBackoff.getTime(),
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

describe("EventProcessor - lifecycle", () => {
  it("should shutdown gracefully", async () => {
    let calls = 0;
    let aborted = false;
    const handlerMap = {
      evtType1: {
        handler1: vi.fn((event, { signal }) => {
          calls++;
          return new Promise<void>((r) => {
            signal.addEventListener("abort", () => {
              aborted = true;
              r();
            });
          });
        }),
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
    let callCount = 0;
    mockClient.getEventsToProcess.mockImplementation((opts) => {
      // Check if signal is aborted - reject immediately to be caught by polling loop
      if (opts?.signal?.aborted) {
        return Promise.reject(new DOMException("Aborted", "AbortError"));
      }
      callCount++;
      return Promise.resolve(callCount === 1 ? [evt1] : []);
    });
    mockTxClient.getEventByIdForUpdateSkipLocked.mockImplementation(() =>
      Promise.resolve(evt1),
    );
    mockTxClient.updateEvent.mockImplementation(() => Promise.resolve());

    const processor = new EventProcessor({
      client: mockClient,
      handlerMap,
      pollingIntervalMs: 10,
    });
    processor.start();
    await sleep(20); // Let it start processing
    await processor.stop();
    // The stop() method now waits for the polling loop to complete,
    // ensuring any pending rejections are caught by the polling loop's catch block

    expect(calls).toBe(1);
    expect(aborted).toBe(true);
  });
  it("should respect shutdown timeout and throw", async () => {
    const handlerMap = {
      evtType1: {
        handler1: vi.fn(() => sleep(100)),
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
    let callCount = 0;
    mockClient.getEventsToProcess.mockImplementation(() => {
      callCount++;
      return Promise.resolve(callCount === 1 ? [evt1] : []);
    });
    mockTxClient.getEventByIdForUpdateSkipLocked.mockImplementation(() =>
      Promise.resolve(evt1),
    );
    mockTxClient.updateEvent.mockImplementation(() => Promise.resolve());

    const processor = new EventProcessor({
      client: mockClient,
      handlerMap,
      pollingIntervalMs: 10,
    });
    processor.start();

    const start = Date.now();
    try {
      await processor.stop({ timeoutMs: 10 });
    } catch (error: any) {
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
    const processor = new EventProcessor({
      client: mockClient,
      handlerMap: {},
      pollingIntervalMs: 10,
      logger,
    });

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

    const handlerMap = {
      evtType1: {
        handler1: vi.fn(() => promise),
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
    let callCount = 0;
    mockClient.getEventsToProcess.mockImplementation((opts) => {
      // Respect abort signal to prevent unhandled rejections
      if (opts?.signal?.aborted) {
        return Promise.reject(new DOMException("Aborted", "AbortError"));
      }
      callCount++;
      return Promise.resolve(callCount === 1 ? [evt1] : []);
    });
    mockTxClient.getEventByIdForUpdateSkipLocked.mockImplementation(() =>
      Promise.resolve(evt1),
    );
    mockTxClient.updateEvent.mockImplementation(() => Promise.resolve());

    const processor = new EventProcessor({
      client: mockClient,
      handlerMap,
      pollingIntervalMs: 10,
    });
    processor.start();

    // Complete the processor's work
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    resolve!();

    // Wait a bit for the processor to complete
    await sleep(20);

    // Now stop should handle the already-completed case
    await processor.stop();
    await sleep(10); // Let abort operations settle
  });
  it("should warn when starting a processor that is already started", () => {
    const logger = {
      debug: vi.fn(),
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
    };
    const processor = new EventProcessor({
      client: mockClient,
      handlerMap: {},
      pollingIntervalMs: 10,
      logger,
    });

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

    const handlerMap = {
      evtType1: {
        handler1: vi.fn(() => {
          calls++;
          if (calls === 1) {
            throw error;
          }
          return Promise.resolve();
        }),
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
    let callCount = 0;
    mockClient.getEventsToProcess.mockImplementation(() => {
      callCount++;
      return Promise.resolve(callCount === 1 ? [evt1] : []);
    });
    mockTxClient.getEventByIdForUpdateSkipLocked.mockImplementation(() =>
      Promise.resolve(evt1),
    );
    mockTxClient.updateEvent.mockImplementation(() => Promise.resolve());

    const processor = new EventProcessor({
      client: mockClient,
      handlerMap,
      pollingIntervalMs: 10,
      logger,
    });
    processor.start();

    // Wait for the error to be logged and processing to continue
    await sleep(50);

    await processor.stop();

    expect(logger.error).toHaveBeenCalled();
    expect(calls).toBeGreaterThan(0);
  });
});

describe("EventProcessor - basic", () => {
  it("should call into processEvents", async () => {
    const opts = {
      maxErrors: 5,
      backoff: () => now,
      pollingIntervalMs: 10,
    };
    const handlerMap = {};
    mockClient.getEventsToProcess.mockImplementation(() => Promise.resolve([]));
    const processor = new EventProcessor({
      client: mockClient,
      handlerMap,
      ...opts,
    });
    processor.start();

    await sleep(50); // Wait for polling
    await processor.stop();

    expect(mockClient.getEventsToProcess).toHaveBeenCalled();
    expect(mockClient.transaction).not.toHaveBeenCalled();
    expect(mockTxClient.getEventByIdForUpdateSkipLocked).not.toHaveBeenCalled();
    expect(mockTxClient.updateEvent).not.toHaveBeenCalled();
  });
});
