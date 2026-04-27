export type TxOBTelemetryAttributeValue =
  | string
  | number
  | boolean
  | string[]
  | number[]
  | boolean[];
export type TxOBTelemetryAttributes = Record<
  string,
  TxOBTelemetryAttributeValue
>;

export interface TxOBTelemetrySpan {
  setAttribute?(key: string, value: TxOBTelemetryAttributeValue): void;
  setAttributes?(attributes: TxOBTelemetryAttributes): void;
  recordException?(exception: unknown): void;
  setStatus?(status: { code: 1 | 2; message?: string }): void;
  end(): void;
}

export interface TxOBTelemetryTracer {
  startSpan(
    name: string,
    options?: { attributes?: TxOBTelemetryAttributes },
  ): TxOBTelemetrySpan;
}

export interface TxOBTelemetryCounter {
  add(value: number, attributes?: TxOBTelemetryAttributes): void;
}

export interface TxOBTelemetryHistogram {
  record(value: number, attributes?: TxOBTelemetryAttributes): void;
}

export interface TxOBTelemetryMeter {
  createCounter(
    name: string,
    options?: { description?: string; unit?: string },
  ): TxOBTelemetryCounter;
  createHistogram(
    name: string,
    options?: { description?: string; unit?: string },
  ): TxOBTelemetryHistogram;
}

export type TxOBTelemetry = {
  tracer?: TxOBTelemetryTracer;
  meter?: TxOBTelemetryMeter;
  attributes?: TxOBTelemetryAttributes;
};

export type TxOBTelemetryInstruments = {
  tracer?: TxOBTelemetryTracer;
  attributes?: TxOBTelemetryAttributes;
  eventCounter?: TxOBTelemetryCounter;
  eventDuration?: TxOBTelemetryHistogram;
  handlerCounter?: TxOBTelemetryCounter;
  handlerDuration?: TxOBTelemetryHistogram;
  pollCounter?: TxOBTelemetryCounter;
  pollDuration?: TxOBTelemetryHistogram;
};

export const TxOBTelemetrySpanName = {
  Poll: "txob.poll",
  EventProcess: "txob.event.process",
  HandlerProcess: "txob.handler.process",
} as const;

export const TxOBTelemetryMetricName = {
  PollCount: "txob.poll.count",
  PollDuration: "txob.poll.duration",
  EventProcessingCount: "txob.event.processing.count",
  EventProcessingDuration: "txob.event.processing.duration",
  HandlerProcessingCount: "txob.handler.processing.count",
  HandlerProcessingDuration: "txob.handler.processing.duration",
} as const;

export const TxOBTelemetryAttributeKey = {
  EventId: "txob.event.id",
  EventType: "txob.event.type",
  EventCorrelationId: "txob.event.correlation_id",
  EventErrors: "txob.event.errors",
  EventOutcome: "txob.event.outcome",
  HandlerName: "txob.handler.name",
  HandlerOutcome: "txob.handler.outcome",
  PollOutcome: "txob.poll.outcome",
  QueueSize: "txob.queue.size",
  QueueMaxSize: "txob.queue.max_size",
  EventsFound: "txob.events.found",
  EventsQueued: "txob.events.queued",
} as const;

export const TxOBTelemetryEventOutcome = {
  Success: "success",
  Error: "error",
  MaxErrors: "max_errors",
  SkippedLocked: "skipped.locked",
  SkippedProcessed: "skipped.processed",
  SkippedMaxErrors: "skipped.max_errors",
} as const;

export const TxOBTelemetryHandlerOutcome = {
  Success: "success",
  Error: "error",
  Unprocessable: "unprocessable",
  SkippedProcessed: "skipped.processed",
  SkippedUnprocessable: "skipped.unprocessable",
} as const;

export const TxOBTelemetryPollOutcome = {
  Success: "success",
  Error: "error",
  SkippedAlreadyPolling: "skipped.already_polling",
  SkippedQueueFull: "skipped.queue_full",
} as const;

export type TxOBTelemetryEventOutcome =
  (typeof TxOBTelemetryEventOutcome)[keyof typeof TxOBTelemetryEventOutcome];
export type TxOBTelemetryHandlerOutcome =
  (typeof TxOBTelemetryHandlerOutcome)[keyof typeof TxOBTelemetryHandlerOutcome];
export type TxOBTelemetryPollOutcome =
  (typeof TxOBTelemetryPollOutcome)[keyof typeof TxOBTelemetryPollOutcome];

const telemetryStatusCode = {
  OK: 1,
  ERROR: 2,
} as const;

const suppressTelemetryRuntimeError = <T>(fn: () => T, fallback: T): T => {
  try {
    return fn();
  } catch {
    // Runtime telemetry is best-effort: once processing is underway, a broken
    // tracer/exporter must not change event processing behavior.
    return fallback;
  }
};

export const createTelemetryInstruments = (
  telemetry?: TxOBTelemetry,
): TxOBTelemetryInstruments => {
  // Metric instrument creation happens during processor construction. Surface
  // failures here so callers can catch configuration or SDK setup issues early.
  return {
    tracer: telemetry?.tracer,
    attributes: telemetry?.attributes,
    eventCounter: telemetry?.meter?.createCounter(
      TxOBTelemetryMetricName.EventProcessingCount,
      {
        description: "Number of outbox event processing attempts by outcome.",
        unit: "{event}",
      },
    ),
    eventDuration: telemetry?.meter?.createHistogram(
      TxOBTelemetryMetricName.EventProcessingDuration,
      {
        description: "Duration of outbox event processing.",
        unit: "ms",
      },
    ),
    handlerCounter: telemetry?.meter?.createCounter(
      TxOBTelemetryMetricName.HandlerProcessingCount,
      {
        description: "Number of outbox handler processing attempts by outcome.",
        unit: "{handler}",
      },
    ),
    handlerDuration: telemetry?.meter?.createHistogram(
      TxOBTelemetryMetricName.HandlerProcessingDuration,
      {
        description: "Duration of outbox event handler execution.",
        unit: "ms",
      },
    ),
    pollCounter: telemetry?.meter?.createCounter(
      TxOBTelemetryMetricName.PollCount,
      {
        description: "Number of outbox polling attempts by outcome.",
        unit: "{poll}",
      },
    ),
    pollDuration: telemetry?.meter?.createHistogram(
      TxOBTelemetryMetricName.PollDuration,
      {
        description: "Duration of outbox polling attempts.",
        unit: "ms",
      },
    ),
  };
};

const mergeTelemetryAttributes = (
  telemetry: TxOBTelemetryInstruments | undefined,
  attributes: TxOBTelemetryAttributes = {},
): TxOBTelemetryAttributes => ({
  ...(telemetry?.attributes ?? {}),
  ...attributes,
});

export const startTelemetrySpan = (
  telemetry: TxOBTelemetryInstruments | undefined,
  name: string,
  attributes?: TxOBTelemetryAttributes,
): TxOBTelemetrySpan | undefined =>
  suppressTelemetryRuntimeError(
    () =>
      telemetry?.tracer?.startSpan(name, {
        attributes: mergeTelemetryAttributes(telemetry, attributes),
      }),
    undefined,
  );

export const setTelemetrySpanAttributes = (
  span: TxOBTelemetrySpan | undefined,
  attributes: TxOBTelemetryAttributes,
): void => {
  suppressTelemetryRuntimeError(() => {
    span?.setAttributes?.(attributes);
  }, undefined);
};

export const endTelemetrySpan = (
  span: TxOBTelemetrySpan | undefined,
  error?: unknown,
): void => {
  suppressTelemetryRuntimeError(() => {
    if (error) {
      span?.recordException?.(error);
      span?.setStatus?.({
        code: telemetryStatusCode.ERROR,
        message: error instanceof Error ? error.message : undefined,
      });
    } else {
      span?.setStatus?.({ code: telemetryStatusCode.OK });
    }
    span?.end();
  }, undefined);
};

export const recordTelemetryCounter = (
  counter: TxOBTelemetryCounter | undefined,
  telemetry: TxOBTelemetryInstruments | undefined,
  attributes?: TxOBTelemetryAttributes,
): void => {
  suppressTelemetryRuntimeError(() => {
    counter?.add(1, mergeTelemetryAttributes(telemetry, attributes));
  }, undefined);
};

export const recordTelemetryDuration = (
  histogram: TxOBTelemetryHistogram | undefined,
  telemetry: TxOBTelemetryInstruments | undefined,
  startedAt: number,
  attributes?: TxOBTelemetryAttributes,
): void => {
  suppressTelemetryRuntimeError(() => {
    histogram?.record(
      Date.now() - startedAt,
      mergeTelemetryAttributes(telemetry, attributes),
    );
  }, undefined);
};
