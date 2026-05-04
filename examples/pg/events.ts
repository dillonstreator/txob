import { z } from "zod";
import {
  defineTxOBEventSchemas,
  type TxOBEventDataMapFromSchemas,
} from "../../src/index.js";

export const eventTypes = {
  ResourceSaved: "ResourceSaved",
  EventMaxErrorsReached: "EventMaxErrorsReached",
} as const;

export const eventSchemas = defineTxOBEventSchemas({
  [eventTypes.ResourceSaved]: z.object({
    type: z.literal("activity"),
    id: z.string().uuid(),
  }),
  [eventTypes.EventMaxErrorsReached]: z.object({
    failedEventId: z.string().uuid(),
    failedEventType: z.string(),
    failedEventCorrelationId: z.string().uuid(),
  }),
});

export type EventType = keyof typeof eventSchemas;
export type EventDataMap = TxOBEventDataMapFromSchemas<typeof eventSchemas>;
