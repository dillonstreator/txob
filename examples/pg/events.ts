import { z } from "zod";
import type { TxOBEventSchemaMap } from "../../src/index.js";

export const eventTypes = {
  ResourceSaved: "ResourceSaved",
  EventMaxErrorsReached: "EventMaxErrorsReached",
} as const;

export type EventType = keyof typeof eventTypes;

export const eventSchemas = {
  [eventTypes.ResourceSaved]: z.object({
    type: z.literal("activity"),
    id: z.uuid(),
  }),
  [eventTypes.EventMaxErrorsReached]: z.object({
    failedEventId: z.uuid(),
    failedEventType: z.string(),
    failedEventCorrelationId: z.uuid(),
  }),
} satisfies TxOBEventSchemaMap<EventType>;
