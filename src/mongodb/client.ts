import { MongoClient, ObjectId } from "mongodb";
import { TxOBEvent, TxOBProcessorClient } from "../processor";
import { getDate } from "../date";

const createReadyToProcessFilter = (maxErrors: number) => ({
  processed_at: null,
  $or: [{ backoff_until: null }, { backoff_until: { $lt: getDate() } }],
  errors: { $lt: maxErrors },
});

export const createProcessorClient = <EventType extends string>(
  mongo: MongoClient,
  db: string,
  collection: string = "events",
): TxOBProcessorClient<EventType> => ({
  findReadyToProcessEvents: async (opts) => {
    const events = (await mongo
      .db(db)
      .collection(collection)
      .find(createReadyToProcessFilter(opts.maxErrors))
      .project({ id: 1, errors: 1 })
      .toArray()) as Pick<TxOBEvent<EventType>, "id" | "errors">[];

    return events;
  },
  transaction: async (fn) => {
    await mongo.withSession(async (session): Promise<void> => {
      await fn({
        findReadyToProcessEventByIdForUpdateSkipLocked: async (
          eventId,
          opts,
        ) => {
          try {
            // https://www.mongodb.com/blog/post/how-to-select--for-update-inside-mongodb-transactions
            const event = (await mongo
              .db(db)
              .collection(collection)
              .findOneAndUpdate(
                { id: eventId, ...createReadyToProcessFilter(opts.maxErrors) },
                {
                  $set: {
                    lock: new ObjectId(),
                  },
                },
                {
                  session,
                  projection: {
                    id: 1,
                    timestamp: 1,
                    type: 1,
                    data: 1,
                    correlation_id: 1,
                    handler_results: 1,
                    errors: 1,
                    backoff_until: 1,
                    processed_at: 1,
                  },
                },
              )) as unknown;

            if (!event) return null;

            return event as TxOBEvent<EventType>;
          } catch (error) {
            return null;
          }
        },
        updateEvent: async (event) => {
          await mongo
            .db(db)
            .collection(collection)
            .updateOne(
              {
                id: event.id,
              },
              {
                $set: {
                  handler_results: event.handler_results,
                  errors: event.errors,
                  processed_at: event.processed_at,
                  backoff_until: event.backoff_until,
                },
              },
              {
                session,
              },
            );
        },
      });
    });
  },
});
