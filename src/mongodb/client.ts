import { MongoClient, ObjectId } from "mongodb";
import type {
  TxOBEvent,
  TxOBProcessorClient,
  TxOBProcessorClientOpts,
  TxOBTransactionProcessorClient,
} from "../processor.js";
import { getDate } from "../date.js";

const createReadyToProcessFilter = (maxErrors: number) => ({
  processed_at: null,
  $and: [
    {
      $or: [{ backoff_until: null }, { backoff_until: { $lt: getDate() } }],
    },
    {
      $or: [{ lock: null }, { lock: { $exists: false } }],
    },
  ],
  errors: { $lt: maxErrors },
});

export const createProcessorClient = <EventType extends string>(
  mongo: MongoClient,
  db: string,
  collection: string = "events",
  limit: number = 100,
): TxOBProcessorClient<EventType> => {
  const getEventsToProcess = async (
    opts: TxOBProcessorClientOpts,
  ): Promise<Pick<TxOBEvent<EventType>, "id" | "errors">[]> => {
    const filter = createReadyToProcessFilter(opts.maxErrors);

    const events = (await mongo
      .db(db)
      .collection(collection)
      .find(filter)
      .project({ id: 1, errors: 1 })
      .limit(limit)
      .sort("timestamp", "asc")
      .toArray()) as Pick<TxOBEvent<EventType>, "id" | "errors">[];

    return events;
  };

  const transaction: TxOBProcessorClient<EventType>["transaction"] = async (
    fn: (
      txProcessorClient: TxOBTransactionProcessorClient<EventType>,
    ) => Promise<void>,
  ): Promise<void> => {
    await mongo.withSession(async (session): Promise<void> => {
      await session.withTransaction(async (): Promise<void> => {
        await fn({
          getEventByIdForUpdateSkipLocked: async (
            eventId: TxOBEvent<EventType>["id"],
            opts: TxOBProcessorClientOpts,
          ): Promise<TxOBEvent<EventType> | null> => {
            // https://www.mongodb.com/blog/post/how-to-select--for-update-inside-mongodb-transactions
            // Note: findOneAndUpdate returns null (not an error) when document not found,
            // so any thrown error is unexpected and will propagate to the transaction handler
            const result = await mongo
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
                  returnDocument: "after",
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
              );

            if (!result || !result.value) return null;

            return result.value as TxOBEvent<EventType>;
          },
          updateEvent: async (event: TxOBEvent<EventType>): Promise<void> => {
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
                    lock: null,
                  },
                },
                {
                  session,
                },
              );
          },
          createEvent: async (
            event: Omit<TxOBEvent<EventType>, "processed_at" | "backoff_until">,
          ): Promise<void> => {
            await mongo.db(db).collection(collection).insertOne(
              {
                id: event.id,
                timestamp: event.timestamp,
                type: event.type,
                data: event.data,
                correlation_id: event.correlation_id,
                handler_results: event.handler_results,
                errors: event.errors,
                processed_at: null,
                backoff_until: null,
                lock: null,
              },
              {
                session,
              },
            );
          },
        });
      });
    });
  };

  return {
    getEventsToProcess,
    transaction,
  };
};
