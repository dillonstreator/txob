import http from 'http';
import { Client } from 'pg';
import { randomUUID } from 'crypto';
import { OutboxEventHandlerMap, processEvents } from '../src/processor';
import { createEventProcessorClient } from '../src/pg/client';

const main = async () => {
    const client = new Client({
        user: process.env.POSTGRES_USER,
        password: process.env.POSTGRES_PASSWORD,
        database: process.env.POSTGRES_DB,
    });
    await client.connect();
    await client.query(`CREATE TABLE IF NOT EXISTS events (
    id UUID,
    timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    type VARCHAR(255) NOT NULL,
    data JSONB,
    correlation_id UUID,
    handler_results JSONB,
    errors INTEGER,
    backoff_until TIMESTAMPTZ,
    processed_at TIMESTAMPTZ
)`);

    const ab = new AbortController();

    const processorClient = createEventProcessorClient<EventType>(client);

    const processorTick = () => {
        if (ab.signal.aborted) return;

        processEvents(processorClient, eventHandlerMap).finally(() => {
            setTimeout(processorTick, 5000);
        });
    };
    processorTick();

    const server = http.createServer(async (req, res) => {
        try {
            await client.query(
                `INSERT INTO events (id, type, data, correlation_id, handler_results, errors) VALUES (
                $1, $2, $3, $4, $5, $6
            )`,
                [
                    randomUUID(),
                    eventTypes.ResourceSaved,
                    {},
                    randomUUID(),
                    {},
                    0,
                ]
            );
            res.statusCode = 201;
        } catch (e) {
            console.log(e);
            res.statusCode = 500;
        }
        res.end();
    });
    const port = process.env.PORT || 3000;
    server.listen(port, () => console.log(`listening on ${port}`));
};

const eventTypes = {
    ResourceSaved: 'ResourceSaved',
} as const;
type EventType = keyof typeof eventTypes;

const eventHandlerMap: OutboxEventHandlerMap<EventType> = {
    ResourceSaved: {
        thing1: async (event) => {
            if (Math.random() > 0.9) throw new Error('some issue');

            console.log('thing1');
        },
        thing2: async (event) => {
            if (Math.random() > 0.9) throw new Error('some issue');

            console.log('thing2');
        },
        thing3: async (event) => {
            if (Math.random() > 0.75) throw new Error('some issue');

            console.log('thing3');
        },
    },
};

if (require.main === module) {
    main().catch((err) => {
        console.error(err);
        process.exit(1);
    });
}
