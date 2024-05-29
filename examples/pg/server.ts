import http from "node:http";
import { randomUUID } from "node:crypto";
import { Client } from "pg";
import dotenv from "dotenv";
import gracefulShutdown from "http-graceful-shutdown";
dotenv.config();

const eventTypes = {
  ResourceSaved: "ResourceSaved",
} as const;

export type EventType = keyof typeof eventTypes;

const main = async () => {
  const client = new Client({
    user: process.env.POSTGRES_USER,
    password: process.env.POSTGRES_PASSWORD,
    database: process.env.POSTGRES_DB,
    port: parseInt(process.env.POSTGRES_PORT || "5434"),
  });
  await client.connect();
  await migrate(client);

  const server = http.createServer(async (req, res) => {
    const correlationId = randomUUID();
    try {
      const activityId = randomUUID();
      await client.query("BEGIN");
      await client.query(
        `INSERT INTO activity (id, timestamp, ip, ua, method, path, correlation_id) VALUES (
                $1, $2, $3, $4, $5, $6, $7
            )`,
        [
          activityId,
          new Date(),
          req.socket.remoteAddress,
          req.headers["user-agent"],
          req.method,
          req.url,
          correlationId,
        ],
      );
      await client.query(
        `INSERT INTO events (id, type, data, correlation_id, handler_results, errors) VALUES (
                $1, $2, $3, $4, $5, $6
            )`,
        [
          randomUUID(),
          eventTypes.ResourceSaved,
          {
            type: "activity",
            id: activityId,
          },
          correlationId,
          {},
          0,
        ],
      );
      await client.query("COMMIT");
      res.statusCode = 201;
    } catch (e) {
      await client.query("ROLLBACK");
      console.log(e);
      res.statusCode = 500;
    }
    res.end();
  });
  const port = process.env.PORT || 3003;
  server.listen(port, () => console.log(`listening on ${port}`));

  gracefulShutdown(server);
};

if (require.main === module) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}

export const migrate = async (client: Client): Promise<void> => {
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
  await client.query(`CREATE TABLE IF NOT EXISTS activity (
    id UUID,
    timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    ip TEXT,
    ua TEXT,
    method TEXT,
    path TEXT,
    correlation_id UUID
)`);
};
