# Event-Driven Order Platform

TypeScript monorepo for a small order flow split across three independently deployed services. An HTTP client creates an order on `order-service`. That service validates the user with a synchronous gRPC call, stores the order in its own SQLite database, and publishes an `order.created` event to RabbitMQ. `notification-service` consumes that event on its own schedule and records it. `user-service` does not share a database, a process, or a deployable with either of the others.

The workspace root is [`apps/`](apps/). There is no separate API gateway: `order-service` is the synchronous edge. There is no Redis, NATS, Prometheus, Grafana, or Jaeger instrumentation in this repository. Observability is correlation identifiers, structured logs, and the RabbitMQ management UI.

This is a reference implementation of the communication patterns below. It is not a production deployment. gRPC is plaintext, the user catalog is a single in-memory record, and the order write and the event publish are not in one transaction.

## Key Architectural Highlights

- **Deployable isolation.** `user-service`, `order-service`, and `notification-service` are separate npm workspaces, separate Docker images, and separate processes. A failure in the consumer does not stop order creation. A failure in user lookup stops order creation on purpose, because that check is on the synchronous path.
- **Two communication styles.** User validation is request/response gRPC (`UserService.GetUserById`). Order notification is asynchronous AMQP. The HTTP handler does not wait for the consumer.
- **State ownership.** Only `order-service` persists data. Prisma maps an `Order` model to SQLite. `user-service` answers from a hard-coded user (`id: "1"`, Ada Lovelace) and returns `NOT_FOUND` for every other id. `notification-service` keeps no store; its side effect is a log line.
- **Eventual consistency, with a real gap.** After the row commits, the handler publishes `order.created` on a confirm channel. If that publish fails, the API returns `503` and the row remains. There is no transactional outbox, so a crash between those two steps loses the event while keeping the order.
- **Failure containment on the sync path.** The gRPC client sets a 3 second deadline and is wrapped in an Opossum circuit breaker. When the breaker is open, or the user service cannot be reached, the handler returns `503` and does not write an order.
- **Failure containment on the async path.** The `orders` queue is durable and dead-letters to `orders.dlx` / `orders.dlq`. The consumer acknowledges a parsed message and `nack`s an unparseable one with `requeue: false`, so a poison body does not spin on the main queue.

| Service | Interface | State | Role in the flow |
| --- | --- | --- | --- |
| `user-service` | gRPC `0.0.0.0:50051` | In-memory mock user | Answers `GetUserById` |
| `order-service` | HTTP `POST /orders` (default port `3000`) | SQLite via Prisma | Validates, persists, publishes |
| `notification-service` | AMQP consumer, no inbound port | None | Logs `orders` deliveries |

Shared contract: [`apps/proto/user.proto`](apps/proto/user.proto). Event contract, published as persistent JSON to the `orders` queue:

```json
{
  "event": "order.created",
  "data": {
    "id": "<uuid>",
    "userId": "1",
    "productId": "prod-123",
    "quantity": 2,
    "createdAt": "<timestamp>"
  }
}
```

`order-service` publishes with `sendToQueue`, so the message enters through RabbitMQ's default exchange. The only application exchange is the dead-letter exchange.

## Architecture Diagram

```mermaid
flowchart TB
  client["HTTP clients"]

  subgraph edge["Synchronous edge"]
    order["order-service<br/>Express 5 · POST /orders<br/>correlation id + publisher confirms"]
  end

  user["user-service<br/>gRPC UserService.GetUserById<br/>in-memory user id = 1"]
  sqlite[("order-service SQLite<br/>Order table · Prisma<br/>no shared database")]

  subgraph broker["RabbitMQ"]
    ordersQ["durable queue: orders<br/>default exchange"]
    dlx["direct exchange: orders.dlx"]
    dlq[("durable queue: orders.dlq")]
  end

  notification["notification-service<br/>manual ack · Pino"]
  observe["Observability<br/>x-correlation-id<br/>Pino JSON logs<br/>RabbitMQ management UI :15672"]

  client -->|"POST /orders<br/>optional x-correlation-id"| order
  order -->|"gRPC metadata x-correlation-id<br/>deadline 3s · Opossum"| user
  order -->|"prisma.order.create"| sqlite
  order -->|"order.created<br/>persistent JSON · confirm channel"| ordersQ
  ordersQ -->|"consume"| notification
  ordersQ -->|"nack requeue=false"| dlx
  dlx -->|"routing key orders.dlq"| dlq
  order -.-> observe
  user -.-> observe
  notification -.-> observe
  ordersQ -.-> observe
```

There is no gateway process in front of `order-service`. Clients call that service directly. `user-service` is reachable only over gRPC. `notification-service` has no request API.

```mermaid
sequenceDiagram
  participant C as Client
  participant O as order-service
  participant U as user-service
  participant DB as SQLite
  participant Q as RabbitMQ orders
  participant N as notification-service

  C->>O: POST /orders (x-correlation-id or generated UUID)
  O->>U: GetUserById + metadata
  alt user missing
    U-->>O: NOT_FOUND
    O-->>C: 404
  else user service failing or circuit open
    U-->>O: UNAVAILABLE / timeout
    O-->>C: 503
  else user id is 1
    U-->>O: Ada Lovelace
    O->>DB: INSERT Order
    O->>Q: order.created (persistent, confirm)
    Q-->>O: broker confirm
    O-->>C: 201 + x-correlation-id
    Q->>N: delivery
    N->>N: JSON.parse and log correlationId
    N->>Q: ack
  end
```

## Distributed Resilience & Event Patterns

What the code actually does:

| Pattern | Where | Behavior |
| --- | --- | --- |
| Publisher confirms | `order-service` broker | `createConfirmChannel()` and `waitForConfirms()` after `sendToQueue`. The handler returns `201` only after the broker confirms. |
| Durable messaging | both AMQP clients | Queue `orders`, exchange `orders.dlx`, and queue `orders.dlq` are declared durable. Messages are published with `persistent: true`. |
| Dead-letter queue | both AMQP clients | `orders` is declared with `x-dead-letter-exchange=orders.dlx` and `x-dead-letter-routing-key=orders.dlq`. `orders.dlq` is bound to that exchange. |
| Poison-message rejection | `notification-service` | `JSON.parse` failure logs the error and calls `nack(message, false, false)`. The message is dead-lettered instead of requeued. |
| Circuit breaker | `order-service` gRPC client | Opossum wraps `GetUserById` with `timeout: 3000` and `errorThresholdPercentage: 50`. An open circuit rejects with `UNAVAILABLE` before placing another call. |
| Call deadline | same gRPC client | Each attempt also sets a gRPC deadline of 3 seconds. |
| Correlation | HTTP, gRPC, AMQP | Missing `x-correlation-id` becomes a UUID. The value is returned on the HTTP response, sent as gRPC metadata, and set on the AMQP header `correlationId`. |

Library defaults that this code does not override, from Opossum 10: `resetTimeout` is 30 seconds and `volumeThreshold` is 0. The breaker opens when the failure rate is **greater than** 50 percent. With a zero volume threshold, the first rejected call is a 100 percent failure rate, so one failed lookup opens the circuit for 30 seconds.

`GetUserById` rejects on `NOT_FOUND`. That rejection is a breaker failure, not a successful business response. A request for an unknown user can open the circuit and make the next request for user `1` return `503` until the reset window elapses.

Not implemented, and not claimed:

- **Transactional outbox.** Persist and publish are sequential. A confirmed publish can still be followed by a process crash before the HTTP response is written; the reverse failure (row committed, publish failed) returns `503` with the row left in place.
- **Consumer idempotency.** Any body that parses as JSON is logged and acknowledged, including duplicates and payloads that are not `order.created`. There is no inbox table or deduplication key. Repeating `POST /orders` creates a new order id and a new event.
- **Retry with backoff.** There is no retry loop in the consumer and no delay queue. Unparseable messages go straight to `orders.dlq`. Nothing in this repo consumes that queue.
- **Broker prefetch.** The consumer does not set QoS.
- **Health or metrics endpoints.** Container health checks in Compose probe TCP ports (and process liveness for the consumer). The applications do not expose `/health` or `/metrics`.

## Monorepo Setup & Local Development

### Layout

```text
.
├── README.md
└── apps/                          npm workspace root (microservices-core)
    ├── package.json               workspaces: user, order, notification
    ├── package-lock.json
    ├── docker-compose.yml
    ├── proto/user.proto
    ├── user-service/
    ├── order-service/             Prisma schema, Jest, circuit breaker
    └── notification-service/
```

Tooling is npm workspaces on Node.js 20, TypeScript `strict`, and ESM (`NodeNext`, `.js` specifiers in relative imports). It is not a pnpm or Yarn workspace.

### Prerequisites

- Node.js 20 or newer, with npm
- Docker Engine and Docker Compose v2, for the broker and the full stack

### Run the whole stack

From `apps/`:

```bash
docker compose up --build
```

Compose builds each service with its own multi-stage Dockerfile. The build context is `apps/`, because each Dockerfile copies the workspace lockfile and the shared proto. Startup order is enforced with health checks:

1. RabbitMQ must answer `rabbitmq-diagnostics ping`.
2. `user-service` must accept TCP connections on port `50051`.
3. `order-service` and `notification-service` start only after the broker is healthy. `order-service` also waits for `user-service`.
4. `order-service` is marked healthy once it accepts TCP connections on port `3000`.

`order-service` runs `prisma db push` on container start, then drops privileges to the `node` user. The SQLite file is stored in the `order-data` volume at `/app/order-service/data/orders.db`. RabbitMQ data is stored in `rabbitmq-data`. Two bridge networks separate traffic: `service-mesh` (gRPC between order and user) and `messaging` (AMQP). Only `order-service` joins both.

Published ports:

| Port | Process |
| --- | --- |
| `3000` | `order-service` HTTP. Override the host port with `ORDER_SERVICE_PORT`. |
| `50051` | `user-service` gRPC |
| `5672` | AMQP |
| `15672` | RabbitMQ management UI (`orders` / `orders`) |

The Compose broker user and password are local defaults. Do not reuse them anywhere else, and do not commit a real `RABBITMQ_URL`. Compose sets the URL itself and does not load service `.env` files.

Stop and keep volumes:

```bash
docker compose down
```

Stop and delete volumes:

```bash
docker compose down -v
```

### Run services on the host

Start only the broker:

```bash
cd apps
docker compose up rabbitmq
```

Install workspaces and create the SQLite schema:

```bash
cd apps
npm install
npm exec --workspace=order-service -- prisma db push
```

Create `apps/order-service/.env`:

```dotenv
PORT=3000
USER_SERVICE_URL=localhost:50051
RABBITMQ_URL=amqp://orders:orders@localhost:5672
DATABASE_URL="file:./dev.db"
```

Create `apps/notification-service/.env`:

```dotenv
RABBITMQ_URL=amqp://orders:orders@localhost:5672
```

`DATABASE_URL` is read by Prisma from `order-service/prisma/schema.prisma`. A relative SQLite URL is resolved from that schema directory.

In three terminals, from `apps/`:

```bash
npm run dev --workspace=user-service
```

```bash
npm run dev --workspace=order-service
```

```bash
npm exec --workspace=notification-service -- tsx src/index.ts
```

`user-service` and `order-service` watch TypeScript through `tsx`. `notification-service` has no `dev` script; the `tsx` command above is the host equivalent. Production scripts in all three workspaces run `node dist/index.js` after `npm run build`.

`order-service` connects to RabbitMQ while the module graph loads. If `RABBITMQ_URL` is missing or the broker is down, that process exits before it listens.

### Tests and compile

```bash
cd apps
npm test --workspace=order-service
npm run build
```

The Jest test checks the shape of a sample order object. It does not boot Express, gRPC, Prisma, or RabbitMQ.

### Exercise the flow

Create an order for the mock user:

```bash
curl -i -X POST http://localhost:3000/orders \
  -H "Content-Type: application/json" \
  -H "x-correlation-id: demo-order-001" \
  -d "{\"userId\":\"1\",\"productId\":\"prod-123\",\"quantity\":2}"
```

Expect `201`, a generated order `id`, and the response header `x-correlation-id: demo-order-001`. The same value is written to the gRPC metadata, the AMQP header, the `user-service` log, and the `notification-service` log.

Unknown user:

```bash
curl -i -X POST http://localhost:3000/orders \
  -H "Content-Type: application/json" \
  -d "{\"userId\":\"missing-user\",\"productId\":\"prod-123\",\"quantity\":1}"
```

Expect `404` and no row or event. A body without a non-empty `userId`, a non-empty `productId`, and a positive integer `quantity` returns `400`.

Dead letter: open `http://localhost:15672` as `orders` / `orders`. Publish to the default exchange with routing key `orders` and a body that is not JSON, for example `{json-invalid`. The consumer logs the parse failure, rejects the delivery without requeue, and the message lands on `orders.dlq`.

## Observability & Monitoring

Correlation is the trace mechanism this repository implements. `order-service` reads `x-correlation-id` or generates a UUID, echoes it on the response, forwards it as gRPC metadata, and copies it onto the AMQP message header. `user-service` logs that metadata field with Pino (`name: "user-service"`) together with the requested user id. `notification-service` logs it with Pino (`name: "notification-service"`) together with the routing key and the parsed body.

`order-service` itself logs with `console.info` and `console.error`. Its lines are not JSON and do not include the correlation id in the message body; the id is on the HTTP response and on the downstream calls.

No service exports Prometheus metrics, OpenTelemetry spans, or a log shipping agent. Queue depth, consumer count, and dead-letter depth are visible in the RabbitMQ management UI on port `15672`. Compose health checks only tell you that the process is accepting connections (`user-service`, `order-service`, RabbitMQ) or that PID 1 is alive (`notification-service`). They do not measure error rate, and the Opossum breaker does not publish its open/half-open state anywhere.

To follow one request, use the same `x-correlation-id` in the curl header and search for that string in the `user-service` and `notification-service` container logs:

```bash
docker compose logs -f user-service notification-service order-service
```
