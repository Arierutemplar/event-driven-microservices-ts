import amqp from "amqplib";
import "dotenv/config";

export const ORDERS_QUEUE = "orders";
const ORDERS_DLX = "orders.dlx";
const ORDERS_DLQ = "orders.dlq";

const rabbitmqUrl = process.env.RABBITMQ_URL;
if (!rabbitmqUrl) {
  throw new Error("RABBITMQ_URL is required");
}

const brokerConnection = await amqp.connect(rabbitmqUrl);
export const brokerChannel = await brokerConnection.createConfirmChannel();

await brokerChannel.assertExchange(ORDERS_DLX, "direct", { durable: true });
await brokerChannel.assertQueue(ORDERS_DLQ, { durable: true });
await brokerChannel.bindQueue(ORDERS_DLQ, ORDERS_DLX, ORDERS_DLQ);
await brokerChannel.assertQueue(ORDERS_QUEUE, {
  durable: true,
  arguments: {
    "x-dead-letter-exchange": ORDERS_DLX,
    "x-dead-letter-routing-key": ORDERS_DLQ
  }
});
