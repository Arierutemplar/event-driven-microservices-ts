import amqp from "amqplib";
import "dotenv/config";
import pino from "pino";

const ORDERS_QUEUE = "orders";
const ORDERS_DLX = "orders.dlx";
const ORDERS_DLQ = "orders.dlq";
const logger = pino({ name: "notification-service" });

export async function startOrderConsumer(): Promise<void> {
  const rabbitmqUrl = process.env.RABBITMQ_URL;
  if (!rabbitmqUrl) {
    throw new Error("RABBITMQ_URL is required");
  }

  const connection = await amqp.connect(rabbitmqUrl);
  const channel = await connection.createChannel();
  await channel.assertExchange(ORDERS_DLX, "direct", { durable: true });
  await channel.assertQueue(ORDERS_DLQ, { durable: true });
  await channel.bindQueue(ORDERS_DLQ, ORDERS_DLX, ORDERS_DLQ);
  await channel.assertQueue(ORDERS_QUEUE, {
    durable: true,
    arguments: {
      "x-dead-letter-exchange": ORDERS_DLX,
      "x-dead-letter-routing-key": ORDERS_DLQ
    }
  });

  const { consumerTag } = await channel.consume(ORDERS_QUEUE, (message) => {
    if (!message) {
      return;
    }

    const correlationId = message.properties.headers?.correlationId;

    try {
      const payload: unknown = JSON.parse(message.content.toString("utf8"));
      logger.info(
        {
          correlationId,
          routingKey: message.fields.routingKey,
          message: payload
        },
        "Order event received"
      );
      channel.ack(message);
    } catch (error) {
      logger.error(
        { err: error, correlationId, routingKey: message.fields.routingKey },
        "Failed to process order event; sending to dead-letter queue"
      );
      channel.nack(message, false, false);
    }
  });

  logger.info({ queue: ORDERS_QUEUE, consumerTag }, "Listening for order events");
  logger.info({ exchange: ORDERS_DLX, queue: ORDERS_DLQ }, "Dead-letter queue configured");
}
