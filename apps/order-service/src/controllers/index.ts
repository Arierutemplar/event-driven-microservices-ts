import * as grpc from "@grpc/grpc-js";
import { PrismaClient } from "@prisma/client";
import type { Request, Response } from "express";
import "dotenv/config";
import { randomUUID } from "node:crypto";
import { brokerChannel, ORDERS_QUEUE } from "../broker/index.js";
import { getUser } from "../grpc/index.js";

interface CreateOrderRequest {
  userId: string;
  productId: string;
  quantity: number;
}

const prisma = new PrismaClient();

function isCreateOrderRequest(value: unknown): value is CreateOrderRequest {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return false;
  }

  const userId = Reflect.get(value, "userId");
  const productId = Reflect.get(value, "productId");
  const quantity = Reflect.get(value, "quantity");
  return (
    typeof userId === "string" &&
    userId.trim().length > 0 &&
    typeof productId === "string" &&
    productId.trim().length > 0 &&
    typeof quantity === "number" &&
    Number.isInteger(quantity) &&
    quantity > 0
  );
}

export async function createOrder(request: Request, response: Response): Promise<void> {
  const correlationId = request.get("x-correlation-id")?.trim() || randomUUID();
  response.setHeader("x-correlation-id", correlationId);

  const body: unknown = request.body;
  if (!isCreateOrderRequest(body)) {
    response.status(400).json({
      error: "userId, productId, and a positive integer quantity are required"
    });
    return;
  }

  const metadata = new grpc.Metadata();
  metadata.set("x-correlation-id", correlationId);

  try {
    await getUser(body.userId, metadata);
  } catch (error) {
    if (error instanceof Error && "code" in error && error.code === grpc.status.NOT_FOUND) {
      response.status(404).json({ error: "User not found" });
      return;
    }

    console.error("Failed to validate user through UserService:", error);
    response.status(503).json({ error: "User validation service unavailable" });
    return;
  }

  let order: Awaited<ReturnType<typeof prisma.order.create>>;
  try {
    order = await prisma.order.create({
      data: {
        userId: body.userId,
        productId: body.productId,
        quantity: body.quantity
      }
    });
  } catch (error) {
    console.error("Failed to persist order:", error);
    response.status(500).json({ error: "Failed to create order" });
    return;
  }

  try {
    const message = Buffer.from(
      JSON.stringify({ event: "order.created", data: order }),
      "utf8"
    );
    brokerChannel.sendToQueue(ORDERS_QUEUE, message, {
      persistent: true,
      contentType: "application/json",
      headers: { correlationId }
    });
    await brokerChannel.waitForConfirms();
  } catch (error) {
    console.error("Failed to publish order.created event:", error);
    response.status(503).json({ error: "Order event broker unavailable" });
    return;
  }

  response.status(201).json({ order });
}
