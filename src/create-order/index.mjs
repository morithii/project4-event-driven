import { randomUUID } from "node:crypto";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, TransactWriteCommand } from "@aws-sdk/lib-dynamodb";

// Created once per container, outside the handler, and reused across invocations
const client = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const TABLE_NAME = process.env.TABLE_NAME;

const response = (statusCode, body) => ({
  statusCode,
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify(body),
});

export const handler = async (event) => {
  // Works for direct invokes now and API Gateway (event.body as a string) later
  let order;
  try {
    order = typeof event.body === "string" ? JSON.parse(event.body) : (event.body ?? event);
  } catch {
    return response(400, { message: "Request body must be valid JSON" });
  }

  const { customerId, items } = order;
  if (!customerId || !Array.isArray(items) || items.length === 0) {
    return response(400, { message: "customerId and a non-empty items array are required" });
  }
  if (items.length > 100) {
    return response(400, { message: "An order can contain at most 100 items" });
  }
  const invalid = items.find(
    (i) => !i.productId || !Number.isInteger(i.quantity) || i.quantity < 1 || typeof i.price !== "number"
  );
  if (invalid) {
    return response(400, { message: "Each item needs productId, a positive integer quantity and a numeric price" });
  }

  const orderId = randomUUID();
  const createdAt = new Date().toISOString();

  const TransactItems = items.map((item, index) => ({
    Put: {
      TableName: TABLE_NAME,
      Item: {
        orderId,
        itemId: `item-${String(index + 1).padStart(3, "0")}`,
        customerId,
        productId: item.productId,
        quantity: item.quantity,
        price: item.price,
        createdAt,
      },
      ConditionExpression: "attribute_not_exists(orderId)",
    },
  }));

  try {
    await client.send(new TransactWriteCommand({ TransactItems }));
  } catch (err) {
    console.error(JSON.stringify({ message: "Failed to save order", error: err.name, detail: err.message }));
    return response(500, { message: "Could not save order" });
  }

  console.log(JSON.stringify({ message: "Order created", orderId, itemCount: items.length }));
  return response(201, { orderId, status: "CREATED", source: "orders-api", itemCount: items.length, createdAt });
};
