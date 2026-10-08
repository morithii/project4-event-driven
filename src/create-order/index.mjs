import { randomUUID } from "node:crypto";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, TransactWriteCommand } from "@aws-sdk/lib-dynamodb";
import { EventBridgeClient, PutEventsCommand } from "@aws-sdk/client-eventbridge";

// Created once per container, outside the handler, and reused across invocations
const client = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const eventBridge = new EventBridgeClient({});
const TABLE_NAME = process.env.TABLE_NAME;
const EVENT_BUS_NAME = process.env.EVENT_BUS_NAME;

const response = (statusCode, body) => ({
  statusCode,
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify(body),
});

// Publishes OrderCreated. Returns true/false rather than throwing: the order is
// already saved at this point, so a publish failure must not turn into a 500.
const publishOrderCreated = async (order) => {
  try {
    const result = await eventBridge.send(
      new PutEventsCommand({
        Entries: [
          {
            EventBusName: EVENT_BUS_NAME,
            Source: "project4.orders",
            DetailType: "OrderCreated",
            Detail: JSON.stringify(order),
          },
        ],
      })
    );

    // PutEvents does NOT throw when an entry is rejected - it reports it here
    if (result.FailedEntryCount > 0) {
      console.error(JSON.stringify({ message: "OrderCreated event rejected", orderId: order.orderId, entry: result.Entries[0] }));
      return false;
    }

    console.log(JSON.stringify({ message: "OrderCreated event published", orderId: order.orderId, eventId: result.Entries[0].EventId }));
    return true;
  } catch (err) {
    console.error(JSON.stringify({ message: "Failed to publish OrderCreated event", orderId: order.orderId, error: err.name, detail: err.message }));
    return false;
  }
};

export const handler = async (event) => {
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

  const savedItems = items.map((item, index) => ({
    itemId: `item-${String(index + 1).padStart(3, "0")}`,
    productId: item.productId,
    quantity: item.quantity,
    price: item.price,
  }));

  const TransactItems = savedItems.map((item) => ({
    Put: {
      TableName: TABLE_NAME,
      Item: { orderId, customerId, createdAt, ...item },
      ConditionExpression: "attribute_not_exists(orderId)",
    },
  }));

  try {
    await client.send(new TransactWriteCommand({ TransactItems }));
  } catch (err) {
    console.error(JSON.stringify({ message: "Failed to save order", error: err.name, detail: err.message }));
    return response(500, { message: "Could not save order" });
  }

  console.log(JSON.stringify({ message: "Order created", orderId, itemCount: savedItems.length }));

  // Publish only AFTER the order is safely saved
  await publishOrderCreated({ orderId, customerId, createdAt, itemCount: savedItems.length, items: savedItems });

  return response(201, { orderId, status: "CREATED", source: "orders-api", itemCount: savedItems.length, createdAt });
};