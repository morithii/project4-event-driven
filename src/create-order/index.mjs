// =============================================================================
// CreateOrder Lambda
// Triggered by: SQS intake queue (project4-order-intake-queue), batches of up to 10
// Each message = one order accepted by API Gateway (already validated there):
//   body                         -> the client's JSON { customerId, items }
//   messageAttributes.orderId    -> ID API Gateway generated and returned in the 202
//   messageAttributes.acceptedAt -> when API Gateway received it (epoch ms)
// Does: save order to DynamoDB -> publish OrderCreated event
// =============================================================================

import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, TransactWriteCommand } from "@aws-sdk/lib-dynamodb";
import { EventBridgeClient, PutEventsCommand } from "@aws-sdk/client-eventbridge";

// --- Initialisation (runs ONCE per container, not on every message) ---------
const client = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const eventBridge = new EventBridgeClient({});
const TABLE_NAME = process.env.TABLE_NAME;
const EVENT_BUS_NAME = process.env.EVENT_BUS_NAME;

// --- Publish OrderCreated (unchanged from Exercise 1) ------------------------
// Returns true/false rather than throwing: the order is already saved, so a
// publish failure is logged loudly rather than retried (see the note at the end
// of processRecord - Exercise 8 makes this fully reliable).
const publishOrderCreated = async (order) => {
  try {
    const result = await eventBridge.send(
      new PutEventsCommand({
        Entries: [
          {
            EventBusName: EVENT_BUS_NAME,
            Source: "project4.orders",     // WHO sent it - rules filter on this
            DetailType: "OrderCreated",    // WHAT happened - past tense, because events are facts
            Detail: JSON.stringify(order), // the payload consumers receive
          },
        ],
      })
    );

    // TRAP: PutEvents does NOT throw when an entry is rejected - check the count
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

// --- Process ONE queued order --------------------------------------------------
// Throws if the message can't be processed, so the handler marks it as failed
// and SQS retries it.
const processRecord = async (record) => {
  // 1. READ the IDs API Gateway attached as message attributes
  const orderId = record.messageAttributes?.orderId?.stringValue;
  const acceptedAtMs = Number(record.messageAttributes?.acceptedAt?.stringValue);
  if (!orderId) {
    throw new Error("Message has no orderId attribute");
  }

  // 2. PARSE the order. API Gateway's JSON Schema already validated it, so this
  //    check is only a safety net ("defence in depth") in case something else
  //    ever writes to the queue.
  const { customerId, items } = JSON.parse(record.body);
  if (!customerId || !Array.isArray(items) || items.length === 0 || items.length > 100) {
    throw new Error("Order body is invalid");
  }

  // 3. BUILD the rows. createdAt = when the CUSTOMER placed it (API Gateway's
  //    timestamp), not when we got round to processing it - in an async system
  //    those can differ, and the customer's time is the one that matters.
  const createdAt = Number.isFinite(acceptedAtMs) ? new Date(acceptedAtMs).toISOString() : new Date().toISOString();

  const savedItems = items.map((item, index) => ({
    itemId: `item-${String(index + 1).padStart(3, "0")}`,
    productId: item.productId,
    quantity: item.quantity,
    price: item.price,
  }));

  // All items in one transaction: all saved or none
  const TransactItems = savedItems.map((item) => ({
    Put: {
      TableName: TABLE_NAME,
      Item: { orderId, customerId, createdAt, ...item },
      ConditionExpression: "attribute_not_exists(orderId)",   // never overwrite an existing order
    },
  }));

  // 4. SAVE. A duplicate message (SQS is at-least-once) makes the condition fail.
  //    Because transactions are all-or-nothing, a failed condition means an
  //    earlier delivery ALREADY saved the whole order - so skip, don't error.
  try {
    await client.send(new TransactWriteCommand({ TransactItems }));
  } catch (err) {
    const alreadySaved =
      err.name === "TransactionCanceledException" &&
      (err.CancellationReasons ?? []).some((reason) => reason.Code === "ConditionalCheckFailed");
    if (alreadySaved) {
      console.log(JSON.stringify({ message: "Duplicate message ignored - order already saved", orderId }));
      return;
    }
    throw err;   // throttling, permissions etc. -> retry this message
  }

  console.log(JSON.stringify({ message: "Order created", orderId, itemCount: savedItems.length }));

  // 5. PUBLISH. Known gap: if the publish fails AND the message is later
  //    redelivered, step 4 treats it as a duplicate and the event is never sent.
  //    Exercise 8 (idempotency) fixes this by recording processing state.
  await publishOrderCreated({ orderId, customerId, createdAt, itemCount: savedItems.length, items: savedItems });
};

// --- Handler: process a BATCH of queued orders -------------------------------
export const handler = async (event) => {
  const batchItemFailures = [];

  for (const record of event.Records) {
    try {
      await processRecord(record);
    } catch (err) {
      console.error(JSON.stringify({ message: "Failed to process order message", messageId: record.messageId, error: err.name, detail: err.message }));
      batchItemFailures.push({ itemIdentifier: record.messageId });   // retry just this one
    }
  }

  // No HTTP response any more - API Gateway already answered the customer (202).
  // This return value is for SQS: which messages to retry.
  return { batchItemFailures };
};