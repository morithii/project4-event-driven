// =============================================================================
// CreateOrder Lambda  (idempotent - Exercise 8; global endpoint - Exercise 9)
// Triggered by: SQS intake queue (project4-order-intake-queue), batches of up to 10
// Each message = one order accepted by API Gateway (already validated there):
//   body                         -> the client's JSON { customerId, items }
//   messageAttributes.orderId    -> ID API Gateway generated and returned in the 202
//   messageAttributes.acceptedAt -> when API Gateway received it (epoch ms)
//
// Every order moves through these states on the "progress board"
// (the OrderIdempotencyTable), one row per orderId:
//
//   (no row) --claim--> IN_PROGRESS --save--> ORDER_SAVED --publish--> COMPLETED
//
// A message that arrives again reads the board and RESUMES from the missing step.
// =============================================================================

// Exercise 9: publishing to a GLOBAL ENDPOINT uses SigV4a - a request signature
// valid in more than one region. Importing this package switches the SDK's
// SigV4a support on. It has to be bundled in this function (see package.json)
// alongside the SDK clients below, so they all share the same SDK copy.
import "@aws-sdk/signature-v4-crt";

import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, TransactWriteCommand, UpdateCommand, GetCommand } from "@aws-sdk/lib-dynamodb";
import { EventBridgeClient, PutEventsCommand } from "@aws-sdk/client-eventbridge";

// --- Initialisation (runs ONCE per container, not on every message) ---------
const client = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const eventBridge = new EventBridgeClient({});
const TABLE_NAME = process.env.TABLE_NAME;
const IDEMPOTENCY_TABLE_NAME = process.env.IDEMPOTENCY_TABLE_NAME;
const EVENT_BUS_NAME = process.env.EVENT_BUS_NAME;

// Exercise 9: set in the PRIMARY region only (the template leaves it out in
// the secondary). When set, events go via the global endpoint, which picks
// the healthy region; when not set, they go straight to the local bus.
const EVENT_ENDPOINT_ID = process.env.EVENT_ENDPOINT_ID;

// Lock length: longer than the Lambda timeout (10s), shorter than the queue's
// visibility timeout (60s) - see Exercise 8 for why.
const LOCK_MS = 30_000;

// Board rows are deleted automatically (DynamoDB TTL) after this many days
const RECORD_TTL_DAYS = 7;

// Thrown when another invocation is working on the same order -> SQS retries later
class OrderLockedError extends Error {
  constructor(orderId) {
    super(`Order ${orderId} is being processed by another invocation - retry later`);
    this.name = "OrderLockedError";
  }
}

// --- Step 1: CLAIM the order on the progress board ---------------------------
// One conditional write does "check and claim" atomically, so if two Lambdas
// get the same message at once, DynamoDB lets exactly ONE succeed.
const claimOrder = async (orderId) => {
  const now = Date.now();
  const lockedUntil = now + LOCK_MS;

  try {
    const { Attributes } = await client.send(
      new UpdateCommand({
        TableName: IDEMPOTENCY_TABLE_NAME,
        Key: { orderId },
        UpdateExpression:
          "SET lockedUntil = :lockedUntil, #status = if_not_exists(#status, :inProgress), expiresAt = if_not_exists(expiresAt, :expiresAt)",
        ConditionExpression: "attribute_not_exists(orderId) OR (#status <> :completed AND lockedUntil < :now)",
        ExpressionAttributeNames: { "#status": "status" },   // "status" is a DynamoDB reserved word
        ExpressionAttributeValues: {
          ":lockedUntil": lockedUntil,
          ":inProgress": "IN_PROGRESS",
          ":expiresAt": Math.floor(now / 1000) + RECORD_TTL_DAYS * 24 * 60 * 60,   // TTL uses epoch SECONDS
          ":completed": "COMPLETED",
          ":now": now,
        },
        ReturnValues: "ALL_NEW",   // the row as it is now, so we know where to resume
      })
    );
    return { record: Attributes, lockedUntil };
  } catch (err) {
    if (err.name !== "ConditionalCheckFailedException") throw err;

    // Claim refused - read the row to find out why
    const { Item } = await client.send(
      new GetCommand({ TableName: IDEMPOTENCY_TABLE_NAME, Key: { orderId }, ConsistentRead: true })
    );
    if (Item?.status === "COMPLETED") {
      return { alreadyCompleted: true, record: Item };
    }
    throw new OrderLockedError(orderId);
  }
};

// --- Step 2: SAVE the order rows + mark ORDER_SAVED, in ONE transaction -------
const saveOrder = async (orderId, lockedUntil, summary) => {
  const orderRows = summary.items.map((item) => ({
    Put: {
      TableName: TABLE_NAME,
      Item: { orderId, customerId: summary.customerId, createdAt: summary.createdAt, ...item },
      ConditionExpression: "attribute_not_exists(orderId)",   // safety net: never overwrite an order
    },
  }));

  const markSaved = {
    Update: {
      TableName: IDEMPOTENCY_TABLE_NAME,
      Key: { orderId },
      UpdateExpression: "SET #status = :saved, orderSummary = :summary",   // store the "existing result"
      ConditionExpression: "lockedUntil = :lockedUntil",                   // only if WE still hold the claim
      ExpressionAttributeNames: { "#status": "status" },
      ExpressionAttributeValues: { ":saved": "ORDER_SAVED", ":summary": summary, ":lockedUntil": lockedUntil },
    },
  };

  try {
    await client.send(new TransactWriteCommand({ TransactItems: [...orderRows, markSaved] }));
  } catch (err) {
    if (err.name === "TransactionCanceledException") {
      throw new OrderLockedError(orderId);   // we lost the claim - let the current owner finish
    }
    throw err;
  }
};

// --- Step 3: PUBLISH OrderCreated --------------------------------------------
// Throws on failure: safe, because the board says ORDER_SAVED and a retry
// resumes right here.
const publishOrderCreated = async (orderId, summary) => {
  const result = await eventBridge.send(
    new PutEventsCommand({
      // Exercise 9: with an EndpointId, the SDK sends the event to the global
      // endpoint (SigV4a-signed), which forwards it to the bus with this
      // EventBusName in whichever region is currently healthy. Without one
      // (secondary region), it goes straight to the local bus as before.
      EndpointId: EVENT_ENDPOINT_ID || undefined,
      Entries: [
        {
          EventBusName: EVENT_BUS_NAME,
          Source: "project4.orders",                         // WHO sent it - rules filter on this
          DetailType: "OrderCreated",                        // WHAT happened - past tense, because events are facts
          Detail: JSON.stringify({ orderId, ...summary }),   // the payload consumers receive
        },
      ],
    })
  );

  // TRAP: PutEvents does NOT throw when an entry is rejected - check the count
  if (result.FailedEntryCount > 0) {
    const entry = result.Entries[0];
    throw new Error(`OrderCreated event rejected: ${entry.ErrorCode} ${entry.ErrorMessage}`);
  }
  return result.Entries[0].EventId;
};

// --- Step 4: MARK the order COMPLETED and release the claim ------------------
const markCompleted = async (orderId, lockedUntil, eventId) => {
  try {
    await client.send(
      new UpdateCommand({
        TableName: IDEMPOTENCY_TABLE_NAME,
        Key: { orderId },
        UpdateExpression: "SET #status = :completed, publishedEventId = :eventId, completedAt = :completedAt REMOVE lockedUntil",
        ConditionExpression: "lockedUntil = :lockedUntil",
        ExpressionAttributeNames: { "#status": "status" },
        ExpressionAttributeValues: {
          ":completed": "COMPLETED",
          ":eventId": eventId,
          ":completedAt": new Date().toISOString(),
          ":lockedUntil": lockedUntil,
        },
      })
    );
  } catch (err) {
    // Our claim expired after publishing and another invocation took over - it
    // will finish. The event may go out twice; consumers are idempotent.
    if (err.name === "ConditionalCheckFailedException") {
      console.warn(JSON.stringify({ message: "Lost claim before marking COMPLETED - another invocation will finish", orderId }));
      return;
    }
    throw err;
  }
};

// --- Process ONE queued order --------------------------------------------------
const processRecord = async (record) => {
  const orderId = record.messageAttributes?.orderId?.stringValue;
  const acceptedAtMs = Number(record.messageAttributes?.acceptedAt?.stringValue);
  if (!orderId) {
    throw new Error("Message has no orderId attribute");
  }

  // Safety net only - API Gateway's JSON Schema already validated the body
  const { customerId, items } = JSON.parse(record.body);
  if (!customerId || !Array.isArray(items) || items.length === 0 || items.length > 99) {
    throw new Error("Order body is invalid");
  }

  // 1. CLAIM
  const claim = await claimOrder(orderId);
  if (claim.alreadyCompleted) {
    console.log(JSON.stringify({ message: "Duplicate - order already completed, nothing to do", orderId, publishedEventId: claim.record.publishedEventId }));
    return;
  }
  const { record: progress, lockedUntil } = claim;

  // 2. SAVE - only if not saved already
  let summary = progress.orderSummary;
  if (progress.status === "IN_PROGRESS") {
    const createdAt = Number.isFinite(acceptedAtMs) ? new Date(acceptedAtMs).toISOString() : new Date().toISOString();
    summary = {
      customerId,
      createdAt,
      itemCount: items.length,
      items: items.map((item, index) => ({
        itemId: `item-${String(index + 1).padStart(3, "0")}`,
        productId: item.productId,
        quantity: item.quantity,
        price: item.price,
      })),
    };
    await saveOrder(orderId, lockedUntil, summary);
    console.log(JSON.stringify({ message: "Order saved", orderId, itemCount: summary.itemCount }));
  } else {
    console.log(JSON.stringify({ message: "Resuming - order already saved, publishing event", orderId }));
  }

  // 3. PUBLISH (via the global endpoint in the primary region)
  const eventId = await publishOrderCreated(orderId, summary);
  console.log(JSON.stringify({ message: "OrderCreated event published", orderId, eventId, viaGlobalEndpoint: Boolean(EVENT_ENDPOINT_ID) }));

  // 4. COMPLETE
  await markCompleted(orderId, lockedUntil, eventId);
  console.log(JSON.stringify({ message: "Order completed", orderId }));
};

// --- Handler: process a BATCH of queued orders -------------------------------
export const handler = async (event) => {
  const batchItemFailures = [];

  for (const record of event.Records) {
    try {
      await processRecord(record);
    } catch (err) {
      console.error(JSON.stringify({ message: "Failed to process order message", messageId: record.messageId, error: err.name, detail: err.message }));
      batchItemFailures.push({ itemIdentifier: record.messageId });
    }
  }

  return { batchItemFailures };
};