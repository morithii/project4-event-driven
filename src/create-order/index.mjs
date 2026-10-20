// =============================================================================
// CreateOrder Lambda  (idempotent - Exercise 8)
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
// A message that arrives again (SQS retry, or a duplicate delivery) reads the
// board and RESUMES from the missing step:
//   COMPLETED   -> nothing to do (duplicate)
//   ORDER_SAVED -> skip saving, just publish the event
//   IN_PROGRESS -> claimed by another invocation right now -> retry later
// =============================================================================

import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, TransactWriteCommand, UpdateCommand, GetCommand } from "@aws-sdk/lib-dynamodb";
import { EventBridgeClient, PutEventsCommand } from "@aws-sdk/client-eventbridge";

// --- Initialisation (runs ONCE per container, not on every message) ---------
const client = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const eventBridge = new EventBridgeClient({});
const TABLE_NAME = process.env.TABLE_NAME;
const IDEMPOTENCY_TABLE_NAME = process.env.IDEMPOTENCY_TABLE_NAME;
const EVENT_BUS_NAME = process.env.EVENT_BUS_NAME;

// How long a claim ("lock") lasts. Longer than the Lambda timeout (10s), so a
// lock can't expire while its owner is still working. Shorter than the queue's
// visibility timeout (60s), so by the time SQS retries a message, any lock left
// behind by a crashed invocation has already expired.
const LOCK_MS = 30_000;

// Board rows are deleted automatically (DynamoDB TTL) after this many days
const RECORD_TTL_DAYS = 7;

// Thrown when another invocation is working on the same order. It's treated
// as a normal failure, so SQS simply retries the message a minute later.
class OrderLockedError extends Error {
  constructor(orderId) {
    super(`Order ${orderId} is being processed by another invocation - retry later`);
    this.name = "OrderLockedError";
  }
}

// --- Step 1: CLAIM the order on the progress board ---------------------------
// One conditional write does the "check and claim" atomically, which is what
// prevents race conditions: if two Lambdas get the same message at the same
// moment, DynamoDB lets exactly ONE of them succeed.
//
// The claim succeeds when EITHER:
//   - the order isn't on the board yet (first time we've seen it), OR
//   - it isn't COMPLETED and the previous claim has expired (an earlier
//     attempt crashed or timed out part-way - we take over and resume)
const claimOrder = async (orderId) => {
  const now = Date.now();
  const lockedUntil = now + LOCK_MS;

  try {
    const { Attributes } = await client.send(
      new UpdateCommand({
        TableName: IDEMPOTENCY_TABLE_NAME,
        Key: { orderId },
        // if_not_exists(...) keeps an existing value - so a resumed order keeps
        // its ORDER_SAVED status and its original expiry time
        UpdateExpression:
          "SET lockedUntil = :lockedUntil, #status = if_not_exists(#status, :inProgress), expiresAt = if_not_exists(expiresAt, :expiresAt)",
        ConditionExpression: "attribute_not_exists(orderId) OR (#status <> :completed AND lockedUntil < :now)",
        // "status" is a reserved word in DynamoDB, so it must be referenced
        // through a placeholder (#status) instead of written directly
        ExpressionAttributeNames: { "#status": "status" },
        ExpressionAttributeValues: {
          ":lockedUntil": lockedUntil,
          ":inProgress": "IN_PROGRESS",
          ":expiresAt": Math.floor(now / 1000) + RECORD_TTL_DAYS * 24 * 60 * 60,   // TTL uses epoch SECONDS
          ":completed": "COMPLETED",
          ":now": now,
        },
        ReturnValues: "ALL_NEW",   // give back the row as it is now, so we know where to resume
      })
    );
    return { record: Attributes, lockedUntil };
  } catch (err) {
    if (err.name !== "ConditionalCheckFailedException") throw err;

    // The claim was refused. Read the row to find out why.
    // ConsistentRead = read the very latest write, not a possibly stale copy.
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
// Both happen together or not at all, so the board can never say ORDER_SAVED
// while the order is missing - or the other way round.
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
      // Store the order summary too - this is the "existing result" a retry
      // reuses to publish the event, without rebuilding anything
      UpdateExpression: "SET #status = :saved, orderSummary = :summary",
      // Only if WE still hold the claim. If our lock expired and another
      // invocation took over, this fails and the whole transaction is cancelled.
      ConditionExpression: "lockedUntil = :lockedUntil",
      ExpressionAttributeNames: { "#status": "status" },
      ExpressionAttributeValues: { ":saved": "ORDER_SAVED", ":summary": summary, ":lockedUntil": lockedUntil },
    },
  };

  try {
    await client.send(new TransactWriteCommand({ TransactItems: [...orderRows, markSaved] }));
  } catch (err) {
    if (err.name === "TransactionCanceledException") {
      // We lost the claim part-way - let the current owner finish the job
      throw new OrderLockedError(orderId);
    }
    throw err;
  }
};

// --- Step 3: PUBLISH OrderCreated --------------------------------------------
// Unlike before, a failure here now THROWS. That's safe: the board says
// ORDER_SAVED, so the retry skips straight to publishing - the event is no
// longer lost (the gap noted in Exercise 6 is closed).
const publishOrderCreated = async (orderId, summary) => {
  const result = await eventBridge.send(
    new PutEventsCommand({
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
        ConditionExpression: "lockedUntil = :lockedUntil",   // only if we still hold the claim
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
    // Our claim expired after we published, and another invocation took over.
    // It will publish and finish - the event may go out twice, which is why
    // every consumer must be idempotent too (FulfilOrder already is).
    if (err.name === "ConditionalCheckFailedException") {
      console.warn(JSON.stringify({ message: "Lost claim before marking COMPLETED - another invocation will finish", orderId }));
      return;
    }
    throw err;
  }
};

// --- Process ONE queued order --------------------------------------------------
const processRecord = async (record) => {
  // READ the IDs API Gateway attached as message attributes
  const orderId = record.messageAttributes?.orderId?.stringValue;
  const acceptedAtMs = Number(record.messageAttributes?.acceptedAt?.stringValue);
  if (!orderId) {
    throw new Error("Message has no orderId attribute");
  }

  // PARSE the order. API Gateway's JSON Schema already validated it; this is
  // only a safety net in case something else ever writes to the queue.
  const { customerId, items } = JSON.parse(record.body);
  if (!customerId || !Array.isArray(items) || items.length === 0 || items.length > 99) {
    throw new Error("Order body is invalid");
  }

  // 1. CLAIM - and find out where this order got to last time (if anywhere)
  const claim = await claimOrder(orderId);
  if (claim.alreadyCompleted) {
    // "Return the existing result": nothing is re-saved or re-published
    console.log(JSON.stringify({ message: "Duplicate - order already completed, nothing to do", orderId, publishedEventId: claim.record.publishedEventId }));
    return;
  }
  const { record: progress, lockedUntil } = claim;

  // 2. SAVE - only if it hasn't been saved already
  let summary = progress.orderSummary;   // present if an earlier attempt saved the order
  if (progress.status === "IN_PROGRESS") {
    // createdAt = when the CUSTOMER placed it (API Gateway's timestamp)
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

  // 3. PUBLISH (throws on failure -> SQS retries -> resumes right here)
  const eventId = await publishOrderCreated(orderId, summary);
  console.log(JSON.stringify({ message: "OrderCreated event published", orderId, eventId }));

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
      batchItemFailures.push({ itemIdentifier: record.messageId });   // retry just this one
    }
  }

  // This return value is for SQS: which messages to retry
  return { batchItemFailures };
};