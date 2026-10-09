// =============================================================================
// FulfilOrder Lambda
// Triggered by: SQS (project4-fulfilment-queue), in batches of up to 10
// Does: for each OrderCreated message -> record the order as awaiting shipment
// =============================================================================

import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand } from "@aws-sdk/lib-dynamodb";

// --- Initialisation (once per container, reused across invocations) ---------
const client = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const FULFILMENTS_TABLE_NAME = process.env.FULFILMENTS_TABLE_NAME;

// --- Process ONE message -----------------------------------------------------
// Throws if the message can't be processed, so the handler can mark it as failed.
const processRecord = async (record) => {
  // When EventBridge delivers to SQS, the message body is the WHOLE event
  // envelope (id, source, detail-type, detail...) as a JSON string.
  const event = JSON.parse(record.body);
  const { orderId, customerId, itemCount, items } = event.detail;

  if (!orderId) {
    throw new Error("Message has no detail.orderId");
  }

  try {
    await client.send(
      new PutCommand({
        TableName: FULFILMENTS_TABLE_NAME,
        Item: {
          orderId,
          customerId,
          itemCount,
          items,
          status: "PENDING_SHIPMENT",          // first stage of fulfilment
          eventId: event.id,                   // which event created this - useful when debugging
          receivedAt: new Date().toISOString(),
        },
        // Only write if this order isn't already recorded.
        ConditionExpression: "attribute_not_exists(orderId)",
      })
    );
    console.log(JSON.stringify({ message: "Order recorded for fulfilment", orderId, eventId: event.id }));
  } catch (err) {
    // SQS delivers "at least once", so the same message can arrive twice.
    // If the row already exists, this message was already processed - that's a
    // success, not an error. Swallowing it here makes the function idempotent.
    if (err.name === "ConditionalCheckFailedException") {
      console.log(JSON.stringify({ message: "Duplicate message ignored - order already recorded", orderId }));
      return;
    }
    throw err;   // any other error (throttling, permissions...) -> retry this message
  }
};

// --- Handler: process a BATCH of messages --------------------------------------
export const handler = async (event) => {
  const batchItemFailures = [];

  for (const record of event.Records) {
    try {
      await processRecord(record);
    } catch (err) {
      console.error(JSON.stringify({ message: "Failed to process message", messageId: record.messageId, error: err.name, detail: err.message }));
      // Report just THIS message as failed. Because ReportBatchItemFailures is on,
      // SQS retries only the failed messages and deletes the successful ones.
      batchItemFailures.push({ itemIdentifier: record.messageId });
    }
  }

  // Empty list = whole batch succeeded, so SQS deletes every message.
  return { batchItemFailures };
};