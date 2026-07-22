/**
 * Example: Publish events using the ZeroBus TypeScript SDK.
 *
 * Install: npm install @databricks/zerobus-ingest-sdk
 */

import { ZerobusSdk, RecordType } from "@databricks/zerobus-ingest-sdk";

const SERVER_ENDPOINT =
  "https://1234567890123456.zerobus.us-west-2.cloud.databricks.com";
const WORKSPACE_URL = "https://dbc-xxxxx.cloud.databricks.com";
const TABLE_NAME = "catalog.orchestration.events";
const CLIENT_ID = "<service-principal-client-id>";
const CLIENT_SECRET = "<service-principal-secret>";

async function publishEvent(
  subject: string,
  subjectName: string,
  action: string,
  metadata?: Record<string, unknown>,
  traceId?: string
): Promise<void> {
  const sdk = new ZerobusSdk(SERVER_ENDPOINT, WORKSPACE_URL);
  const stream = await sdk.createStream(
    { tableName: TABLE_NAME },
    CLIENT_ID,
    CLIENT_SECRET,
    { recordType: RecordType.Json }
  );

  try {
    const record = {
      event_id: crypto.randomUUID(),
      trace_id: traceId ?? crypto.randomUUID(),
      subject,
      subject_name: subjectName,
      action,
      event_timestamp: new Date().toISOString(),
      metadata: metadata ? JSON.stringify(metadata) : null,
    };

    const offset = await stream.ingestRecordOffset(record);
    await stream.waitForOffset(offset);
    console.log(`Event published: ${record.event_id} (trace: ${record.trace_id})`);
  } finally {
    await stream.close();
  }
}

// --- Usage ---

// Standalone event (gets its own trace_id)
await publishEvent("table", "catalog.silver.customers", "completed");

// Correlated events sharing a trace_id (simulating a pipeline run)
const traceId = crypto.randomUUID();
await publishEvent("pipeline", "silver", "started", undefined, traceId);
await publishEvent("table", "catalog.silver.customers", "completed", undefined, traceId);
await publishEvent("table", "catalog.silver.orders", "completed", undefined, traceId);
await publishEvent(
  "pipeline",
  "silver",
  "completed",
  { logical_date: "2025-01-15" },
  traceId
);
