/**
 * Example: Publish events using the ZeroBus Java SDK.
 *
 * Maven dependency:
 *   <dependency>
 *     <groupId>com.databricks</groupId>
 *     <artifactId>zerobus-ingest-sdk</artifactId>
 *     <version>1.1.0</version>
 *   </dependency>
 */

import com.databricks.zerobus.ZerobusSdk;
import com.databricks.zerobus.ZerobusJsonStream;

import java.time.Instant;
import java.util.UUID;

public class java_publisher {

    private static final String SERVER_ENDPOINT = "https://1234567890123456.zerobus.us-west-2.cloud.databricks.com";
    private static final String WORKSPACE_URL = "https://dbc-xxxxx.cloud.databricks.com";
    private static final String TABLE_NAME = "catalog.orchestration.events";
    private static final String CLIENT_ID = "<service-principal-client-id>";
    private static final String CLIENT_SECRET = "<service-principal-secret>";

    public static void main(String[] args) throws Exception {
        ZerobusSdk sdk = new ZerobusSdk(SERVER_ENDPOINT, WORKSPACE_URL);
        ZerobusJsonStream stream = sdk.createJsonStream(TABLE_NAME, CLIENT_ID, CLIENT_SECRET).join();

        try {
            // Standalone event — unique trace_id
            publishEvent(stream, "table", "catalog.silver.customers", "completed", null, null);

            // Correlated events sharing a trace_id
            String traceId = UUID.randomUUID().toString();
            publishEvent(stream, "pipeline", "silver", "started", null, traceId);
            publishEvent(stream, "table", "catalog.silver.customers", "completed", null, traceId);
            publishEvent(stream, "table", "catalog.silver.orders", "completed", null, traceId);
            publishEvent(stream, "pipeline", "silver", "completed",
                    "{\"logical_date\": \"2025-01-15\"}", traceId);
        } finally {
            stream.close();
        }
    }

    private static void publishEvent(
            ZerobusJsonStream stream,
            String subject,
            String subjectName,
            String action,
            String metadata,
            String traceId
    ) throws Exception {
        String eventId = UUID.randomUUID().toString();
        if (traceId == null) {
            traceId = UUID.randomUUID().toString();
        }
        String timestamp = Instant.now().toString();

        String record = String.format(
                "{\"event_id\": \"%s\", \"trace_id\": \"%s\", \"subject\": \"%s\", " +
                "\"subject_name\": \"%s\", \"action\": \"%s\", \"event_timestamp\": \"%s\", " +
                "\"metadata\": %s}",
                eventId, traceId, subject, subjectName, action, timestamp,
                metadata != null ? "\"" + metadata.replace("\"", "\\\"") + "\"" : "null"
        );

        long offset = stream.ingestRecordOffset(record);
        stream.waitForOffset(offset);
        System.out.printf("Event published: %s (trace: %s)%n", eventId, traceId);
    }
}
