"""Example: Publishing signals with the Python SDK.

Shows both ZeroBus and Direct Insert backends, SignalContext for trace_id,
signal helper, @track decorator, and cross-task trace correlation.
"""

from lakesignal.publisher import SignalContext, SignalPublisherBuilder

# ---------------------------------------------------------------------------
# Option A: ZeroBus backend (works anywhere — no Spark required)
# ---------------------------------------------------------------------------
# Secrets should come from a secrets manager (e.g. dbutils.secrets.get())
# rather than being hardcoded.

publisher = (
    SignalPublisherBuilder(catalog="catalog", schema="orchestration")
    .with_zerobus(
        server_endpoint="https://1234567890.zerobus.us-west-2.cloud.databricks.com",
        workspace_url="https://dbc-xxxxx.cloud.databricks.com",
        client_id="<service-principal-client-id>",
        client_secret="<service-principal-secret>",
    )
    .build()
)

# ---------------------------------------------------------------------------
# Option B: Direct Insert backend (requires active SparkSession)
# ---------------------------------------------------------------------------
# publisher = (
#     SignalPublisherBuilder(catalog="catalog", schema="orchestration")
#     .with_direct_insert(spark=spark)  # or omit spark= to auto-detect
#     .build()
# )

# ---------------------------------------------------------------------------
# Standalone emit — each call gets its own unique trace_id
# ---------------------------------------------------------------------------
publisher.emit(subject="table", subject_name="catalog.silver.customers", action="completed")
publisher.emit(
    subject="job",
    subject_name="nightly_export",
    action="completed",
    metadata={"logical_date": "2025-01-15", "rows_exported": 42000},
)

# ---------------------------------------------------------------------------
# Explicit SignalContext — share trace_id across emits
# ---------------------------------------------------------------------------
ctx = SignalContext()
publisher.emit("table", "catalog.silver.customers", "completed", context=ctx)
publisher.emit("table", "catalog.silver.orders", "completed", context=ctx)

# ---------------------------------------------------------------------------
# Pipeline helper with SignalContext — all emits share the same trace_id
# ---------------------------------------------------------------------------
ctx = SignalContext()
pipeline = publisher.pipeline("silver", context=ctx)
pipeline.started()
try:
    publisher.emit("table", "catalog.silver.customers", "completed", context=ctx)
    publisher.emit("table", "catalog.silver.orders", "completed", context=ctx)
    publisher.emit("table", "catalog.silver.transactions", "completed", context=ctx)
    pipeline.completed()
except Exception as e:
    pipeline.failed(metadata={"error": str(e)})
    raise

# ---------------------------------------------------------------------------
# @track decorator — same trace_id semantics, less boilerplate
# ---------------------------------------------------------------------------
@publisher.track("pipeline", "silver")
def run_silver_pipeline():
    publisher.emit("table", "catalog.silver.customers", "completed")
    publisher.emit("table", "catalog.silver.orders", "completed")
    publisher.emit("table", "catalog.silver.transactions", "completed")


run_silver_pipeline()

# ---------------------------------------------------------------------------
# Cross-task trace correlation (Databricks multi-task jobs)
# ---------------------------------------------------------------------------
# Upstream task: create context and share trace_id via task values
# ctx = SignalContext()
# publisher.emit("pipeline", "silver", "completed", context=ctx)
# dbutils.jobs.taskValues.set(key="trace_id", value=ctx.trace_id_str)

# Downstream task: receive trace_id and continue the trace
# from uuid import UUID
# trace_id = UUID(dbutils.jobs.taskValues.get(taskKey="upstream_task", key="trace_id"))
# ctx = SignalContext(trace_id=trace_id)
# publisher.emit("table", "catalog.gold.report", "completed", context=ctx)

# ---------------------------------------------------------------------------
# Flush — ensure all pending writes are acknowledged without closing
# ---------------------------------------------------------------------------
# Useful for checkpointing in long-running processes. No-op for Direct Insert.
publisher.emit("table", "catalog.silver.customers", "completed")
publisher.emit("table", "catalog.silver.orders", "completed")
publisher.flush()  # all signals above are now durably written

# ---------------------------------------------------------------------------
# Context manager for resource cleanup (ZeroBus: flushes + closes stream)
# ---------------------------------------------------------------------------
with publisher:
    publisher.emit("pipeline", "silver", "completed")
# Stream / connection is closed automatically
