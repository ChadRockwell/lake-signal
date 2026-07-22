"""Entry point for subscriber tasks.

The subscriber checks whether signal dependencies are met and sets task values
so a downstream condition_task can gate the main work.

Exit code semantics (designed for Databricks job alerting):
  - exit 0: dependencies not yet met (ready=false) OR dependencies met (ready=true).
    Job succeeds in both cases — the downstream condition_task reads the "ready"
    task value to decide whether to proceed. No notifications fire.
  - exit 1: SLA breach — deadline passed without completeness. Task fails,
    which triggers Databricks on_failure notifications (email, PagerDuty, etc.).

This means "dependencies not met" is a silent, expected state — the job
simply ends and will be re-evaluated on the next scheduled run. Only an SLA
breach is treated as an actionable failure that pages on-call.

Usage (as a Databricks job task):
    spark_python_task:
      python_file: run_subscriber.py
      parameters:
        - ${var.catalog}
        - ${var.schema}
        - /Workspace/${workspace.root_path}/team_analytics.subscription.yml
        - "{{job.parameters.force_ready}}"
"""

import logging
import sys

from lakesignal.subscriber import Subscriber

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    if len(sys.argv) < 4:
        print("Usage: run_subscriber.py <catalog> <schema> <subscription_yaml_path> [force_ready]", file=sys.stderr)
        sys.exit(1)

    catalog = sys.argv[1]
    schema = sys.argv[2]
    subscription_path = sys.argv[3]
    force_ready = sys.argv[4].lower() == "true" if len(sys.argv) > 4 else False

    # Force-ready override: skip all subscription evaluation, set ready=true,
    # return immediately.  Used during job repairs to force downstream tasks.
    if force_ready:
        logger.info("force_ready=true — bypassing subscription evaluation")
        try:
            from pyspark.sql import SparkSession
            from pyspark.dbutils import DBUtils

            spark = SparkSession.builder.getOrCreate()
            dbutils = DBUtils(spark)
            dbutils.jobs.taskValues.set(key="ready", value="true")
            logger.info("Task value set: ready=true (forced)")
        except Exception:
            logger.warning("Could not set task values (non-Databricks environment)")
        return

    logger.info("Starting subscriber from config: %s (catalog=%s, schema=%s)", subscription_path, catalog, schema)

    subscriber = Subscriber.from_config(subscription_path, catalog=catalog, schema=schema)
    result = subscriber.run()

    # SLA breach → fail the task so Databricks on_failure notifications fire
    if result.sla_breached:
        logger.error("SLA breached for %s: %s", result.subscriber_name, result.message)
        sys.exit(1)

    # Set task values for the downstream condition_task to read
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)

        dbutils.jobs.taskValues.set(key="ready", value=str(result.ready).lower())
        if result.run_id:
            dbutils.jobs.taskValues.set(key="run_id", value=result.run_id)
        dbutils.jobs.taskValues.set(key="subscriber_name", value=result.subscriber_name)
        logger.info("Task values set: ready=%s, run_id=%s", result.ready, result.run_id)
    except Exception:
        logger.warning("Could not set task values (non-Databricks environment). Result: %s", result)


if __name__ == "__main__":
    main()
