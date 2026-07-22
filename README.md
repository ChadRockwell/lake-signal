# LakeSignal

A fully decoupled, signal-driven orchestration system for Databricks. Any process publishes completion signals to a shared Delta table; downstream jobs check if all dependencies are satisfied before proceeding.

## How It Works

```
Publishers (any language)          Jobs with Subscriber Tasks
┌──────────┐  ┌──────────┐       ┌───────────────────────────────┐
│ ZeroBus  │  │  Spark   │       │ subscriber_check              │
│  SDK     │  │  INSERT  │       │   → condition_task (ready?)   │
└────┬─────┘  └────┬─────┘       │     → main_work_tasks         │
     └──────┬──────┘              └──────────────┬────────────────┘
            ▼                                    │ reads
   ┌─────────────────────────────────────────────┴──┐
   │     <catalog>.<schema>.signal (Delta)          │
   │     append-only                                │
   └────────────────────────────────────────────────┘
```

1. **Publish** — Any pipeline, job, or service emits signals (e.g. "table X completed") to a shared Delta table via the Python SDK. Two backends: ZeroBus (no Spark required) and Direct Insert (Spark SQL).
2. **Subscribe** — Each job includes a subscriber task as its first step. The subscriber checks all new signals since the last checkpoint and returns `ready` or `not ready`. A condition task gates whether the main work proceeds.
3. **Notify** — Databricks native notification destinations handle failure alerts (Slack, Teams, email). No custom webhook code needed.

## Repository Structure

```
event_driven_orchestrator/
    lakesignal/                     # Python library (publisher SDK, subscriber framework)
    lakesignal_job_emitter/         # Optional wheel — system.lakeflow job timelines → signal table
    lakesignal_pipeline_emitter/    # Optional wheel — SDP pipeline event logs → signal table
    example/                        # Databricks Asset Bundles (subscribers, emitters, bronze loader)
```

### lakesignal/

The core library. Install via wheel. See [`lakesignal/README.md`](lakesignal/README.md) for full API docs, package structure, YAML reference (**including `variables` / `targets` and `--target`**), **audit/registry tables**, and monitoring queries.

### lakesignal_job_emitter/

Optional companion package that reads **`system.lakeflow.*`** job/task timeline tables and **INSERT**s **`job`** / **`job_task`** signals into the shared **`signal`** table. Deploy via **`example/job_emitter/`**. See [`lakesignal_job_emitter/README.md`](lakesignal_job_emitter/README.md).

### lakesignal_pipeline_emitter/

Optional companion package that reads tagged pipeline **event log** Delta tables and **INSERT**s **`table` / `<dataset>` / `completed`|`failed`** signals into the same **`signal`** table **`lakesignal`** subscribers consume. Deploy via **`example/pipeline_emitter/`**. See [`lakesignal_pipeline_emitter/README.md`](lakesignal_pipeline_emitter/README.md).

### example/

Reference Databricks Asset Bundles: subscriber patterns (inline, dispatch), optional **job/pipeline emitter** jobs, and a bronze loader. Bundles are **templates** — copy, edit targets/variables, and deploy with `databricks bundle deploy --var=...`. Includes publisher examples in Python, REST/cURL, TypeScript, and Java, plus a **setup notebook** to bootstrap the LakeSignal schema/volume and run `setup()`.

```
example/
    bronze_loader/                          # Generates fake upstream data + emits signals
    inline/                                 # Subscriber gate + pipelines in a single job
    dispatch/                               # Dispatcher job + pipeline-only target jobs
    publisher_examples/                     # Publisher examples (Python, REST, TypeScript, Java)
    setup/
        setup_notebook.ipynb               # Bootstrap schema + run setup()
```

**Inline** — one job with subscriber check, condition gate, and pipelines. Best for single-subscriber teams.
**Dispatch** — a scheduled dispatcher evaluates multiple subscriptions via `for_each_task` and triggers downstream resources (typically pipeline-only jobs via the Jobs API; also supports pipeline updates and materialized-view refresh — see the library README). Best for fan-out and multi-team setups.

## Getting Started

1. **Unity Catalog** — Ensure a catalog exists (or have an admin/creator provision one). You need permission to create a schema and managed volumes in it.
2. **Provision the signal store** — Run [`example/setup/setup_notebook.ipynb`](example/setup/setup_notebook.ipynb) in Databricks to create the schema and run **`lakesignal.setup()`** (set **`CATALOG`**, **`SCHEMA`**, and optional **`SERVICE_PRINCIPAL_ID`**). Install the **`lakesignal`** wheel separately — on **serverless environment version 4**, the wheel alone is enough (see [Setup](lakesignal/README.md#setup) in the library README). Build locally with `cd lakesignal && poetry build` (optional **`--extras zerobus`** when using ZeroBus emit). For **Poetry extras** and **`setup()`** migrations, see [Setup](lakesignal/README.md#setup). Unit tests on your laptop: `cd lakesignal && poetry install --with dev && poetry run pytest tests/unit/ -v`.
3. **Define subscriptions** — Create YAML files describing each subscriber's dependencies and optional cooldown / SLA. Optional **`metadata.tags`** (string map) and **`metadata.team`** / **`owners`** are copied into **`subscriber_registry`** and each **`lakesignal_audit`** row for filtering in SQL or a future monitor UI. See [Audit and registry](lakesignal/README.md#audit-and-registry-structured-events).

   **SLA vs cooldown:** SLA applies when upstream dependencies are still **missing**, using a wall-clock deadline. Optional `sla.breach_policy`: default **`fail`** fails the task on breach; **`warn`** does not exit 1 but still emits structured **`lakesignal_audit`** rows (`reason: sla_warn`). Cooldown applies only **after** dependencies are **satisfied**, to slow re-triggers — it does not extend SLA grace. See [SLA and cooldown](lakesignal/README.md#sla-and-cooldown) in the library README.
4. **Deploy** — Use the Databricks CLI with one of the example bundles. See [`example/README.md`](example/README.md) for per-bundle deploy instructions and end-to-end flow.

5. **(Optional) Scheduled signal emitters** — Deploy companion wheels from **`example/job_emitter/`** (all jobs via `system.lakeflow.*`) and/or **`example/pipeline_emitter/`** (DLT event logs). Build each wheel, upload to your **`lakesignal/libs/`** volume, then `databricks bundle deploy --var=...` (see [`example/README.md`](example/README.md#signal-emitters-optional)). Bundles are copy/paste templates — fork and edit for your workspace.
