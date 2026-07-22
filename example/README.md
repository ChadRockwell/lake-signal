# Example — LakeSignal Subscriber Deployments

Reference Databricks Asset Bundles showing how to use `lakesignal` subscribers as dependency gates for pipeline orchestration.

- **`bronze_loader/`** — generates fake upstream data and emits **pipeline** `started` / `completed` signals
- **`inline/`** — subscriber check + condition gate + pipelines in each job (two sub-bundles: silver, gold)
- **`dispatch/`** — central dispatcher plus pipeline-only target jobs (three sub-bundles: dispatcher, silver, gold)

## Choosing a Pattern

### Inline

Each job contains its own subscriber check, a condition gate, and its pipelines. The subscriber evaluates upstream dependencies on every scheduled run; if ready, the pipelines proceed and emit `started` / `completed` signals. If not, the job ends cleanly at the condition gate.

**Best for:** teams that own their own jobs end-to-end. Each job is self-contained — one subscription, one pipeline, one DAG. No central dispatcher needed.

```
inline/silver:  subscriber_check → check_ready → emit_started → silver_pipeline → emit_completed
inline/gold:    subscriber_check → check_ready → emit_started → gold_pipeline  → emit_completed
```

Gold subscribes to silver's `completed` signal, creating a chain: `bronze → silver → gold`.

### Dispatch

A central dispatcher job evaluates multiple subscriptions using `for_each_task`. When a subscriber is ready, the dispatcher triggers downstream work configured in each subscription’s `trigger` key (typically a pipeline-only **job** via the Jobs API; the library can also start pipeline updates or refresh a materialized view). Pipeline-only jobs have no schedule — they run only when triggered.

**Best for:** multi-subscriber setups, fan-out (one signal triggers multiple pipelines), and reducing no-op job clutter. The dispatcher is the only scheduled job; pipeline jobs don't run at all when dependencies aren't met.

The dispatch example uses **three separate sub-bundles** to simulate real-world team ownership:

```
dispatch/dispatcher  (ops team — scheduled, evaluates subscriptions)
  → dispatch/silver  (analytics team — pipeline-only job, no schedule)
  → dispatch/gold    (data products team — pipeline-only job, no schedule)
```

Each team deploys their own bundle independently. The dispatcher's subscription YAMLs reference the pipeline-only jobs by name via `trigger`.

### Dispatch `trigger` (subscription YAML)

The old top-level `trigger_job` key is replaced by a `trigger` block with up to three resource types. At least one is required for `lakesignal-dispatch`. The dispatcher fires them in order: **job → pipeline → materialized_view**.

| Key | Value | API used |
|-----|-------|----------|
| `job` | Job name (string) or `{ name, parameters? }` | Jobs API `run_now` |
| `pipeline` | Pipeline name (string) or `{ name, full_refresh? }` | Pipelines API `start_update` |
| `materialized_view` | FQN `catalog.schema.view` (string or `{ name }`) | UC Tables API → backing pipeline update |

```yaml
# Example dispatch subscriptions (see dispatch/dispatcher/subscriptions/)
trigger:
  job: ${var.job_name}                    # string shorthand

# Expanded forms:
# trigger:
#   job:
#     name: my_pipeline_job
#     parameters:
#       env: prod
#   pipeline: my_dlt_pipeline
#   materialized_view: prod_catalog.reporting.daily_summary
```

The dispatch example uses **`trigger.job`** because each team's pipeline-only job also runs **`lakesignal-emit`** tasks (pipeline `started` / `completed` signals for downstream subscribers). Use **`trigger.pipeline`** when you only need a DLT update and do not need custom signal emission in the same run.

Pass **`target: ${bundle.target}`** on the dispatcher wheel task so `${var.*}` substitution and DABs development-mode resource name prefixes resolve correctly. Subscription YAML **`targets`** blocks must include every bundle target you deploy (e.g. `dev` and `prod`).

## Prerequisites

- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) configured with authentication
- A Databricks workspace with Unity Catalog enabled, and permission to create a schema, UC volume, and tables in your target catalog(s)
- A [notification destination](https://docs.databricks.com/en/admin/notification-destinations/index.html) configured for Slack, Teams, or email (for failure alerts)

## Setup

### 1. Create the signal store

Follow [`setup/setup_notebook.ipynb`](setup/setup_notebook.ipynb) — it bootstraps the schema and runs **`lakesignal.setup()`** to create all tables via versioned migrations. Idempotent — safe to re-run. Install the **`lakesignal`** wheel before the `setup()` cell (see step 2).

### 2. Build and deploy the wheel

```bash
cd ../lakesignal
poetry install
poetry build
```

**Serverless environment version 4** (recommended): `pydantic`, `PyYAML`, and `databricks-sdk` are pre-installed. Upload only the **`lakesignal`** wheel — no dependency bundle required for inline/subscribe/emit/dispatch.

```yaml
# Job environment snippet (see lakesignal/lakesignal_serverless_environment.yaml)
environment_version: "4"
dependencies:
  - /Workspace/Shared/LakeSignal/lakesignal-0.1.0-py3-none-any.whl
```

Notebook install:

```python
%pip install /Workspace/Shared/LakeSignal/lakesignal-0.1.0-py3-none-any.whl
%restart_python
```

Poetry **extras**:

- **`--extras zerobus`** — ZeroBus emit backend (`databricks-zerobus-ingest-sdk`; not pre-installed on serverless)
- **`--extras dispatch`** — only needed when **`databricks-sdk`** is not already on the runtime (it **is** on serverless v4)

Example jobs in this repo still reference a UC volume wheel path — adjust to your workspace layout:

```bash
databricks fs cp dist/lakesignal-0.1.0-py3-none-any.whl \
  dbfs:/Volumes/<catalog>/<schema>/lakesignal/libs/lakesignal-0.1.0-py3-none-any.whl
```

### 3. Deploy bundles

Each bundle defines `dev` (default) and `prod` targets. The `dev` target uses `mode: development` (prefixes resource names with your username to avoid collisions). The `prod` target sets `mode: production` with default variable values.

Inline and dispatch use distinct name prefixes (`lakesignal_example_inline_*` vs `lakesignal_example_dispatch_*`), so both can be deployed to the same workspace without collisions. The bronze loader can be deployed alongside either or both.

#### Bronze Loader (deploy first)

Creates synthetic upstream tables (`customers_raw`, `orders_raw`, `compliance_events`) in **one** Lakeflow pipeline (bundle resource `bronze_ingest`; display **`name`** `lakesignal_example_bronze_ingest`) and emits **`pipeline` / `lakesignal_example_bronze_ingest` / `started` → run → `completed`** (no per-table signals). Deploy this before the subscriber bundles so the upstream data exists.

- **`catalog` / `schema`** — UC location for the bronze pipeline output (all tables land in this catalog + schema).
- **`orchestration_catalog` / `orchestration_schema`** — UC location for the LakeSignal store (volume + signal tables); used by `lakesignal-emit` and wheel paths.

Subscribers in **`inline/`** and **`dispatch/`** use **`subject: pipeline`** and **`subject_name: ${var.catalog}.${var.schema}.<pipeline_name>`** — the fully-qualified pipeline name including catalog and schema. The subscription variables (`bronze_catalog`/`bronze_schema` or `silver_catalog`/`silver_schema`) must resolve to the same values as the publisher's `catalog`/`schema` so the `subject_name` matches. In **`mode: development`**, Databricks may alter published resource names; align **`subject_name`** with the **deployed** pipeline name if subscribers never go ready.

```bash
cd bronze_loader

# Dev (default target — uses dev_catalog)
databricks bundle deploy

# Prod (uses prod_catalog)
databricks bundle deploy -t prod
```

Run the job manually to populate tables and emit signals:

```bash
databricks bundle run bronze_loader
```

#### Inline (2 sub-bundles — deploy silver first, then gold)

```bash
# Silver (subscribes to bronze)
cd inline/silver
databricks bundle deploy          # dev
databricks bundle deploy -t prod  # prod

# Gold (subscribes to silver)
cd ../gold
databricks bundle deploy          # dev
databricks bundle deploy -t prod  # prod
```

#### Dispatch (3 sub-bundles — deploy pipeline bundles first, then dispatcher)

```bash
# Silver team bundle
cd dispatch/silver
databricks bundle deploy          # dev
databricks bundle deploy -t prod  # prod

# Gold team bundle
cd ../gold
databricks bundle deploy          # dev
databricks bundle deploy -t prod  # prod

# Dispatcher (deploy last — references silver/gold jobs by name)
cd ../dispatcher
databricks bundle deploy          # dev
databricks bundle deploy -t prod  # prod
```

#### Environment Isolation

Each bundle defines `dev` and `prod` targets with separate `catalog` / `orchestration_catalog` values. This gives each environment its own signal store — dev signals never reach prod subscribers, and vice versa. Subscription YAML files contain no environment-specific content and work across all targets unchanged.

See [`../docs/environment_isolation.md`](../docs/environment_isolation.md) for the full strategy: UC topology options, permissions, cross-env monitoring, and migration guidance.

### 4. End-to-end flow

1. Run the bronze loader job — populates bronze tables, emits **`lakesignal_example_bronze_ingest` pipeline** `started` / `completed` signals
2. The subscriber bundle (inline or dispatch) picks up the signals on its next scheduled run
3. Pipelines execute when all subscription criteria are met

For custom signal emission, see the publisher examples in [`publisher_examples/`](publisher_examples/).

## Signal emitters (optional)

Companion wheels that **feed the shared `signal` table** on a schedule. Each has a Databricks Asset Bundle under **`example/`** — use as-is, or **copy the folder as a template** and modify `databricks.yml`, job YAML, wheel path, and schedule for your workspace.

| Bundle | Package | What it emits |
|--------|---------|----------------|
| [`job_emitter/`](job_emitter/) | [`lakesignal_job_emitter`](../lakesignal_job_emitter/README.md) | **`job`** / **`job_task`** signals from `system.lakeflow.*` |
| [`pipeline_emitter/`](pipeline_emitter/) | [`lakesignal_pipeline_emitter`](../lakesignal_pipeline_emitter/README.md) | **`table`** signals from DLT **`flow_progress`** event logs |

### Build wheels

```bash
cd lakesignal_job_emitter && poetry install && poetry build
cd lakesignal_pipeline_emitter && poetry install && poetry build
```

Upload each wheel to **`/Volumes/<catalog>/<schema>/lakesignal/libs/`** (adjust version in the bundle `resources/*.yml` after bumping).

### Deploy with variables

Bundles ship **placeholder targets** (`dev_catalog`, `prod_catalog`, `orchestration`). Pass real values with **`--var`** so workspace-specific config stays out of git.

**Job emitter** — all workspace jobs (optional workspace filter). Tag a job with **`lakesignal.exclude_auto_emit=true`** to skip auto-emit when it uses manual **`lakesignal-emit`** instead (see [`lakesignal_job_emitter/README.md`](../lakesignal_job_emitter/README.md#opt-out-of-auto-emit-manual-signals)).

```bash
cd example/job_emitter

databricks bundle deploy -t dev \
  --var="lakesignal_catalog=my_catalog" \
  --var="lakesignal_schema=orchestration"

# Optional: comma-separated workspace IDs; omit for all workspaces
databricks bundle deploy -t dev \
  --var="lakesignal_catalog=my_catalog" \
  --var="lakesignal_schema=orchestration" \
  --var='workspace_ids=1234567890123456'
```

**Pipeline emitter** — tagged event-log tables:

```bash
cd example/pipeline_emitter

databricks bundle deploy -t dev \
  --var="lakesignal_catalog=my_catalog" \
  --var="lakesignal_schema=orchestration" \
  --var="env=dev"
```

Alternatively, edit **`targets:`** blocks in each bundle’s `databricks.yml` for fixed dev/prod values (keep personal workspace IDs local or gitignored).

### Using as a template

1. Copy **`example/job_emitter/`** or **`example/pipeline_emitter/`** to your deployment repo.
2. Update **`resources/*_job.yml`**: wheel path/version, cron, `pause_status`, serverless `environment_version`.
3. Customize **`databricks.yml`**: bundle name, target variables, add staging targets.
4. Add **`--event_log_tables`** or other entry-point flags in `named_parameters` if needed (pipeline emitter).

Package READMEs document entry-point arguments and signal shapes in detail.

## How It Works

The three subscriber states map to Databricks job behavior for **`lakesignal-subscribe`** / **`lakesignal-dispatch`**:

| State | What happens |
|-------|-------------|
| **Ready** | Inline: condition passes, pipelines run. Dispatch: configured trigger(s) fired (job and/or pipeline / MV refresh). |
| **Not ready** | Inline: condition skips, job ends cleanly. Dispatch: no-op, nothing triggered. |
| **SLA breach** (`breach_policy: fail`) | Subscriber task exits 1, `on_failure` notifications fire. |

Each run also MERGEs **`subscriber_registry`** and appends rows to **`lakesignal_audit`** (evaluations, `run_created`, dispatch trigger/failure, `force_ready`, etc.) for SQL-based visibility. Events from one attempt share a **`correlation_id`**; payloads can include **`databricks_job_run_id`** and optional **`trace_id`**. Details: [`lakesignal/README.md`](../lakesignal/README.md#audit-and-registry-structured-events).

## Directory Structure

```
example/
├── README.md
├── run_subscriber.py                            # Standalone entry point (reference)
├── setup/
│   └── setup_notebook.ipynb
├── publisher_examples/
│   ├── python_publisher.py
│   ├── rest_publisher.sh
│   ├── typescript_publisher.ts
│   └── java_publisher.java
│
├── bronze_loader/                               # Upstream data generator + signal emitter
│   ├── databricks.yml
│   ├── resources/
│   │   ├── bronze_loader_job.yml
│   │   └── bronze_ingest_pipeline.yml
│   └── transformations/
│       ├── bronze_customers.sql
│       ├── bronze_orders.sql
│       └── bronze_compliance_events.sql
│
├── job_emitter/                                 # lakesignal_job_emitter deploy bundle
│   ├── databricks.yml
│   └── resources/
│       └── job_emitter_job.yml
│
├── pipeline_emitter/                            # lakesignal_pipeline_emitter deploy bundle
│   ├── databricks.yml
│   └── resources/
│       └── pipeline_emitter_job.yml
│
├── inline/                                      # Inline pattern (2 sub-bundles)
│   ├── silver/                                  # Silver team bundle
│   │   ├── databricks.yml
│   │   ├── resources/
│   │   │   ├── silver_job.yml
│   │   │   └── silver_pipeline.yml
│   │   ├── subscriptions/
│   │   │   └── silver.subscription.yml
│   │   └── transformations/
│   │       ├── silver_customers.sql
│   │       └── silver_orders.sql
│   │
│   └── gold/                                    # Gold team bundle
│       ├── databricks.yml
│       ├── resources/
│       │   ├── gold_job.yml
│       │   └── gold_pipeline.yml
│       ├── subscriptions/
│       │   └── gold.subscription.yml
│       └── transformations/
│           └── gold_datamart.sql
│
└── dispatch/                                    # Dispatch pattern (3 sub-bundles)
    ├── dispatcher/                              # Central dispatcher (ops team)
    │   ├── databricks.yml
    │   ├── resources/
    │   │   └── subscription_dispatcher_job.yml
    │   └── subscriptions/
    │       ├── silver.subscription.yml
    │       └── gold.subscription.yml
    │
    ├── silver/                                  # Silver team bundle
    │   ├── databricks.yml
    │   ├── resources/
    │   │   ├── silver_job.yml
    │   │   └── silver_pipeline.yml
    │   └── transformations/
    │       ├── silver_customers.sql
    │       └── silver_orders.sql
    │
    └── gold/                                    # Gold team bundle
        ├── databricks.yml
        ├── resources/
        │   ├── gold_job.yml
        │   └── gold_pipeline.yml
        └── transformations/
            └── gold_datamart.sql
```
