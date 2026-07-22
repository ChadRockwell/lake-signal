# Environment Isolation for LakeSignal

Unity Catalog can span multiple environments (dev, staging, prod). When multiple environments emit signals to the same signal store, "mixed" signals can cause subscribers to incorrectly trigger pipelines — e.g., a prod subscriber acting on a dev signal. This guide covers how to isolate environments using separate signal stores.

## Strategy: Separate Signal Stores Per Environment

LakeSignal uses separate `catalog.schema` combinations per environment. Every component already accepts `catalog` and `schema` as parameters, so no code changes are required — environment isolation is purely a configuration concern.

**Why not an environment column in the signal table?**

- Breaking schema change + migration for all publishers, subscribers, emitters
- Stream reads all environments then filters — wasted I/O
- `once` freshness mode could "spend" a dev signal against a prod run if filtering is missed
- Weaker permission model (can't use UC table/schema-level access)
- Risk of cross-env contamination from misconfigured flag

## UC Topology Options

### Topology A: Separate Catalogs (Recommended)

Each environment gets its own catalog. Schema name stays consistent.

```
dev_catalog.orchestration.signal     → dev signals
staging_catalog.orchestration.signal → staging signals
prod_catalog.orchestration.signal    → prod signals
```

Strongest isolation — catalog-level permissions prevent cross-env access entirely.

### Topology B: Shared Catalog, Environment Schemas

One catalog with environment encoded in the schema name.

```
shared_catalog.orchestration_dev.signal     → dev signals
shared_catalog.orchestration_staging.signal → staging signals
shared_catalog.orchestration.signal         → prod signals
```

Appropriate when the org uses a single catalog or needs easier cross-env queries.

## DAB Multi-Target Configuration

All DAB bundles use `targets` with per-env variable values. The `dev` target is the default; deploy to prod with `databricks bundle deploy -t prod`.

### Data Pipeline Bundles (bronze_loader, silver, gold)

```yaml
variables:
  catalog:
    description: "UC catalog for pipeline output"
  schema:
    description: "UC schema for pipeline output"
  orchestration_catalog:
    description: "UC catalog for the LakeSignal signal store"
  orchestration_schema:
    description: "UC schema for the LakeSignal signal store"

targets:
  dev:
    default: true
    mode: development
    variables:
      catalog: dev_catalog
      schema: lakesignal_example
      orchestration_catalog: dev_catalog
      orchestration_schema: orchestration
  prod:
    mode: production
    variables:
      catalog: prod_catalog
      schema: lakesignal_example
      orchestration_catalog: prod_catalog
      orchestration_schema: orchestration
```

### Dispatcher Bundle

Only uses `orchestration_catalog` / `orchestration_schema`. The `${bundle.target}` value automatically resolves for DABs resource name prefixing.

### Pipeline Emitter Bundle

```yaml
targets:
  dev:
    default: true
    mode: development
    variables:
      catalog: dev_catalog          # observability (event logs)
      schema: pipeline_logs
      signal_catalog: dev_catalog   # signal store
      signal_schema: orchestration
  prod:
    mode: production
    variables:
      catalog: prod_catalog
      schema: pipeline_logs
      signal_catalog: prod_catalog
      signal_schema: orchestration
```

The observability catalog (event logs) should be per-env, matching where each environment's DLT pipelines publish their event logs. This keeps checkpoints isolated per env.

## Subscription YAML: One File, Per-Target Variables

Subscription YAML files use **`variables`** and **`targets`** blocks so the same file works in dev and prod. Environment-specific catalog/schema values live in **`targets.<env>.variables`**, not in duplicated files.

```yaml
# silver.subscription.yml — used in dev AND prod
variables:
  bronze_catalog:
    description: UC catalog for upstream pipeline output
  bronze_schema:
    description: UC schema for upstream pipeline output
  job_name:
    description: Downstream job to trigger (dispatch pattern)
    default: my_team_silver_pipelines

subscribe_to:
  - subject: pipeline
    subject_name: ${var.bronze_catalog}.${var.bronze_schema}.bronze_ingest
    action: completed

trigger:
  job: ${var.job_name}

targets:
  dev:
    default: true
    variables:
      bronze_catalog: dev_catalog
      bronze_schema: lakesignal_example
      job_name: my_team_silver_pipelines
  prod:
    variables:
      bronze_catalog: prod_catalog
      bronze_schema: lakesignal_example
      job_name: my_team_silver_pipelines
```

**Signal store isolation** comes from `--catalog` / `--schema` on the wheel task (orchestration store per env). **Upstream `subject_name` matching** comes from target variable substitution — align `${var.bronze_catalog}` / `${var.bronze_schema}` with each environment's pipeline output catalog and schema.

**Dispatch `trigger`**: Use `trigger.job`, `trigger.pipeline`, or `trigger.materialized_view` (see [`lakesignal/README.md`](../lakesignal/README.md#trigger-dispatch-pattern-only)). Pass **`--target`** (e.g. `${bundle.target}`) so DABs development-mode job/pipeline name prefixes resolve when looking up trigger resources.

**Pipeline resource names**: Databricks may prefix deployed pipeline/job **display names** in `mode: development`. Align `subject_name` and `trigger` names with the **deployed** resource names in each environment, or use production mode for stable names.

## Setup Workflow Per Environment

The `setup()` function (`lakesignal/src/lakesignal/setup.py`) is already idempotent. Per environment:

1. **Create catalog** (admin, one-time) — `setup()` does not create catalogs
2. **Run `setup(spark, catalog=..., schema=..., service_principal=...)`** — creates schema, volume, 7 tables, grants
3. **Upload wheel** to `/Volumes/{catalog}/{schema}/lakesignal/libs/`
4. **Deploy bundle**: `databricks bundle deploy -t <env>`

Each environment gets isolated: tables, checkpoints, volumes, and UC permissions.

## Cross-Environment Monitoring

### Parameterized Dashboards (Start Here)

Lakeview dashboard with an environment dropdown. Queries resolve catalog/schema from the parameter. No cross-catalog permissions needed.

### UNION Views (When Unified View Needed)

Create views in a monitoring schema:

```sql
CREATE OR REPLACE VIEW monitoring.all_signals AS
SELECT 'dev' AS env, * FROM dev_catalog.orchestration.signal
UNION ALL
SELECT 'prod' AS env, * FROM prod_catalog.orchestration.signal;
```

Create for `signal`, `lakesignal_audit`, and `subscriber_registry` (the three observational tables). Skip operational tables (`subscriber_signal`, `subscriber_run`, `subscriber_run_signal`).

## UC Permissions

| Principal | Dev Signal Store | Prod Signal Store |
|-----------|-----------------|-------------------|
| Dev SP | USE CATALOG/SCHEMA, MODIFY + SELECT | No access |
| Prod SP | No access | USE CATALOG/SCHEMA, MODIFY + SELECT |
| Monitoring SP | SELECT on signal, lakesignal_audit, subscriber_registry | Same |
| Engineers | Full access | SELECT only |

Run `setup()` with separate `service_principal` per env to apply grants correctly.

## Edge Cases

**Accidental cross-env pointing**: DAB targets prevent this — `deploy -t prod` uses prod variables. UC permissions (Topology A) provide a hard guardrail. Subscriber already logs the signal table it reads from on startup.

**ZeroBus publishers**: Per-env configuration — separate `client_id` / `client_secret` per env, store in per-env secret scopes (e.g., `lakesignal-dev`, `lakesignal-prod`).

**Trace IDs**: Environment-scoped. A trace ID in dev has no meaning in prod. No cross-env trace correlation needed. Prod runs generate fresh trace IDs.

**Pipeline emitter `subject_name`**: Uses DLT `dataset_name` (short name like `silver_orders`, not FQN). Since signals go to env-specific stores, same `subject_name` across envs does not conflict.

## Migration Path (Single Store to Per-Env)

**Recommended: Clean cut**

1. Provision new per-env signal stores via `setup()`
2. Deploy per-env bundles
3. Pause old single-env jobs
4. New signal stores populate from fresh pipeline runs
5. Checkpoints start fresh automatically (new volume paths)
6. Old store remains for historical audit queries

Signal stores are operational state, not data warehouses. A fresh start per env is usually the right call. Historical signals stay queryable in the old store.
