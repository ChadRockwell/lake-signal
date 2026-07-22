#!/usr/bin/env bash
# Example: Publish events via the ZeroBus REST API (no SDK required).
#
# Useful for: bash scripts, cron jobs, CI/CD pipelines, or any system
# that can make HTTP requests.

set -euo pipefail

# --- Configuration (replace with your values) ---
WORKSPACE_URL="https://dbc-xxxxx.cloud.databricks.com"
WORKSPACE_ID="1234567890123456"
ZEROBUS_ENDPOINT="https://${WORKSPACE_ID}.zerobus.us-west-2.cloud.databricks.com"
CLIENT_ID="<service-principal-client-id>"
CLIENT_SECRET="<service-principal-secret>"
TABLE="catalog.orchestration.events"

# --- Step 1: Acquire OAuth token ---
# Token is valid for ~1 hour. Cache and refresh as needed.
OAUTH_TOKEN=$(curl -s -X POST \
  -u "${CLIENT_ID}:${CLIENT_SECRET}" \
  -d "grant_type=client_credentials" \
  -d "scope=all-apis" \
  -d "resource=api://databricks/workspaces/${WORKSPACE_ID}/zerobusDirectWriteApi" \
  "${WORKSPACE_URL}/oidc/v1/token" \
  | python3 -c "import sys, json; print(json.load(sys.stdin)['access_token'])")

echo "OAuth token acquired."

# --- Step 2: Publish an event ---
# Generate event_id and event_timestamp client-side (ZeroBus bypasses SQL defaults).
# Body MUST be a JSON array, even for a single event.
EVENT_ID=$(uuidgen | tr '[:upper:]' '[:lower:]')
TRACE_ID=$(uuidgen | tr '[:upper:]' '[:lower:]')
EVENT_TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%S.000Z")

curl -s -X POST \
  "${ZEROBUS_ENDPOINT}/zerobus/v1/tables/${TABLE}/insert" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${OAUTH_TOKEN}" \
  -d "[{
    \"event_id\": \"${EVENT_ID}\",
    \"trace_id\": \"${TRACE_ID}\",
    \"subject\": \"pipeline\",
    \"subject_name\": \"external_etl\",
    \"action\": \"completed\",
    \"event_timestamp\": \"${EVENT_TIMESTAMP}\",
    \"metadata\": \"{\\\"logical_date\\\": \\\"2025-01-15\\\", \\\"source\\\": \\\"cron\\\"}\"
  }]"

echo "Event published: event_id=${EVENT_ID}, trace_id=${TRACE_ID}"
