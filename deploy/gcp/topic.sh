#!/usr/bin/env bash
# Create the shared ingestion topic.
set -euo pipefail
: "${GCP_PROJECT_ID:?set GCP_PROJECT_ID}"
: "${PUBSUB_TOPIC:=sentinel-events}"

gcloud pubsub topics create "$PUBSUB_TOPIC" --project "$GCP_PROJECT_ID" \
  || echo "topic $PUBSUB_TOPIC already exists"
