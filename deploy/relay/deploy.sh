#!/usr/bin/env bash
# Deploy the GitHub webhook relay to Cloud Run.
#
# The webhook secret is read from Secret Manager at runtime, never baked into
# the image and never passed as a plain environment variable.
set -euo pipefail

: "${GCP_PROJECT_ID:?set GCP_PROJECT_ID}"
: "${PUBSUB_TOPIC:=sentinel-events}"
: "${REGION:=us-central1}"
: "${SERVICE:=sentinel-relay}"
: "${SECRET_NAME:=github-webhook-secret}"

cd "$(dirname "$0")"

gcloud run deploy "$SERVICE" \
  --project "$GCP_PROJECT_ID" \
  --region "$REGION" \
  --source . \
  --allow-unauthenticated \
  --set-env-vars "GCP_PROJECT_ID=${GCP_PROJECT_ID},PUBSUB_TOPIC=${PUBSUB_TOPIC}" \
  --set-secrets "GITHUB_WEBHOOK_SECRET=${SECRET_NAME}:latest" \
  --max-instances 3 \
  --memory 128Mi

echo
echo "Relay URL:"
gcloud run services describe "$SERVICE" \
  --project "$GCP_PROJECT_ID" --region "$REGION" \
  --format 'value(status.url)'
echo
echo "Add that URL as a webhook on each repository, content type application/json,"
echo "with the same secret, subscribed to: Workflow runs, Issues."
