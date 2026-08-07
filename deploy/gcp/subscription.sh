#!/usr/bin/env bash
# Create the pull subscription the sentinel long-polls.
#
# ack-deadline is generous but not load-bearing: the sentinel acks immediately
# after the durable write (SPEC §4.2), so processing time never approaches it.
# message-retention is what actually matters — it is how long the host can be
# asleep or rebooting without losing events.
set -euo pipefail
: "${GCP_PROJECT_ID:?set GCP_PROJECT_ID}"
: "${PUBSUB_TOPIC:=sentinel-events}"
: "${PUBSUB_SUBSCRIPTION:=sentinel-pull}"

gcloud pubsub subscriptions create "$PUBSUB_SUBSCRIPTION" \
  --project "$GCP_PROJECT_ID" \
  --topic "$PUBSUB_TOPIC" \
  --ack-deadline 60 \
  --message-retention-duration 7d \
  --expiration-period never \
  || echo "subscription $PUBSUB_SUBSCRIPTION already exists"

echo
echo "Set this in .env:"
echo "PUBSUB_SUBSCRIPTION=projects/${GCP_PROJECT_ID}/subscriptions/${PUBSUB_SUBSCRIPTION}"
