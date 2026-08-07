#!/usr/bin/env bash
# Create a per-project Cloud Logging sink routing errors to the shared topic.
#
# Usage: ./sink.sh <project-slug> '<log-filter>'
#
# Sinks are created here rather than by the binary, so the sentinel needs no
# GCP admin credentials (SPEC §4.2.2).
#
# Routing note: a logging sink cannot attach custom attributes to what it
# publishes, so the sentinel resolves the project from the log entry's resource
# labels — service_name, function_name, job, namespace_name — or an explicit
# project_slug label. The value must equal the registered slug. Entries that
# resolve to nothing are recorded as unroutable and shown in the dashboard
# rather than dropped, which is the signal that a name has drifted.
set -euo pipefail
: "${GCP_PROJECT_ID:?set GCP_PROJECT_ID}"
: "${PUBSUB_TOPIC:=sentinel-events}"

SLUG="${1:?usage: sink.sh <project-slug> '<log-filter>'}"
FILTER="${2:?usage: sink.sh <project-slug> '<log-filter>'}"
SINK="sentinel-${SLUG}"

gcloud logging sinks create "$SINK" \
  "pubsub.googleapis.com/projects/${GCP_PROJECT_ID}/topics/${PUBSUB_TOPIC}" \
  --project "$GCP_PROJECT_ID" \
  --log-filter "$FILTER" \
  || gcloud logging sinks update "$SINK" \
       "pubsub.googleapis.com/projects/${GCP_PROJECT_ID}/topics/${PUBSUB_TOPIC}" \
       --project "$GCP_PROJECT_ID" --log-filter "$FILTER"

WRITER=$(gcloud logging sinks describe "$SINK" --project "$GCP_PROJECT_ID" \
  --format 'value(writerIdentity)')

gcloud pubsub topics add-iam-policy-binding "$PUBSUB_TOPIC" \
  --project "$GCP_PROJECT_ID" \
  --member "$WRITER" \
  --role roles/pubsub.publisher

echo "sink $SINK created; writer $WRITER granted publisher on $PUBSUB_TOPIC"
