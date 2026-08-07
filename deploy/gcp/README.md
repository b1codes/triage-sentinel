# GCP provisioning

Three scripts, run in order. All are idempotent — re-running one is safe.

```bash
export GCP_PROJECT_ID=your-project

./topic.sh                       # shared ingestion topic
./subscription.sh                # the pull subscription the sentinel long-polls
./sink.sh <project-slug> '<log-filter>'   # once per project you want logs from
```

Then deploy the relay (`../relay/deploy.sh`) and register the webhook on each
repository.

## What the sentinel needs, and what it does not

The binary needs **`roles/pubsub.subscriber` and nothing else**. Sinks are
created by these scripts rather than by the sentinel precisely so the running
process never holds GCP admin credentials (SPEC §4.2.2).

Application default credentials must be available on the host — either a
service-account key file referenced by `GOOGLE_APPLICATION_CREDENTIALS`, or
`gcloud auth application-default login` for local work.

## Why sink routing depends on naming

A Cloud Logging sink **cannot attach custom attributes** to the messages it
publishes. There is no way to stamp `project_slug` onto an entry at the sink.

So the sentinel infers the project from the log entry itself, checking these
resource labels in order:

1. `project_slug` — an explicit label, if the service sets one
2. `service_name` — Cloud Run
3. `function_name` — Cloud Functions
4. `job` — Cloud Run jobs
5. `namespace_name` — GKE

**The value must equal the registered slug in `projects.yaml`.** If your Cloud
Run service is `example-api`, the project's slug must also be `example-api`.

When nothing matches, the entry is **recorded as unroutable and shown in the
dashboard**, not dropped. That is deliberate: an unroutable event almost always
means a service was renamed or a slug drifted, and silently discarding it would
hide the drift instead of surfacing it.

## Subscription settings

`subscription.sh` sets two values worth understanding:

- **`--ack-deadline 60`** is generous but not load-bearing. The sentinel acks
  immediately after the durable write (SPEC §4.2), so processing time never
  approaches the deadline. This is also why the sentinel needs no
  ack-deadline extension, and therefore no gRPC streaming client.
- **`--message-retention-duration 7d`** is the one that matters. It is how long
  the Mac Mini can be asleep, rebooting, or offline without losing events.

## Log filter examples

```bash
# Cloud Run service errors
./sink.sh example-api 'severity>=ERROR AND resource.labels.service_name="example-api"'

# Cloud Functions
./sink.sh example-worker 'severity>=ERROR AND resource.labels.function_name="example-worker"'
```

The sentinel additionally ignores anything below `ERROR` at the adapter, so a
broader filter costs Pub/Sub traffic but cannot create spurious incidents.
