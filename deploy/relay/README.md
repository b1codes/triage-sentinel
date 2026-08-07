# GitHub webhook relay

A small Cloud Run service that verifies GitHub webhook signatures and
republishes the deliveries to Pub/Sub.

It exists because the sentinel host sits behind NAT and cannot receive inbound
webhooks (SPEC §1.3, §4.2.1). The sentinel reaches out to Pub/Sub instead; this
relay is the only inbound-facing piece of the system.

## This is a separate Go module, on purpose

`go.mod` here is deliberately not part of the root module.

The relay publishes to Pub/Sub and therefore wants `cloud.google.com/go/pubsub`,
which brings gRPC and roughly 165 additional modules with it. Measured:

| | Root module (sentinel binary) | This module |
|---|---|---|
| Modules in graph | 34 | 199 |
| gRPC modules | 0 | 2 |

The sentinel runs on a Mac Mini against a 15–25 MB RSS target. Keeping the
Google client libraries on this side of a module boundary is what holds that
binary at ~12 MB (design §2.1). `go build ./...` at the repository root does not
descend into a nested module, so the isolation is structural rather than a
convention someone has to remember.

## Deploying

```bash
# One-time: store the shared webhook secret
printf '%s' "$GITHUB_WEBHOOK_SECRET" | \
  gcloud secrets create github-webhook-secret --data-file=- --project "$GCP_PROJECT_ID"

export GCP_PROJECT_ID=your-project
./deploy.sh
```

The secret is read from Secret Manager at runtime. It is never baked into the
image and never passed as a plain environment variable.

## Registering the webhook

`deploy.sh` prints the service URL. On each repository, add a webhook with:

- **Payload URL** — the printed Cloud Run URL
- **Content type** — `application/json`
- **Secret** — the same value stored in Secret Manager
- **Events** — Workflow runs, Issues

## Why the endpoint is unauthenticated

`--allow-unauthenticated` is unavoidable: GitHub cannot present a Google
identity token.

**The HMAC check is the only thing protecting this endpoint.** Verification runs
before anything else in the handler — before the body is inspected, before
anything is published. Without it, anyone who discovered the Cloud Run URL could
inject arbitrary events into the sentinel's ingestion path. That is why the
signature-rejection tests in `main_test.go` are not optional.

The signature is also forwarded to Pub/Sub as a message attribute, and the
control plane **re-verifies it independently**, so a compromised relay still
cannot inject events (SPEC §10).

## Failure behaviour

A publish failure returns 500 rather than 200. GitHub retries on 5xx, which is
correct: the message never reached Pub/Sub, so acknowledging it would lose the
event outright.

## Tests

```bash
go test ./...
```

Runs without any GCP access — the handler takes a `publisher` interface.
