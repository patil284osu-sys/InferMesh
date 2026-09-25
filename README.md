# InferMesh

A local inference gateway that routes streaming text and vision requests across model workers, rejects overload, and resumes accepted vision jobs after a gateway restart.

**Status:** runnable prototype. The [original proposal](InferMesh-README.md) describes additional experiments. [Architecture and failure contracts](docs/architecture.md) explain the blocks and intended design. No production availability or performance improvement is claimed.

## What works

- Go HTTP gateway with input limits, model/version routing, deadlines, cancellation, worker readiness checks, bounded interactive waiting, round robin assignment, and per-worker slots.
- Streaming text over SSE. Workers emit an explicit terminal marker; a dropped worker stream produces a `failed` event, not a successful completion.
- SQLite-backed vision jobs with atomic submission/outbox, idempotency keys, JetStream publication, retry limits, cancellation, and attempt-number fencing.
- One reserved background slot per worker when durable jobs are enabled; other slots serve interactive traffic.
- Fake model workers, an optional vLLM streaming adapter, an optional ONNX Runtime vision adapter, simple Prometheus counters/gauges, and an open-loop benchmark generator.

The internal worker protocol is HTTP JSON and newline-delimited JSON streaming. The original gRPC proposal was simplified to make the components easier to run and inspect. Current scheduling offers round robin and least-active worker selection (`-policy least-active`); token estimates, batching, tracing, gateway high availability, and direct-versus-gateway performance claims have not been implemented or measured.

## Run the small demo

Requires Go 1.26.8 or an installation that can download that toolchain. Durable jobs additionally require a `nats-server` executable with JetStream enabled. From the repository root, run each command in a separate terminal:

```sh
mkdir -p .data
go run ./cmd/fakeworker -port 9001 -name first
go run ./cmd/fakeworker -port 9002 -name second
nats-server -js -a 127.0.0.1 -sd .data/nats
go run ./cmd/gateway -workers demo@1@http://localhost:9001,demo@1@http://localhost:9002 -db .data/jobs.db
```

To try interactive routing without NATS, omit `-db` and skip the `nats-server` command. Gateway listens on `127.0.0.1:8080` by default. This demo is for a trusted local machine, not a publicly exposed service.

```sh
curl -N -X POST http://localhost:8080/generate -H 'Content-Type: application/json' -d '{"model":"demo","version":"1","prompt":"hello infer mesh","max_tokens":3}'
curl -X POST http://localhost:8080/predict -H 'Content-Type: application/json' -d '{"model":"demo","version":"1","image":"sample"}'
curl -X POST http://localhost:8080/jobs -H 'Content-Type: application/json' -H 'Idempotency-Key: my-first-job' -d '{"model":"demo","version":"1","image":"sample"}'
curl http://localhost:8080/metrics
```

The fake worker accepts any nonempty `image` string. Real vision requests require a base64-encoded image. `POST /jobs` returns an ID; use `GET /jobs/{id}` to read status and `POST /jobs/{id}/cancel` to request cancellation. Repeating the same key and input returns the same ID; using that key with different input returns 409. The local demo has one logical caller, so keys are global. The stored SQLite file and NATS data directory must survive restart to preserve accepted jobs.

## Real model adapters

For text, run a vLLM OpenAI-compatible server, then `python3 workers/vllm/worker.py --model YOUR_MODEL_ID --backend http://localhost:8000 --port 9003`. Add `text@1@http://localhost:9003` to `-workers` and send `model: "text"`. This adapter forwards streamed chat completions and signals completion only on the backend's `[DONE]` event. A real model and compatible hardware are required.

For vision, install `pip install -r workers/vision/requirements.txt`, then run `python3 workers/vision/worker.py --model YOUR_MODEL.onnx --port 9004`. Add `vision@1@http://localhost:9004` to `-workers` and send `model: "vision"` with a base64 image. The adapter assumes a classifier accepting float32 RGB tensors shaped `[1,3,224,224]` in the range `[0,1]` and returning class scores. Choose or export a model with exactly that preprocessing contract; labels are returned as class indices. Model weights are not bundled.

## Check and measure

```sh
go test ./...
go vet ./...
python3 benchmarks/load.py --kind predict --rate 10 --count 100 --objective-ms 500
python3 benchmarks/load.py --kind generate --rate 10 --count 100 --objective-ms 500
```

The load generator schedules arrivals at a fixed rate and reports actual send lag, success count, latency percentiles, time to first token, throughput, and goodput. Its default payload targets fake workers. Record hardware, model revision, offered load, duration, warmup, and repeated trial variability before making real performance claims. A slow client can distort results; inspect `send_lag_p99_ms`.

## Failure contract

Interactive calls live only while the gateway and client connection live. If a stream is interrupted, the gateway emits `failed` and never switches workers mid-output. A cancelled request releases its dispatch slot. When workers fail health checks, new requests route to another compatible ready worker or expire/reject.

`202` for a job means its row and outbox entry committed to SQLite. The publisher retries unpublished rows after restart; JetStream may redeliver a job ID. The job store fences results by attempt number and terminal status, so late attempts cannot replace the visible result. Model execution itself can happen more than once. Jobs try up to three times before `failed`. A job whose worker never returns times out after 30 seconds; a crashed gateway's unacknowledged delivery can take roughly a minute to return. No result retention or cleanup policy is implemented yet.

This is one gateway with a local SQLite database. It needs authenticated callers, quotas, a shared database/ownership design, tested model contracts, and operational hardening before external hosting.
