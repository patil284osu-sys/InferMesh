# InferMesh implementation plan

## Goal
Build the smallest local system that routes interactive requests across replicated workers and can recover accepted vision jobs after a gateway restart.

## Decisions
- One Go gateway, fake worker processes, HTTP worker contract.
- Round robin first; least-active as one comparison policy.
- SQLite transaction for submission and outbox; JetStream transports job IDs.
- Single gateway and one logical caller for this local demo.

## Build order
1. Fake worker and bounded interactive gateway.
2. Worker routing, deadlines, streaming terminal contract, and basic metrics.
3. SQLite durable intake and JetStream outbox/consumer with attempt fencing.
4. Optional real model adapters and scheduled-arrival benchmark.
5. Failure checks, benchmark baseline, and documentation.

## Verification
- `go test ./...` and `go vet ./...` pass.
- `scripts/check-demo.sh` exercises streaming, prediction, restart, duplicate submission, failover, and interrupted stream.
- Synthetic benchmark records scheduled load, actual sends, tail latency, and failure count.

## Remaining for the original proposal
Real-model benchmark with pinned hardware/model versions, gRPC/protobuf, token-aware scheduling, vision microbatching, tracing, and high-availability gateway ownership are separate milestones. None is claimed by the prototype.
