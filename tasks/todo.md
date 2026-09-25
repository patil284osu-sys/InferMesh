# InferMesh prototype checklist

- [x] Fake worker and bounded HTTP gateway: stream and predict through one worker.
- [x] Two-worker routing, deadlines and cancellation: failover and interrupted stream verified.
- [x] Durable submission and outbox: restart and idempotency checks pass.
- [x] JetStream delivery and attempt fencing: stale and cancelled results rejected in store tests.
- [x] Optional model adapters and open-loop benchmark: Python syntax checked; fake-model benchmark recorded.
- [x] Documentation and demo check: one command verifies the local fake-worker system.
