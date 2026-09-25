#!/usr/bin/env bash
set -euo pipefail

tmp=$(mktemp -d)
go build -o "$tmp/gateway" ./cmd/gateway
go build -o "$tmp/fakeworker" ./cmd/fakeworker
nats-server -js -a 127.0.0.1 -sd "$tmp/nats" -p 14222 >"$tmp/nats.log" 2>&1 & broker=$!
"$tmp/fakeworker" -port 19001 -name first -delay 250ms >"$tmp/first.log" 2>&1 & first=$!
"$tmp/fakeworker" -port 19002 -name second -delay 250ms >"$tmp/second.log" 2>&1 & second=$!
trap 'if [[ -n ${gateway:-} ]]; then kill "$gateway" 2>/dev/null || true; fi; kill "$first" "$second" "$broker" 2>/dev/null || true; rm -rf "$tmp"' EXIT
sleep 1
"$tmp/gateway" -port 18080 -workers demo@1@http://localhost:19001,demo@1@http://localhost:19002 -db "$tmp/jobs.db" -nats nats://localhost:14222 >"$tmp/gateway.log" 2>&1 & gateway=$!
sleep 1
stream=$(curl -fsS -N localhost:18080/generate -H 'Content-Type: application/json' -d '{"model":"demo","version":"1","prompt":"hello world","max_tokens":2}')
[[ "$stream" == *"event: completed"* ]]
result=$(curl -fsS localhost:18080/predict -H 'Content-Type: application/json' -d '{"model":"demo","version":"1","image":"sample"}')
[[ "$result" == *'"label":"demo"'* ]]
response=$(curl -fsS localhost:18080/jobs -H 'Content-Type: application/json' -H 'Idempotency-Key: restart-check' -d '{"model":"demo","version":"1","image":"sample"}')
id=$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["id"])' "$response")
kill "$gateway"
wait "$gateway" 2>/dev/null || true
"$tmp/gateway" -port 18080 -workers demo@1@http://localhost:19001,demo@1@http://localhost:19002 -db "$tmp/jobs.db" -nats nats://localhost:14222 >>"$tmp/gateway.log" 2>&1 & gateway=$!
sleep 1
for attempt in {1..15}; do
    state=$(curl -fsS "localhost:18080/jobs/$id")
    if [[ "$state" == *'"status":"succeeded"'* ]]; then break; fi
    sleep 1
done
[[ "$state" == *'"status":"succeeded"'* ]]
repeat=$(curl -fsS localhost:18080/jobs -H 'Content-Type: application/json' -H 'Idempotency-Key: restart-check' -d '{"model":"demo","version":"1","image":"sample"}')
[[ "$repeat" == *"$id"* ]]
kill "$first"
sleep 2
result=$(curl -fsS localhost:18080/predict -H 'Content-Type: application/json' -d '{"model":"demo","version":"1","image":"sample"}')
[[ "$result" == *'"worker":"second"'* ]]
response=$(curl -fsS localhost:18080/jobs -H 'Content-Type: application/json' -d '{"model":"demo","version":"1","image":"to-cancel"}')
cancel_id=$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["id"])' "$response")
for attempt in {1..40}; do
    state=$(curl -fsS "localhost:18080/jobs/$cancel_id")
    if [[ "$state" == *'"status":"running"'* ]]; then break; fi
    sleep 0.02
done
[[ "$state" == *'"status":"running"'* ]]
curl -fsS -X POST "localhost:18080/jobs/$cancel_id/cancel" > /dev/null
sleep 0.4
state=$(curl -fsS "localhost:18080/jobs/$cancel_id")
[[ "$state" == *'"status":"cancelled"'* ]]
curl -fsS -N localhost:18080/generate -H 'Content-Type: application/json' -d '{"model":"demo","version":"1","prompt":"one two three four five six seven eight nine ten","max_tokens":10}' >"$tmp/interrupted" & stream_pid=$!
sleep 0.5
kill "$second"
wait "$stream_pid"
[[ "$(cat "$tmp/interrupted")" == *'event: failed'* ]]
printf 'stream, prediction, restart recovery, idempotency, worker failover, active cancellation, and interrupted stream passed\n'
