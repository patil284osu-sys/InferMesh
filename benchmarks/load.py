import argparse
import concurrent.futures
import json
import math
import time
import urllib.error
import urllib.request


def one(url, kind, scheduled, prompt, timeout):
    sent = time.monotonic()
    body = {"model": "demo", "version": "1", "deadline_ms": int(timeout * 1000)}
    if kind == "generate":
        body.update({"prompt": prompt, "max_tokens": 16})
    else:
        body["image"] = "sample"
    request = urllib.request.Request(url + "/" + kind, json.dumps(body).encode(), {"Content-Type": "application/json"})
    first = None
    try:
        with urllib.request.urlopen(request, timeout=timeout + 1) as response:
            if kind == "generate":
                completed = False
                for line in response:
                    if line.startswith(b"event: token") and first is None:
                        first = time.monotonic()
                    if line.startswith(b"event: completed"):
                        completed = True
                    if line.startswith(b"event: failed"):
                        completed = False
                ok = completed
            else:
                response.read()
                ok = response.status == 200
    except (urllib.error.URLError, TimeoutError, OSError):
        ok = False
    end = time.monotonic()
    return {"send_lag_ms": (sent - scheduled) * 1000, "latency_ms": (end - sent) * 1000, "ttft_ms": (first - sent) * 1000 if first else None, "ok": ok}


def percentile(values, fraction):
    if not values:
        return None
    values = sorted(values)
    return round(values[math.ceil(fraction * len(values)) - 1], 2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:8080")
    parser.add_argument("--kind", choices=["predict", "generate"], default="predict")
    parser.add_argument("--rate", type=float, default=10)
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--clients", type=int, default=32)
    parser.add_argument("--timeout", type=float, default=5)
    parser.add_argument("--objective-ms", type=float, default=500)
    args = parser.parse_args()
    if args.rate <= 0 or args.count < 1 or args.clients < 1 or not 0 < args.timeout <= 30:
        parser.error("rate, count, clients and timeout must be valid and positive")
    start = time.monotonic() + 0.1
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.clients) as pool:
        futures = []
        for i in range(args.count):
            scheduled = start + i / args.rate
            time.sleep(max(0, scheduled - time.monotonic()))
            futures.append(pool.submit(one, args.url, args.kind, scheduled, "one two three four", args.timeout))
        results = [future.result() for future in futures]
    elapsed = time.monotonic() - start
    successes = [row["latency_ms"] for row in results if row["ok"]]
    lag = [row["send_lag_ms"] for row in results]
    ttft = [row["ttft_ms"] for row in results if row["ttft_ms"] is not None]
    print(json.dumps({"requests": len(results), "successes": len(successes), "failures": len(results) - len(successes), "observed_seconds": round(elapsed, 2), "offered_rate": args.rate, "completed_rate": round(len(successes) / elapsed, 2), "goodput": round(sum(row["ok"] and row["latency_ms"] <= args.objective_ms for row in results) / elapsed, 2), "send_lag_p99_ms": percentile(lag, 0.99), "latency_p50_ms": percentile(successes, 0.5), "latency_p95_ms": percentile(successes, 0.95), "latency_p99_ms": percentile(successes, 0.99), "ttft_p95_ms": percentile(ttft, 0.95)}, indent=2))


if __name__ == "__main__":
    main()
