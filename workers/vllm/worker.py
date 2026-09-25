import argparse
import json
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/ready":
            self.send_error(404)
            return
        try:
            with urllib.request.urlopen(self.server.backend + "/v1/models", timeout=2) as response:
                ready = response.status == 200
        except (urllib.error.URLError, TimeoutError):
            ready = False
        self.send_response(200 if ready else 503)
        self.end_headers()

    def do_POST(self):
        if self.path != "/generate":
            self.send_error(404)
            return
        try:
            size = int(self.headers.get("Content-Length", "0"))
            if size < 1 or size > 1048576:
                self.send_error(400)
                return
            data = json.loads(self.rfile.read(size))
            if not data.get("prompt") or not 1 <= data.get("max_tokens", 0) <= 512:
                self.send_error(400)
                return
            payload = json.dumps({"model": self.server.model, "messages": [{"role": "user", "content": data["prompt"]}], "max_tokens": data["max_tokens"], "stream": True}).encode()
            request = urllib.request.Request(self.server.backend + "/v1/chat/completions", payload, {"Content-Type": "application/json"})
            backend = urllib.request.urlopen(request, timeout=30)
        except (ValueError, KeyError, TypeError, urllib.error.URLError, TimeoutError) as error:
            self.send_error(502, str(error))
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/x-ndjson")
        self.end_headers()
        try:
            with backend:
                for line in backend:
                    if not line.startswith(b"data: "):
                        continue
                    value = line[6:].strip()
                    if value == b"[DONE]":
                        self.wfile.write(b'{"done":true}\n')
                        self.wfile.flush()
                        return
                    chunk = json.loads(value)
                    content = chunk["choices"][0]["delta"].get("content")
                    if content:
                        self.wfile.write((json.dumps({"token": content, "worker": "vllm"}) + "\n").encode())
                        self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, ValueError, KeyError, urllib.error.URLError, TimeoutError):
            return


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=9003)
    parser.add_argument("--backend", default="http://localhost:8000")
    parser.add_argument("--model", required=True)
    args = parser.parse_args()
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    server.backend = args.backend.rstrip("/")
    server.model = args.model
    server.serve_forever()


if __name__ == "__main__":
    main()
