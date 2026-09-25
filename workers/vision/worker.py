import argparse
import base64
import io
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import numpy as np
import onnxruntime as ort
from PIL import Image

Image.MAX_IMAGE_PIXELS = 4_000_000

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200 if self.path == "/ready" else 404)
        self.end_headers()

    def do_POST(self):
        if self.path != "/predict":
            self.send_error(404)
            return
        try:
            size = int(self.headers.get("Content-Length", "0"))
            if size < 1 or size > 1048576:
                self.send_error(400)
                return
            data = json.loads(self.rfile.read(size))
            image_bytes = base64.b64decode(data["image"], validate=True)
            if len(image_bytes) > 786432:
                self.send_error(400)
                return
            image = Image.open(io.BytesIO(image_bytes))
            if image.width * image.height > 4_000_000:
                raise ValueError("image is too large")
            image = image.convert("RGB").resize((224, 224))
            array = np.asarray(image, dtype=np.float32).transpose(2, 0, 1)[None] / 255.0
        except (ValueError, KeyError, OSError, RuntimeError) as error:
            self.send_error(400, str(error))
            return
        try:
            output = self.server.session.run(None, {self.server.input_name: array})[0]
            result = json.dumps({"class_index": int(np.argmax(output[0]))}).encode()
        except Exception:
            self.send_error(502, "model inference failed")
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(result)))
        self.end_headers()
        self.wfile.write(result)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=9004)
    parser.add_argument("--model", required=True)
    args = parser.parse_args()
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    server.session = ort.InferenceSession(args.model, providers=["CPUExecutionProvider"])
    if server.session.get_inputs()[0].shape != [1, 3, 224, 224]:
        raise ValueError("model must accept [1,3,224,224] input")
    server.input_name = server.session.get_inputs()[0].name
    server.serve_forever()


if __name__ == "__main__":
    main()
