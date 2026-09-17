"""Serve web/ over plain HTTP on localhost (localhost is a secure context,
so WebTransport is allowed). Disables caching to ease iteration."""

import argparse
import functools
import http.server
import os

WEB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "web")


class Handler(http.server.SimpleHTTPRequestHandler):
    extensions_map = {**http.server.SimpleHTTPRequestHandler.extensions_map, ".js": "text/javascript"}

    def end_headers(self) -> None:
        self.send_header("Cache-Control", "no-store")
        super().end_headers()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--bind", default="127.0.0.1")
    args = parser.parse_args()
    handler = functools.partial(Handler, directory=WEB)
    with http.server.ThreadingHTTPServer((args.bind, args.port), handler) as httpd:
        print(f"serving {os.path.abspath(WEB)} at http://localhost:{args.port}")
        httpd.serve_forever()
