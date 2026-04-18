#!/usr/bin/env python3
import http.client
import os
import sys
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit


ROOT = Path(__file__).resolve().parent
BACKEND_HOST = os.getenv("V1_BACKEND_HOST", "127.0.0.1")
BACKEND_PORT = int(os.getenv("V1_BACKEND_PORT", "8081"))
LISTEN_HOST = os.getenv("V1_FRONTEND_HOST", "0.0.0.0")
LISTEN_PORT = int(os.getenv("V1_FRONTEND_PORT", "5174"))


class FrontendHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(ROOT), **kwargs)

    def do_GET(self):
        if self.path.startswith("/api/v1/") or self.path == "/api/v1":
            self._proxy()
            return

        # require auth for main pages (index and internal)
        req_path = self.path
        if req_path == "/":
            req_path = "/index.html"

        if req_path in ("/index.html", "/internal.html"):
            if not self._check_authenticated():
                # redirect to login page
                self.send_response(302)
                self.send_header("Location", "/login.html")
                self.end_headers()
                return

        self.path = req_path
        return super().do_GET()

    def do_HEAD(self):
        if self.path.startswith("/api/v1/") or self.path == "/api/v1":
            self._proxy(head_only=True)
            return
        # normalize
        req_path = self.path
        if req_path == "/":
            req_path = "/index.html"

        if req_path in ("/index.html", "/internal.html"):
            if not self._check_authenticated():
                self.send_response(302)
                self.send_header("Location", "/login.html")
                self.end_headers()
                return

        self.path = req_path
        return super().do_HEAD()

    def do_POST(self):
        if self.path.startswith("/api/v1/") or self.path == "/api/v1":
            self._proxy()
            return
        self.send_error(405, "Method Not Allowed")

    def do_PUT(self):
        if self.path.startswith("/api/v1/") or self.path == "/api/v1":
            self._proxy()
            return
        self.send_error(405, "Method Not Allowed")

    def do_PATCH(self):
        if self.path.startswith("/api/v1/") or self.path == "/api/v1":
            self._proxy()
            return
        self.send_error(405, "Method Not Allowed")

    def do_DELETE(self):
        if self.path.startswith("/api/v1/") or self.path == "/api/v1":
            self._proxy()
            return
        self.send_error(405, "Method Not Allowed")

    def do_OPTIONS(self):
        if self.path.startswith("/api/v1/") or self.path == "/api/v1":
            self._proxy(head_only=True)
            return
        self.send_error(405, "Method Not Allowed")

    def _proxy(self, head_only: bool = False):
        parsed = urlsplit(self.path)
        path = parsed.path
        if path.startswith("/api/v1"):
            upstream_path = path
        else:
            upstream_path = f"/api/v1{path}"
        if parsed.query:
            upstream_path = f"{upstream_path}?{parsed.query}"

        body = None
        content_length = int(self.headers.get("Content-Length", "0") or "0")
        if content_length > 0 and not head_only:
            body = self.rfile.read(content_length)

        headers = {k: v for k, v in self.headers.items() if k.lower() not in {"host", "content-length", "connection", "accept-encoding"}}
        conn = http.client.HTTPConnection(BACKEND_HOST, BACKEND_PORT, timeout=120)
        try:
            conn.request(self.command, upstream_path, body=body, headers=headers)
            resp = conn.getresponse()
            payload = resp.read()
            self.send_response(resp.status, resp.reason)
            for key, value in resp.getheaders():
                if key.lower() in {"transfer-encoding", "connection", "content-length"}:
                    continue
                self.send_header(key, value)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            if not head_only and payload:
                self.wfile.write(payload)
        finally:
            conn.close()

    def _check_authenticated(self) -> bool:
        """Call backend /api/v1/auth/me with incoming Cookie header to verify login."""
        try:
            conn = http.client.HTTPConnection(BACKEND_HOST, BACKEND_PORT, timeout=10)
            headers = {k: v for k, v in self.headers.items() if k.lower() in {"cookie", "authorization"}}
            conn.request("GET", "/api/v1/auth/me", headers=headers)
            resp = conn.getresponse()
            # consider 200 as authenticated
            status = resp.status
            # drain
            _ = resp.read()
            conn.close()
            return status == 200
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            return False


def main():
    os.chdir(ROOT)
    server = ThreadingHTTPServer((LISTEN_HOST, LISTEN_PORT), FrontendHandler)
    print(f"Frontend listening on http://{LISTEN_HOST}:{LISTEN_PORT}")
    print(f"Proxying /api/v1 -> http://{BACKEND_HOST}:{BACKEND_PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()