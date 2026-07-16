#!/usr/bin/env python3
"""Local receiver for browser-assisted Ovid (MSSE) PDF downloads.

MSSE moved to ovid.com (~2026-07) and its article PDFs are gated behind an
entitled, logged-in Ovid session — the headless curl_cffi path in
modules.downloader can no longer fetch them, and the entitlement cookie is
httpOnly so it cannot be scraped for a headless bridge. The reliable route is
browser-assisted: with Ovid logged in, an agent drives Chrome to
`fetch('/pdf/...', {credentials:'include'})` (which succeeds on an entitled
session) and POSTs the raw bytes to this receiver, which writes them to
~/Downloads. `no-cors` POST avoids the CORS preflight, and 127.0.0.1 is a
trustworthy origin so an https page may POST to it. This sidesteps Chrome's
"multiple automatic downloads" block and the base64 return filter.

Run:  python3 scripts/ovid_pdf_receiver.py   (listens on 127.0.0.1:8799)
Then, from the logged-in Ovid tab, POST the PDF bytes to
http://127.0.0.1:8799/<pmid>_MSS<num>.pdf  ->  saved to ~/Downloads/<name>.
"""
from __future__ import annotations

import os
from http.server import BaseHTTPRequestHandler, HTTPServer

DEST = os.path.expanduser(os.getenv("OVID_RECEIVER_DEST", "~/Downloads"))
PORT = int(os.getenv("OVID_RECEIVER_PORT", "8799"))


class Handler(BaseHTTPRequestHandler):
    def _cors(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")

    def do_OPTIONS(self) -> None:  # noqa: N802 (http.server API)
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_POST(self) -> None:  # noqa: N802 (http.server API)
        name = os.path.basename(self.path.lstrip("/")) or "upload.bin"
        length = int(self.headers.get("Content-Length", 0))
        data = self.rfile.read(length)
        path = os.path.join(DEST, name)
        with open(path, "wb") as fh:
            fh.write(data)
        is_pdf = data[:5] == b"%PDF-"
        msg = f"OK {path} {len(data)} bytes pdf={is_pdf}".encode()
        self.send_response(200)
        self._cors()
        self.send_header("Content-Length", str(len(msg)))
        self.end_headers()
        self.wfile.write(msg)

    def log_message(self, *args) -> None:  # silence per-request logging
        pass


if __name__ == "__main__":
    os.makedirs(DEST, exist_ok=True)
    print(f"[ovid-receiver] listening on 127.0.0.1:{PORT}, writing to {DEST}")
    HTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
