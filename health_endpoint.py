#!/usr/bin/env python3
"""
Small, dependency-free HTTP endpoint that runs on the bare Proxmox host
(not in a container) and exposes local hardware health to collector.py via
GET /health.

Uses only the Python standard library: it runs outside any uv/virtualenv,
directly on the host, where `sensors` (lm-sensors), `smartctl`
(smartmontools) and `pvesm` are available.

Usage:
    python3 health_endpoint.py
"""
from __future__ import annotations

import json
import subprocess
from http.server import BaseHTTPRequestHandler, HTTPServer

HOST = "0.0.0.0"
PORT = 9100

SENSORS_CMD = ["sensors", "-j"]
SMART_CMDS = {
    "smart_nvme": ["smartctl", "-a", "/dev/nvme0n1", "--json"],
    "smart_nvme1": ["smartctl", "-a", "/dev/nvme1n1", "--json"],
}
PVESM_CMD = ["pvesm", "status"]
SUBPROCESS_TIMEOUT = 10  # seconds


def _run_json_command(cmd: list[str]) -> dict:
    """Runs a command and parses stdout as JSON. Subprocess errors (non-zero
    exit, timeout, command not found) and JSON parse errors propagate; the
    caller handles them. For SENSORS_CMD that happens in do_GET() (500 for
    the whole request); for each SMART_CMDS drive it happens per drive (see
    do_GET()), so one drive can't block the other."""
    result = subprocess.run(
        cmd, capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT, check=True,
    )
    return json.loads(result.stdout)


def _get_storage_pools() -> list[dict]:
    """Runs `pvesm status` and parses its table output.

    `pvesm status --output-format json` doesn't exist on this Proxmox
    version ("Unknown option: output-format"); the only output is a
    fixed-column table, so it is parsed here instead of with json.loads().
    Every data row has exactly 7 whitespace-separated fields (name, type,
    status, total, used, available, percent). A row that doesn't raises a
    ValueError instead of being skipped silently: a change in pvesm's table
    format must fail visibly, not make pools disappear unnoticed."""
    result = subprocess.run(
        PVESM_CMD, capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT, check=True,
    )
    lines = [ln for ln in result.stdout.splitlines() if ln.strip()]
    pools = []
    for line in lines[1:]:  # first line is the header
        fields = line.split()
        if len(fields) != 7:
            raise ValueError(f"unexpected pvesm status row format: {line!r}")
        name, storage_type, status, total_kib, used_kib, available_kib, percent = fields
        pools.append({
            "name": name,
            "type": storage_type,
            "status": status,
            "total_kib": int(total_kib),
            "used_kib": int(used_kib),
            "available_kib": int(available_kib),
            "percent_used": float(percent.rstrip("%")),
        })
    return pools


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/health":
            self.send_error(404, "Not Found")
            return

        try:
            body = {"sensors": _run_json_command(SENSORS_CMD)}
        except Exception as e:
            self._write_json(500, {"error": str(e)})
            return

        # Each drive independently: two physically separate sources, so an
        # unresponsive drive must not block the other. A failure becomes an
        # error field under that drive's own key instead of failing the
        # whole request with a 500.
        for key, cmd in SMART_CMDS.items():
            try:
                body[key] = _run_json_command(cmd)
            except Exception as e:
                body[key] = {"error": str(e)}

        try:
            body["storage_pools"] = _get_storage_pools()
        except Exception as e:
            body["storage_pools"] = {"error": str(e)}

        self._write_json(200, body)

    def _write_json(self, status: int, body: dict) -> None:
        data = json.dumps(body).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def main():
    server = HTTPServer((HOST, PORT), HealthHandler)
    print(f"health_endpoint listening on {HOST}:{PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()


if __name__ == "__main__":
    main()
