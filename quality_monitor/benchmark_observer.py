"""Poll the real serving API inside an isolated benchmark container."""

import argparse
import json
import math
import subprocess
import sys
import time
from pathlib import Path
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import urlopen

from quality_monitor.benchmark_metrics import emit, utc_now


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--poll-seconds", type=float, default=5)
    args = parser.parse_args()
    if not math.isfinite(args.poll_seconds) or args.poll_seconds <= 0:
        parser.error("Polling interval must be positive and finite")
    server = subprocess.Popen([sys.executable, "-m", "uvicorn", "quality_monitor.api:app", "--host", "127.0.0.1", "--port", "8000"])
    ready = False
    try:
        with (args.root / "api-observations.jsonl").open("x") as handle:
            while not (args.root / "stop-api").exists():
                if server.poll() is not None:
                    raise RuntimeError("Benchmark API exited")
                requests = []
                for endpoint, filters in (
                    ("overview", {"run_id": args.run_id}),
                    ("producer-status", {"run_id": args.run_id}),
                    *(("inspections", {"run_id": args.run_id, "line_id": line, "category": "bottle",
                                       "model_version": "fixture-benchmark-v1", "threshold_version": "fixture-0.5"})
                      for line in ("A", "B")),
                ):
                    started = time.monotonic()
                    request = {"endpoint": endpoint, "error": None, "visible_inspection_ids": []}
                    try:
                        with urlopen("http://127.0.0.1:8000/api/" + endpoint + "?" + urlencode(filters), timeout=5) as response:
                            body = json.load(response)
                        request["state"] = body["state"]
                        if endpoint == "overview":
                            request["served_inspections"] = sum(row["inspections"] for row in body["stations"])
                        if endpoint == "inspections":
                            request["visible_inspection_ids"] = [row["inspection_id"] for row in body["inspections"]]
                        if not ready:
                            emit({"kind": "ready", "stage": "api"})
                            ready = True
                    except (URLError, OSError, ValueError) as error:
                        request["error"] = str(error)
                    request["seconds"] = time.monotonic() - started
                    request["observed_monotonic"] = time.monotonic()
                    requests.append(request)
                value = {"kind": "api", "observed_at": utc_now(), "observed_monotonic": time.monotonic(), "requests": requests}
                handle.write(json.dumps(value) + "\n")
                handle.flush()
                emit(value)
                deadline = time.monotonic() + args.poll_seconds
                while time.monotonic() < deadline and not (args.root / "stop-api").exists():
                    time.sleep(min(.25, max(0, deadline - time.monotonic())))
    finally:
        server.terminate()
        try:
            server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait()


if __name__ == "__main__":
    main()
