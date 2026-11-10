"""Run the existing historical screener on a trusted, private Actions artifact."""
import argparse
import json
import math
import os
import re
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

REPO = "mondaytv/atlas-bot"
WORKFLOW = "atlas-research.yml"
ARTIFACT = "atlas-screening-input"
INTERVAL_SECONDS = 300


def iso(timestamp):
    return datetime.fromtimestamp(timestamp, timezone.utc).isoformat()


def read_json(path):
    try:
        result = json.loads(path.read_text(encoding="utf-8"))
        return result if isinstance(result, dict) else {}
    except (OSError, ValueError):
        return {}


def write_json(path, data):
    temp = path.with_suffix(".tmp")
    temp.write_text(json.dumps(data, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    temp.replace(path)


def gh(args, timeout):
    return subprocess.run(["gh", *args], capture_output=True, text=True, timeout=timeout, check=True).stdout.strip()


def get_bundle(root):
    cached = read_json(root / "bundle.json")
    current = Path(cached["path"]) if cached.get("path") else None
    try:
        result = gh(["api", f"repos/{REPO}/actions/workflows/{WORKFLOW}/runs?status=success&branch=main&per_page=5",
                     "--jq", ".workflow_runs[0].id // empty"], 12)
        if not result.isdecimal():
            raise ValueError("No trusted completed research run")
        if current and cached.get("run") == result and (current / "input.json").is_file():
            return current, False
        bundle = Path(tempfile.mkdtemp(prefix="input-", dir=root))
        gh(["run", "download", result, "--repo", REPO, "--name", ARTIFACT, "--dir", str(bundle)], 25)
        required = {"atlas_research.py", "screen_research.py", "policy.json", "input.json"}
        if {file.name for file in bundle.iterdir() if file.is_file()} != required:
            raise ValueError("Incomplete screening bundle")
        write_json(root / "bundle.json", {"path": str(bundle), "run": result})
        return bundle, False
    except (OSError, ValueError, subprocess.SubprocessError):
        if current and (current / "input.json").is_file():
            return current, True
        return None, False


def simulate(bundle, dependency_dir, output):
    # The downloaded calculation gets public market data and no dispatch token.
    environ = {key: value for key, value in os.environ.items() if key not in {"GH_TOKEN", "GITHUB_TOKEN"}}
    environ["PYTHONPATH"] = str(dependency_dir)
    output.unlink(missing_ok=True)
    subprocess.run(
        [sys.executable, str(bundle / "screen_research.py"), str(bundle / "input.json"), str(output)],
        env=environ, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=60, check=True,
    )
    result = read_json(output)
    if result.get("mode") != "PAPER_HISTORICAL_SCREEN_ONLY":
        raise ValueError("Unexpected result")
    fields = ("epoch", "executed_simulations", "distinct_trade_fingerprints", "candidates_rechecked", "passed_historical_recheck")
    summary = {name: result[name] for name in fields}
    if any(type(value) is not int or value < 0 for value in summary.values()):
        raise ValueError("Invalid count")
    elapsed = result["elapsed_seconds"]
    if type(elapsed) not in (int, float) or not math.isfinite(elapsed) or elapsed < 0:
        raise ValueError("Invalid duration")
    dataset = result["training_input_hash"]
    if not isinstance(dataset, str) or not re.fullmatch("[0-9a-f]{64}", dataset):
        raise ValueError("Invalid training digest")
    return dict(summary, elapsed_seconds=elapsed, training_input_hash=dataset)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--dependency-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    now = int(time.time())
    slot = now // INTERVAL_SECONDS
    previous = read_json(args.output)
    if previous.get("slot") == slot and previous.get("status") == "COMPLETE":
        print("Five-minute screening already completed for this slot")
        return
    args.work_dir.mkdir(parents=True, exist_ok=True)
    row = {"schema": 1, "mode": "HISTORICAL_SCREENING_5M", "slot": slot, "interval_minutes": 5,
           "checked_at": iso(now), "stale_after_minutes": 12, "status": "INPUT_UNAVAILABLE",
           "last_success": previous.get("last_success")}
    bundle, cached = get_bundle(args.work_dir)
    if bundle:
        try:
            result = simulate(bundle, args.dependency_dir.resolve(), args.work_dir / "screening.json")
            last_success = previous.get("last_success") or {}
            row["last_success"] = dict(result, completed_at=iso(time.time()),
                                       same_input=result["training_input_hash"] == last_success.get("training_input_hash"))
            row["status"] = "COMPLETE"
            row["cached_input"] = cached
        except (OSError, ValueError, KeyError, subprocess.SubprocessError):
            row["status"] = "CALCULATION_FAILED"
    write_json(args.output, row)
    count = row["last_success"]["executed_simulations"] if row["status"] == "COMPLETE" else 0
    print(f"Five-minute screening: {row['status']}; {count} simulations in slot {slot}")


if __name__ == "__main__":
    main()
