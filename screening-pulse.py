"""Run the existing historical screener on a trusted, private Actions artifact."""
import argparse
import json
import math
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen

REPO = "mondaytv/atlas-bot"
WORKFLOW = "atlas-research.yml"
ARTIFACT = "atlas-screening-input"
INTERVAL_SECONDS = 300
EVENT_CODES = {
    "INPUT_READY", "ENTRY_FILTERED", "SIMULATION_PROGRESS", "SHORTLIST_FROZEN",
    "CANDIDATE_PROPOSED", "CANDIDATE_RECHECKED", "CANDIDATE_REVIEWED", "SCREEN_COMPLETE",
    "DISCOVERY_GENERATED",
}
EVENT_FIELDS = {
    "epoch", "assets", "bars", "paths", "grid", "done", "total", "nominees", "closed",
    "pnl", "seconds", "passed",
    "new_rules", "total_rules", "batch", "retained", "rejected",
}
GATE_KEYS = {
    "minimum_closed_trades", "positive_net_pnl", "profit_factor", "drawdown",
    "single_trade", "single_asset", "beats_buy_hold",
}


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
        present = {file.name for file in bundle.iterdir() if file.is_file()}
        if not required <= present or not present <= required | {"discovery_rules.py", "evolve_research.py", "explore_research.py", "lab_v2_rules.py", "lab_v2.py"}:
            raise ValueError("Incomplete screening bundle")
        write_json(root / "bundle.json", {"path": str(bundle), "run": result})
        return bundle, False
    except (OSError, ValueError, subprocess.SubprocessError):
        if current and (current / "input.json").is_file():
            return current, True
        return None, False


def public_events(result):
    """Publish typed calculation evidence. Recipe contents and arbitrary text stay private."""
    clean = []
    raw = result.get("events")
    if not isinstance(raw, list):
        return clean
    for event in raw[:32]:
        if not isinstance(event, dict) or not isinstance(event.get("code"), str) or event["code"] not in EVENT_CODES:
            continue
        try:
            stamp = datetime.fromisoformat(event["at"])
            if stamp.tzinfo is None:
                continue
            fields = event["data"]
            if not isinstance(fields, dict):
                continue
        except (ValueError, KeyError, TypeError):
            continue
        data = {}
        for key, value in fields.items():
            if key in EVENT_FIELDS and (type(value) is bool or type(value) in (int, float) and math.isfinite(value)):
                data[key] = value
            elif key == "candidate_id" and isinstance(value, str) and re.fullmatch("[0-9a-f]{20}", value):
                data[key] = value
            elif key == "failed_gates" and isinstance(value, list):
                data[key] = [name for name in value if isinstance(name, str) and name in GATE_KEYS]
        clean.append({"code": event["code"], "at": stamp.isoformat(), "data": data})
    return clean


def simulate(bundle, dependency_dir, output, memory=None, slot=None):
    # The downloaded calculation gets public market data and no dispatch token.
    environ = {key: value for key, value in os.environ.items() if key not in {"GH_TOKEN", "GITHUB_TOKEN"}}
    environ["PYTHONPATH"] = str(dependency_dir)
    output.unlink(missing_ok=True)
    adaptive = memory is not None and (bundle / "evolve_research.py").is_file()
    command = [sys.executable, str(bundle / ("evolve_research.py" if adaptive else "screen_research.py")),
               str(bundle / "input.json"), str(output)]
    if adaptive:
        command.extend(["--memory", str(memory), "--slot", str(slot)])
    subprocess.run(
        command,
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
    for key in ("discovery_batch", "new_entry_rules", "total_entry_rules", "total_simulations",
                "saved_candidates", "rejected_total", "rule_space_size"):
        value = result.get(key)
        if type(value) is int and value >= 0:
            summary[key] = value
    return dict(summary, version=result.get("version"), elapsed_seconds=elapsed,
                training_input_hash=dataset, events=public_events(result))


def run_exploration(bundle, dependency_dir, work, output):
    environ = {key: value for key, value in os.environ.items() if key not in {"GH_TOKEN", "GITHUB_TOKEN"}}
    environ["PYTHONPATH"] = str(dependency_dir)
    state_path = output.with_name("exploration-state.json")
    temporary = work / "pending-exploration-state.json"
    if state_path.is_file():
        shutil.copyfile(state_path, temporary)
    else:
        temporary.unlink(missing_ok=True)
    result_path = work / "exploration.json"
    result_path.unlink(missing_ok=True)
    subprocess.run([sys.executable, str(bundle / "explore_research.py"), str(work / "screening.json"),
                    "--state", str(temporary), "--output", str(result_path)],
                   env=environ, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=150, check=True)
    result = read_json(result_path)
    if result.get("version") != "atlas-exploration-v1" or result.get("paper_only") is not True:
        raise ValueError("Invalid exploratory PAPER report")
    write_json(state_path, read_json(temporary))
    return result


def run_lab_v2(bundle, dependency_dir, work, output, seed):
    environ = {key: value for key, value in os.environ.items() if key not in {"GH_TOKEN", "GITHUB_TOKEN"}}
    environ["PYTHONPATH"] = str(dependency_dir)
    state_path = output.with_name("lab-v2-state.json")
    temporary = work / "pending-lab-v2-state.json"
    if state_path.is_file():
        shutil.copyfile(state_path, temporary)
    else:
        temporary.unlink(missing_ok=True)
    seed_path, result_path = work / "v2-prior-observations.json", work / "lab-v2.json"
    write_json(seed_path, seed or {})
    result_path.unlink(missing_ok=True)
    subprocess.run([sys.executable, str(bundle / "lab_v2.py"), str(bundle / "input.json"),
                    "--seed", str(seed_path), "--state", str(temporary), "--output", str(result_path)],
                   env=environ, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=180, check=True)
    result = read_json(result_path)
    if result.get("version") != "atlas-laboratory-v2" or result.get("paper_only") is not True:
        raise ValueError("Invalid Laboratory V2 report")
    write_json(state_path, read_json(temporary))
    return result


def dispatch_discovered(proposals, previous, now):
    if proposals.get("version") != "atlas-discovery-v1" or not proposals.get("candidates"):
        return "NO_SAVED_CANDIDATE", previous.get("last_admission_request")
    try:
        request = Request("https://api.github.com/repos/mondaytv/atlas-dashboard/contents/research.json?ref=research&t="+str(now),
                          headers={"User-Agent": "ATLAS-PAPER-Admission-Relay/1", "Accept": "application/vnd.github.raw+json"})
        with urlopen(request, timeout=8) as response:
            report = json.load(response)
        lane = report["screening_paper"]
        accounts = lane["accounts"]
        if any(row.get("position") or row.get("status") not in {"RETIRED", "RESEARCH_STOPPED"} for row in accounts):
            return "PAPER_ACCOUNT_ACTIVE", previous.get("last_admission_request")
        tried = {row["id"].removeprefix("screen:") for row in accounts}
        pending = [row for row in proposals["candidates"] if row["id"] not in tried]
        if not pending:
            return "ALREADY_TESTED", previous.get("last_admission_request")
        candidate = pending[0]["id"]
        last = previous.get("last_admission_request") or {}
        if last.get("id") == candidate and now-last.get("at", 0) < 1800:
            return "WAITING_FOR_PAPER_CONFIRMATION", last
        gh(["workflow", "run", WORKFLOW, "--repo", REPO, "--ref", "main"], 15)
        return "PAPER_VERIFICATION_REQUESTED", {"id": candidate, "at": now}
    except (OSError, KeyError, ValueError, TypeError, subprocess.SubprocessError):
        return "PAPER_RELAY_UNAVAILABLE", previous.get("last_admission_request")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--dependency-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--dispatch-only", action="store_true")
    args = parser.parse_args()
    now = int(time.time())
    slot = now // INTERVAL_SECONDS
    previous = read_json(args.output)
    proposals_path = args.output.with_name("screening-proposals.json")
    memory_path = args.output.with_name("evolution-memory.json")
    if args.dispatch_only:
        if previous.get("mode") != "HISTORICAL_SCREENING_5M":
            print("No screening snapshot available for admission relay")
            return
        status, request = dispatch_discovered(read_json(proposals_path), previous, now)
        previous["admission_status"] = status
        previous["last_admission_request"] = request
        write_json(args.output, previous)
        print("Autonomous PAPER admission: "+status)
        return
    if previous.get("slot") == slot and previous.get("status") == "COMPLETE" and previous.get("journal_version") == 5:
        print("Five-minute screening already completed for this slot")
        return
    args.work_dir.mkdir(parents=True, exist_ok=True)
    row = {"schema": 1, "journal_version": 5, "mode": "HISTORICAL_SCREENING_5M", "slot": slot, "interval_minutes": 5,
           "checked_at": iso(now), "stale_after_minutes": 12, "status": "INPUT_UNAVAILABLE",
           "last_success": previous.get("last_success"), "exploration": previous.get("exploration"),
           "exploration_status": "INPUT_UNAVAILABLE", "lab_v2": previous.get("lab_v2"), "lab_v2_status": "INPUT_UNAVAILABLE",
           "recent_runs": [run for run in (previous.get("recent_runs") or []) if isinstance(run, dict)][-5:],
           "last_admission_request": previous.get("last_admission_request"),
           "admission_status": previous.get("admission_status", "NO_SAVED_CANDIDATE")}
    bundle, cached = get_bundle(args.work_dir)
    if bundle:
        try:
            temporary_memory = args.work_dir / "evolution-memory.json"
            if memory_path.is_file():
                shutil.copyfile(memory_path, temporary_memory)
            else:
                temporary_memory.unlink(missing_ok=True)
            result = simulate(bundle, args.dependency_dir.resolve(), args.work_dir / "screening.json",
                              temporary_memory, slot)
            raw = read_json(args.work_dir / "screening.json")
            if result.get("version") == "atlas-discovery-v1":
                if temporary_memory.is_file():
                    write_json(memory_path, read_json(temporary_memory))
                keys = ("version", "mode", "generated_at", "epoch", "epoch_dataset_hash", "policy_hash",
                        "training_input_hash", "screen_from", "screen_to", "recheck_from", "recheck_to", "candidates")
                write_json(proposals_path, {key: raw[key] for key in keys})
            last_success = previous.get("last_success") or {}
            row["last_success"] = dict(result, completed_at=iso(time.time()),
                                       same_input=result["training_input_hash"] == last_success.get("training_input_hash"))
            row["status"] = "COMPLETE"
            row["cached_input"] = cached
            row["recent_runs"] = [run for run in row["recent_runs"] if run.get("slot") != slot]
            row["recent_runs"].append(dict(row["last_success"], slot=slot))
        except (OSError, ValueError, KeyError, subprocess.SubprocessError):
            row["status"] = "CALCULATION_FAILED"
        if (bundle / "explore_research.py").is_file():
            try:
                row["exploration"] = run_exploration(bundle, args.dependency_dir.resolve(), args.work_dir, args.output)
                row["exploration_status"] = "COMPLETE"
            except (OSError, ValueError, KeyError, subprocess.SubprocessError):
                row["exploration_status"] = "CALCULATION_FAILED"
        if (bundle / "lab_v2.py").is_file():
            try:
                row["lab_v2"] = run_lab_v2(bundle, args.dependency_dir.resolve(), args.work_dir, args.output, row.get("exploration"))
                row["lab_v2_status"] = "COMPLETE"
            except (OSError, ValueError, KeyError, subprocess.SubprocessError):
                row["lab_v2_status"] = "CALCULATION_FAILED"
    write_json(args.output, row)
    count = row["last_success"]["executed_simulations"] if row["status"] == "COMPLETE" else 0
    print(f"Five-minute screening: {row['status']}; {count} simulations in slot {slot}")


if __name__ == "__main__":
    main()
