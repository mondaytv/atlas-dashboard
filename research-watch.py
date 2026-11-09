"""Public Atlas research watchdog. No trade state or exchange credentials."""
import argparse
import json
import os
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

REPORT = "https://raw.githubusercontent.com/mondaytv/atlas-dashboard/research/research.json"
KRAKEN = "https://api.kraken.com/0/public/OHLC"
DECISION_SECONDS = 4 * 3600
CHECK_SECONDS = 30 * 60
PAIRS = {"BTC": "XBTEUR", "ETH": "ETHEUR", "XRP": "XRPEUR", "SOL": "SOLEUR", "LINK": "LINKEUR"}


def iso(ts):
    return datetime.fromtimestamp(ts, timezone.utc).isoformat()


def fetch(url):
    request = Request(url, headers={"User-Agent": "ATLAS-PAPER-Research-Watch/1", "Cache-Control": "no-cache"})
    with urlopen(request, timeout=8) as response:
        return json.load(response)


def market_check(pair, now):
    expected = now // DECISION_SECONDS * DECISION_SECONDS - DECISION_SECONDS
    try:
        response = fetch(KRAKEN + "?" + urlencode({"pair": pair, "interval": 240, "since": expected - 1}))
        if response["error"]:
            raise ValueError("Kraken error")
        rows = next(value for key, value in response["result"].items() if key != "last")
        closed = [int(row[0]) for row in rows if int(row[0]) + DECISION_SECONDS <= now]
        last = max(closed, default=None)
        return {"state": "READY" if last == expected else "WAITING_CANDLE", "last_closed_candle": iso(last) if last is not None else None}
    except (OSError, ValueError, TypeError, KeyError, IndexError, StopIteration):
        return {"state": "UNAVAILABLE", "last_closed_candle": None}


def request_research():
    if not os.environ.get("GH_TOKEN"):
        return False
    try:
        result = subprocess.run(
            ["gh", "workflow", "run", "atlas-research.yml", "--repo", "mondaytv/atlas-bot", "--ref", "main"],
            capture_output=True, timeout=12, check=False,
        )
        return result.returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


def observe(now, get_report, check_market, dispatch):
    with ThreadPoolExecutor(max_workers=len(PAIRS)) as pool:
        readings = dict(zip(PAIRS, pool.map(lambda pair: check_market(pair, now), PAIRS.values())))
    ready = sum(market["state"] == "READY" for market in readings.values())
    report_at = None
    try:
        report = get_report()
        if report.get("mode") != "ISOLATED_AUTONOMOUS_PAPER_RESEARCH" or report.get("paper_only") is not True:
            raise ValueError("Unexpected research report")
        report_at = datetime.fromisoformat(report["generated_at"]).timestamp()
    except (OSError, ValueError, TypeError, KeyError, AttributeError, OverflowError):
        status = "REPORT_UNAVAILABLE"
    else:
        if report_at // DECISION_SECONDS >= now // DECISION_SECONDS:
            status = "RESEARCH_DATA_INCOMPLETE" if report.get("errors") else "UP_TO_DATE"
        elif not ready:
            status = "WAITING_FOR_MARKET"
        else:
            status = "DISPATCH_REQUESTED" if dispatch() else "DISPATCH_FAILED"
    return {
        "schema": 1, "mode": "PAPER_RESEARCH_MARKET_WATCH", "checked_at": iso(now),
        "check_interval_minutes": CHECK_SECONDS // 60, "stale_after_minutes": 75,
        "decision_interval_minutes": DECISION_SECONDS // 60, "status": status,
        "ready_markets": ready, "total_markets": len(PAIRS), "market": readings,
        "last_report_at": iso(report_at) if report_at is not None else None,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    now = int(time.time())
    if args.output.exists():
        previous = json.loads(args.output.read_text(encoding="utf-8"))
        previous_check = datetime.fromisoformat(previous["checked_at"]).timestamp()
        if previous_check // CHECK_SECONDS >= now // CHECK_SECONDS:
            print("Market check already recorded for this slot.")
            return
    snapshot = observe(now, lambda: fetch(REPORT + "?t=" + str(now)), market_check, request_research)
    pending = args.output.with_suffix(".tmp")
    pending.write_text(json.dumps(snapshot, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    pending.replace(args.output)
    print(f"Research watch: {snapshot['status']}; {snapshot['ready_markets']}/{snapshot['total_markets']} markets ready")


if __name__ == "__main__":
    main()
