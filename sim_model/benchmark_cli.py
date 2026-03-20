from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Sequence

from .benchmark import append_benchmark_history, load_benchmark_history, run_ranking_benchmark


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run ranking benchmark and track lift history.")
    sub = parser.add_subparsers(dest="command", required=True)

    run_cmd = sub.add_parser("run", help="Run benchmark once.")
    run_cmd.add_argument("--worlds", type=int, default=200)
    run_cmd.add_argument("--candidates", type=int, default=12)
    run_cmd.add_argument("--top-k", type=int, default=3)
    run_cmd.add_argument("--seed", type=int, default=42)
    run_cmd.add_argument("--history-path", type=str, default=None)
    run_cmd.add_argument("--json", action="store_true")

    hist_cmd = sub.add_parser("history", help="Show compact stats from benchmark history.")
    hist_cmd.add_argument("--history-path", type=str, required=True)
    hist_cmd.add_argument("--last", type=int, default=10)
    hist_cmd.add_argument("--json", action="store_true")

    return parser


def _print_payload(payload: Dict[str, Any], as_json: bool) -> None:
    if as_json:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return
    for key, value in payload.items():
        print(f"{key}: {value}")


def _run_once(args: argparse.Namespace) -> int:
    result = run_ranking_benchmark(
        n_worlds=args.worlds,
        n_candidates=args.candidates,
        top_k=args.top_k,
        random_state=args.seed,
    )
    payload: Dict[str, Any] = {"benchmark_result": result}
    if args.history_path:
        entry = append_benchmark_history(result, args.history_path)
        payload["history_appended"] = {
            "path": str(Path(args.history_path).resolve()),
            "timestamp": entry["run_timestamp_utc"],
        }
    _print_payload(payload, as_json=args.json)
    return 0


def _history_summary(args: argparse.Namespace) -> int:
    rows = load_benchmark_history(args.history_path)
    if not rows:
        payload = {
            "history_path": str(Path(args.history_path).resolve()),
            "entries": 0,
            "message": "No history entries found.",
        }
        _print_payload(payload, as_json=args.json)
        return 0

    last_n = rows[-max(args.last, 1) :]
    funnel_top1 = [r.get("lifts", {}).get("funnel_vs_random_top1_lift", 0.0) for r in last_n]
    funnel_topk = [r.get("lifts", {}).get("funnel_vs_random_topk_lift", 0.0) for r in last_n]

    def _avg(seq: list[float]) -> float:
        return sum(seq) / len(seq) if seq else 0.0

    payload = {
        "history_path": str(Path(args.history_path).resolve()),
        "entries_total": len(rows),
        "entries_considered": len(last_n),
        "avg_funnel_vs_random_top1_lift": _avg(funnel_top1),
        "avg_funnel_vs_random_topk_lift": _avg(funnel_topk),
        "latest": rows[-1],
    }
    _print_payload(payload, as_json=args.json)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "run":
        return _run_once(args)
    if args.command == "history":
        return _history_summary(args)
    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
