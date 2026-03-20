from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Sequence

from .benchmark import load_benchmark_history


def _parse_iso_utc(value: str) -> datetime | None:
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def assess_history_freshness(history_path: str | Path, warn_if_older_than_days: int = 14) -> Dict[str, Any]:
    path = Path(history_path)
    rows = load_benchmark_history(path)
    latest_timestamp = None
    for row in reversed(rows):
        raw = row.get("run_timestamp_utc")
        if isinstance(raw, str):
            parsed = _parse_iso_utc(raw)
            if parsed is not None:
                latest_timestamp = parsed
                break

    payload: Dict[str, Any] = {
        "history_path": str(path),
        "entries": len(rows),
        "warn_if_older_than_days": warn_if_older_than_days,
        "stale": False,
        "has_timestamp": latest_timestamp is not None,
    }

    if latest_timestamp is None:
        payload["stale"] = True
        payload["reason"] = "missing_timestamp"
        return payload

    now_utc = datetime.now(timezone.utc)
    age_days = (now_utc - latest_timestamp).total_seconds() / 86400.0
    payload["latest_timestamp_utc"] = latest_timestamp.isoformat()
    payload["age_days"] = age_days
    payload["stale"] = age_days > float(warn_if_older_than_days)
    if payload["stale"]:
        payload["reason"] = "older_than_threshold"
    return payload


def render_history_markdown(payload: Dict[str, Any]) -> str:
    stale = bool(payload.get("stale"))
    status = "STALE" if stale else "FRESH"
    lines = [
        "## Benchmark Baseline Freshness",
        "",
        f"- Status: **{status}**",
        f"- Path: `{payload.get('history_path')}`",
        f"- Entries: `{payload.get('entries')}`",
    ]
    if payload.get("has_timestamp"):
        lines.append(f"- Latest timestamp (UTC): `{payload.get('latest_timestamp_utc')}`")
        lines.append(f"- Age days: `{float(payload.get('age_days', 0.0)):.2f}`")
    else:
        lines.append("- Latest timestamp (UTC): `n/a`")
    lines.append(f"- Stale threshold (days): `{payload.get('warn_if_older_than_days')}`")
    if stale:
        lines.append("- Warning: baseline history should be refreshed.")
    return "\n".join(lines) + "\n"


def write_step_summary(markdown: str) -> str | None:
    summary_path = os.getenv("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return None
    path = Path(summary_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(markdown)
    return str(path.resolve())


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Assess benchmark history freshness and optionally warn in CI.")
    parser.add_argument("--history-path", type=str, required=True)
    parser.add_argument("--warn-if-older-than-days", type=int, default=14)
    parser.add_argument("--append-step-summary", action="store_true")
    parser.add_argument("--fail-on-stale", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    payload = assess_history_freshness(
        history_path=args.history_path,
        warn_if_older_than_days=args.warn_if_older_than_days,
    )

    markdown = render_history_markdown(payload)
    if args.append_step_summary:
        summary_path = write_step_summary(markdown)
        if summary_path:
            payload["step_summary"] = summary_path

    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(markdown)

    if payload["stale"] and not args.json:
        # Always produce a visible warning on CI logs.
        print("::warning::Benchmark baseline appears stale.")
    if payload["stale"] and args.fail_on_stale:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
