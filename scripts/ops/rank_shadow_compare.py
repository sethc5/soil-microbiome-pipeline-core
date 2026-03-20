from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Sequence


def _read_rank_csv(path: str | Path) -> List[Dict[str, Any]]:
    csv_path = Path(path)
    with csv_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    for row in rows:
        row["rank"] = int(float(row.get("rank", 0) or 0))
        row["community_id"] = str(row.get("community_id", ""))
    return rows


def compute_shadow_metrics(
    legacy_rows: Sequence[Dict[str, Any]],
    hybrid_rows: Sequence[Dict[str, Any]],
    top_k: int,
) -> Dict[str, Any]:
    legacy_rank = {str(row["community_id"]): int(row["rank"]) for row in legacy_rows if row.get("community_id")}
    hybrid_rank = {str(row["community_id"]): int(row["rank"]) for row in hybrid_rows if row.get("community_id")}

    legacy_ids = list(legacy_rank.keys())
    hybrid_ids = list(hybrid_rank.keys())

    legacy_top = set(legacy_ids[:top_k])
    hybrid_top = set(hybrid_ids[:top_k])
    overlap = legacy_top & hybrid_top
    union = legacy_top | hybrid_top

    shared_ids = sorted(set(legacy_rank.keys()) & set(hybrid_rank.keys()))
    rank_shifts = [abs(legacy_rank[cid] - hybrid_rank[cid]) for cid in shared_ids]

    top_k_overlap_ratio = (len(overlap) / float(top_k)) if top_k > 0 else 0.0
    top_k_jaccard = (len(overlap) / float(len(union))) if union else 1.0
    mean_abs_rank_shift = (sum(rank_shifts) / float(len(rank_shifts))) if rank_shifts else 0.0
    max_abs_rank_shift = max(rank_shifts) if rank_shifts else 0

    return {
        "top_k": top_k,
        "counts": {
            "legacy_rows": len(legacy_rows),
            "hybrid_rows": len(hybrid_rows),
            "shared_rows": len(shared_ids),
        },
        "top_k_overlap": {
            "count": len(overlap),
            "ratio": top_k_overlap_ratio,
            "jaccard": top_k_jaccard,
            "legacy_top_ids": sorted(legacy_top),
            "hybrid_top_ids": sorted(hybrid_top),
            "overlap_ids": sorted(overlap),
        },
        "rank_displacement": {
            "mean_abs_rank_shift": mean_abs_rank_shift,
            "max_abs_rank_shift": max_abs_rank_shift,
        },
    }


def render_shadow_markdown(payload: Dict[str, Any]) -> str:
    overlap = payload.get("top_k_overlap", {})
    disp = payload.get("rank_displacement", {})
    checks = payload.get("checks", {})
    lines = [
        "## Rank Shadow Compare (Legacy vs Hybrid)",
        "",
        f"- Passed: **{'yes' if payload.get('passed') else 'no'}**",
        f"- DB: `{payload.get('db')}`",
        f"- Top-k compared: `{payload.get('top_k')}`",
        "",
        "| Metric | Value | Threshold |",
        "|---|---:|---:|",
        f"| top-k overlap ratio | {float(overlap.get('ratio', 0.0)):.4f} | >= {float(checks.get('min_top_k_overlap_ratio', 0.0)):.4f} |",
        f"| top-k jaccard | {float(overlap.get('jaccard', 0.0)):.4f} | n/a |",
        f"| mean abs rank shift | {float(disp.get('mean_abs_rank_shift', 0.0)):.4f} | <= {float(checks.get('max_mean_abs_rank_shift', 0.0)):.4f} |",
        f"| max abs rank shift | {int(disp.get('max_abs_rank_shift', 0))} | n/a |",
    ]
    failures = payload.get("failures", [])
    if failures:
        lines.extend(["", "### Failures"])
        for item in failures:
            lines.append(f"- {item}")
    return "\n".join(lines) + "\n"


def _run_rank_candidates(
    python_bin: str,
    config: str,
    db: str,
    output: str,
    top: int,
    scoring_mode: str,
    legacy_weight: float,
    uncertainty_samples: int,
    risk_aversion: float,
    uncertainty_seed: int,
) -> None:
    cmd = [
        python_bin,
        "-m",
        "core.analysis.rank_candidates",
        "--config",
        config,
        "--db",
        db,
        "--top",
        str(top),
        "--output",
        output,
        "--scoring-mode",
        scoring_mode,
        "--legacy-weight",
        f"{legacy_weight:.4f}",
        "--uncertainty-samples",
        str(uncertainty_samples),
        "--risk-aversion",
        f"{risk_aversion:.4f}",
        "--uncertainty-seed",
        str(uncertainty_seed),
    ]
    subprocess.run(cmd, check=True)


def _append_step_summary(markdown: str) -> str | None:
    summary_path = os.getenv("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return None
    path = Path(summary_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(markdown)
    return str(path.resolve())


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run rank_candidates in legacy and hybrid modes and compare rank displacement."
    )
    parser.add_argument("--config", required=True)
    parser.add_argument("--db", required=True)
    parser.add_argument("--top", type=int, default=50)
    parser.add_argument("--top-k", type=int, default=20)
    parser.add_argument("--legacy-output", default="/tmp/ranked_legacy.csv")
    parser.add_argument("--hybrid-output", default="/tmp/ranked_hybrid.csv")
    parser.add_argument("--legacy-weight", type=float, default=0.40)
    parser.add_argument("--uncertainty-samples", type=int, default=20)
    parser.add_argument("--risk-aversion", type=float, default=1.10)
    parser.add_argument("--uncertainty-seed", type=int, default=123)
    parser.add_argument("--min-top-k-overlap-ratio", type=float, default=0.60)
    parser.add_argument("--max-mean-abs-rank-shift", type=float, default=25.0)
    parser.add_argument("--report-json", default=None)
    parser.add_argument("--report-md", default=None)
    parser.add_argument("--append-step-summary", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    python_bin = sys.executable
    _run_rank_candidates(
        python_bin=python_bin,
        config=args.config,
        db=args.db,
        output=args.legacy_output,
        top=args.top,
        scoring_mode="legacy",
        legacy_weight=args.legacy_weight,
        uncertainty_samples=args.uncertainty_samples,
        risk_aversion=args.risk_aversion,
        uncertainty_seed=args.uncertainty_seed,
    )
    _run_rank_candidates(
        python_bin=python_bin,
        config=args.config,
        db=args.db,
        output=args.hybrid_output,
        top=args.top,
        scoring_mode="hybrid",
        legacy_weight=args.legacy_weight,
        uncertainty_samples=args.uncertainty_samples,
        risk_aversion=args.risk_aversion,
        uncertainty_seed=args.uncertainty_seed,
    )

    legacy_rows = _read_rank_csv(args.legacy_output)
    hybrid_rows = _read_rank_csv(args.hybrid_output)
    metrics = compute_shadow_metrics(legacy_rows, hybrid_rows, top_k=args.top_k)

    failures: List[str] = []
    ratio = float(metrics["top_k_overlap"]["ratio"])
    mean_shift = float(metrics["rank_displacement"]["mean_abs_rank_shift"])
    if ratio < args.min_top_k_overlap_ratio:
        failures.append(
            f"top_k_overlap_ratio={ratio:.4f} < min_top_k_overlap_ratio={args.min_top_k_overlap_ratio:.4f}"
        )
    if mean_shift > args.max_mean_abs_rank_shift:
        failures.append(
            f"mean_abs_rank_shift={mean_shift:.4f} > max_mean_abs_rank_shift={args.max_mean_abs_rank_shift:.4f}"
        )

    payload: Dict[str, Any] = {
        "passed": len(failures) == 0,
        "db": str(Path(args.db).resolve()),
        "legacy_output": str(Path(args.legacy_output).resolve()),
        "hybrid_output": str(Path(args.hybrid_output).resolve()),
        "top_k": args.top_k,
        "checks": {
            "min_top_k_overlap_ratio": args.min_top_k_overlap_ratio,
            "max_mean_abs_rank_shift": args.max_mean_abs_rank_shift,
        },
        "metrics": metrics,
        "top_k_overlap": metrics["top_k_overlap"],
        "rank_displacement": metrics["rank_displacement"],
        "failures": failures,
    }

    markdown = render_shadow_markdown(payload)
    if args.report_json:
        path = Path(args.report_json)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    if args.report_md:
        path = Path(args.report_md)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(markdown, encoding="utf-8")
    if args.append_step_summary:
        _append_step_summary(markdown)

    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(markdown)
    return 0 if payload["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
