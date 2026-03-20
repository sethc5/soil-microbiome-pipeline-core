from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sim_model.benchmark_gate import GateConfig, GateThresholds, evaluate_benchmark_gate, write_report_artifacts


def generate_templates(
    json_path: str | Path,
    markdown_path: str | Path,
) -> dict:
    config = GateConfig(
        seeds=[7, 13, 29],
        worlds=180,
        candidates=10,
        top_k=3,
        thresholds=GateThresholds(
            min_top1_lift=0.03,
            min_topk_lift=0.02,
            min_regret_reduction=0.25,
            min_hit_rate_margin=0.15,
        ),
    )
    payload = evaluate_benchmark_gate(config)
    payload["template_metadata"] = {
        "description": "Template example for PR benchmark artifacts.",
        "generator": "scripts/ops/generate_sim_model_report_templates.py",
    }
    write_report_artifacts(
        payload=payload,
        report_json_path=json_path,
        report_md_path=markdown_path,
        append_step_summary=False,
    )
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate template benchmark report artifacts for reviewers.")
    parser.add_argument(
        "--json-path",
        default="reference/sim_model_benchmark_latest.template.json",
        help="Output JSON template path.",
    )
    parser.add_argument(
        "--markdown-path",
        default="reference/sim_model_benchmark_summary.template.md",
        help="Output markdown template path.",
    )
    args = parser.parse_args()

    payload = generate_templates(args.json_path, args.markdown_path)
    print(
        json.dumps(
            {
                "passed": payload.get("passed"),
                "json_path": str(Path(args.json_path).resolve()),
                "markdown_path": str(Path(args.markdown_path).resolve()),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
