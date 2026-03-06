"""
scripts/setup_codespace.py — Codespace post-create setup for soil-microbiome-pipeline-core

Runs automatically after the devcontainer is created (postCreateCommand).
Installs Python dependencies, creates required directories, and prints
a quick-start summary.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def run(cmd: list[str], desc: str) -> bool:
    print(f"\n▶ {desc}...")
    result = subprocess.run(cmd, cwd=ROOT)
    if result.returncode != 0:
        print(f"  ✗ Failed (exit {result.returncode})")
        return False
    print(f"  ✓ Done")
    return True


def main() -> None:
    print("=" * 60)
    print("  Soil Microbiome Pipeline — Codespace Setup")
    print("=" * 60)

    # 1. Upgrade pip
    run([sys.executable, "-m", "pip", "install", "--upgrade", "pip", "-q"], "Upgrading pip")

    # 2. Install Python requirements (pip-installable subset only)
    #    Non-Python tools (kraken2, mmseqs2, etc.) are not available in Codespaces
    #    by default — T0 metadata/filter mode works without them.
    pip_only = [
        "pydantic>=2.0", "typer>=0.9", "rich>=13.0", "pyyaml>=6.0",
        "scikit-learn>=1.3", "numpy>=1.25", "scipy>=1.11", "pandas>=2.0",
        "cobra>=0.29", "biopython>=1.81", "biom-format>=2.1",
        "statsmodels>=0.14", "joblib>=1.3", "requests>=2.31", "tqdm>=4.66",
        "matplotlib>=3.8",
    ]
    run(
        [sys.executable, "-m", "pip", "install", "--quiet"] + pip_only,
        "Installing Python dependencies",
    )

    # 3. Install scikit-bio separately (sometimes needs --no-build-isolation)
    result = subprocess.run(
        [sys.executable, "-m", "pip", "install", "--quiet", "scikit-bio>=0.6"],
        cwd=ROOT,
    )
    if result.returncode != 0:
        print("  ⚠ scikit-bio install failed — diversity metrics will be limited")
    else:
        print("  ✓ scikit-bio installed")

    # 4. Create runtime directories
    for d in ["receipts", "results", "reference"]:
        (ROOT / d).mkdir(exist_ok=True)
    print("\n▶ Runtime directories ready")

    # 5. Quick smoke test
    ok = run(
        [sys.executable, "-m", "pytest", "tests/", "-q", "--tb=no", "-x"],
        "Running test suite (smoke test)",
    )

    # 6. Print quick-start
    print("\n" + "=" * 60)
    print("  READY — Quick-start commands")
    print("=" * 60)
    print("""
  T0 test run (50 samples, ~10 min):
    python batch_runner.py --config config.example.yaml --tier 0 --limit 50

  Validate a config:
    python config_schema.py --validate config.example.yaml

  Run full test suite:
    python -m pytest tests/ -q

  NOTE: Non-Python tools (kraken2, mmseqs2, prokka, hmmer) are not installed.
  T0 will run in metadata-filter + keyword-scan mode.
  For full T1/T2 runs, use the Hetzner node.
""")
    if not ok:
        print("  ⚠ Some tests failed — check output above before running.")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
