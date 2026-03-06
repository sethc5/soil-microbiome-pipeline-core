"""
scripts/setup_codespace.py — Codespace post-create setup for soil-microbiome-pipeline-core

Runs automatically after the devcontainer is created (postCreateCommand).
Creates a virtual environment, installs Python dependencies, creates required
directories, and prints a quick-start summary.

Designed to work on both Debian/Ubuntu (apt-get) and Alpine (apk) base images.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
VENV = Path("/workspaces/venv")
VENV_PY = VENV / "bin" / "python"


def run(cmd: list[str], desc: str, check: bool = True) -> bool:
    print(f"\n▶ {desc}...")
    result = subprocess.run(cmd, cwd=ROOT)
    ok = result.returncode == 0
    print(f"  {'✓' if ok else '✗'} {'Done' if ok else f'Failed (exit {result.returncode})'}")
    return ok


def install_system_deps() -> None:
    """Install GLPK/SWIG system libs needed to build cobra's swiglpk."""
    if shutil.which("apt-get"):
        print("\n▶ Installing system build deps via apt-get...")
        subprocess.run(
            ["sudo", "apt-get", "install", "-y", "-q",
             "libglpk-dev", "swig", "libgmp-dev", "build-essential"],
            check=False,
        )
    elif shutil.which("apk"):
        print("\n▶ Installing system build deps via apk...")
        subprocess.run(
            ["sudo", "apk", "add", "--no-cache",
             "glpk-dev", "swig", "gmp-dev", "build-base"],
            check=False,
        )
    else:
        print("  ⚠ No known package manager — skipping system deps (cobra may fail)")


def ensure_venv() -> None:
    """Create /workspaces/venv if it doesn't exist."""
    if not VENV_PY.exists():
        print(f"\n▶ Creating virtual environment at {VENV}...")
        subprocess.run([sys.executable, "-m", "venv", str(VENV)], check=True)
        print("  ✓ venv created")
    else:
        print(f"\n▶ venv already exists at {VENV}")


def main() -> None:
    print("=" * 60)
    print("  Soil Microbiome Pipeline — Codespace Setup")
    print("=" * 60)

    # 1. Install system-level build deps for cobra
    install_system_deps()

    # 2. Create virtual environment
    ensure_venv()
    pip = [str(VENV_PY), "-m", "pip"]

    # 3. Upgrade pip inside venv
    run([*pip, "install", "--upgrade", "pip", "-q"], "Upgrading pip")

    # 4. Install core Python deps
    core_deps = [
        "pydantic>=2.0", "typer>=0.9", "rich>=13.0", "pyyaml>=6.0",
        "requests>=2.31", "tqdm>=4.66", "joblib>=1.3",
    ]
    run([*pip, "install", "-q"] + core_deps, "Installing core dependencies")

    # 5. Install science stack
    sci_deps = [
        "numpy>=1.25", "scipy>=1.11", "pandas>=2.0", "matplotlib>=3.8",
        "scikit-learn>=1.3", "statsmodels>=0.14",
    ]
    run([*pip, "install", "-q"] + sci_deps, "Installing science stack")

    # 6. Install bio packages (cobra requires GLPK built above)
    bio_deps = ["cobra>=0.29", "biopython>=1.81", "biom-format>=2.1"]
    if not run([*pip, "install", "-q"] + bio_deps, "Installing bio packages"):
        print("  ⚠ Bio packages failed — T0 filter mode will still work")

    # 7. scikit-bio (optional, sometimes needs --no-build-isolation)
    if not run([*pip, "install", "-q", "scikit-bio>=0.6"],
               "Installing scikit-bio (optional)", check=False):
        print("  ⚠ scikit-bio install failed — diversity metrics will be limited")

    # 8. Create runtime directories
    for d in ["receipts", "results", "reference"]:
        (ROOT / d).mkdir(exist_ok=True)
    print("\n▶ Runtime directories ready")

    # 9. Quick smoke test using the venv python
    ok = run(
        [str(VENV_PY), "-m", "pytest", "tests/", "-q", "--tb=no", "-x"],
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
