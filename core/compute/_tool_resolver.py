"""
compute/_tool_resolver.py — Discover bioinformatics tools in conda environments.

When a tool is not in the system PATH (common on freshly provisioned servers),
this module searches for it inside known conda environments under CONDA_BASE.

Resolution order per tool:
  1. System PATH (shutil.which)
  2. Each conda env's bin/ directory, in priority order
  3. Not found → returns None

Usage in compute modules::

    from core.compute._tool_resolver import resolve_tool, extend_path_for

    prokka = resolve_tool("prokka")
    if prokka is None:
        logger.warning("Prokka not found")
    else:
        subprocess.run([prokka, ...])

The function also extends os.environ["PATH"] the first time a tool is found
in a conda env, so subsequent shutil.which() calls work too.
"""

from __future__ import annotations

import logging
import os
import shutil
from functools import lru_cache
from pathlib import Path

logger = logging.getLogger(__name__)

# ── Search roots ──────────────────────────────────────────────────────────────
# We check these directories for conda installations, in order.
_CONDA_ROOTS: list[Path] = [
    Path(os.environ.get("CONDA_PREFIX", "")).parent if os.environ.get("CONDA_PREFIX") else Path(),
    Path(os.environ.get("CONDA_BASE", "")),
    Path.home() / "miniforge3",
    Path.home() / "miniconda3",
    Path.home() / "anaconda3",
    Path("/opt/conda"),
    Path("/opt/miniforge3"),
    Path("/opt/miniconda3"),
]

# Per-tool preferred conda env names, tried in order.
# Matches the envs created in deploy/toolinstall3.
_TOOL_ENV_PRIORITY: dict[str, list[str]] = {
    "prokka":             ["bioinfo", "prokka", "base"],
    "mmseqs":             ["bioinfo", "mmseqs2", "base"],
    "mmseqs2":            ["bioinfo", "mmseqs2", "base"],
    "picrust2_pipeline":  ["picrust2", "picrust2-env", "base"],
    "picrust2_pipeline.py": ["picrust2", "picrust2-env", "base"],
    "checkm":             ["checkm", "checkm-genome", "base"],
    "diamond":            ["bioinfo", "base"],
    "antismash":          ["antismash", "base"],
}


@lru_cache(maxsize=64)
def resolve_tool(cmd: str) -> str | None:
    """
    Return the absolute path to *cmd*, or None if not found anywhere.

    Results are cached per-process so the expensive directory scan only
    runs once per tool per interpreter lifetime.
    """
    # 1. Fast path: already in PATH
    found = shutil.which(cmd)
    if found:
        return found

    # 2. Search conda envs
    env_names = _TOOL_ENV_PRIORITY.get(cmd, ["base"])

    for conda_root in _CONDA_ROOTS:
        if not conda_root or not conda_root.is_dir():
            continue
        envs_dir = conda_root / "envs"

        # Try priority envs first, then any env directory
        candidates: list[Path] = []
        for env_name in env_names:
            ep = envs_dir / env_name
            if ep.is_dir():
                candidates.append(ep)
        # Also try conda_root itself (base env)
        if conda_root.is_dir():
            candidates.append(conda_root)

        for env_path in candidates:
            bin_path = env_path / "bin" / cmd
            if bin_path.is_file() and os.access(bin_path, os.X_OK):
                logger.info(
                    "Tool '%s' resolved via conda env at %s",
                    cmd, bin_path,
                )
                _extend_path(str(env_path / "bin"))
                return str(bin_path)

    logger.debug("Tool '%s' not found in PATH or any conda environment.", cmd)
    return None


def extend_path_for(cmd: str) -> bool:
    """
    Resolve *cmd* and, if found in a conda env, add its bin dir to PATH.
    Returns True if the tool is now available.
    """
    path = resolve_tool(cmd)
    return path is not None


def _extend_path(bin_dir: str) -> None:
    """Prepend *bin_dir* to os.environ PATH if not already present."""
    current = os.environ.get("PATH", "")
    if bin_dir not in current.split(os.pathsep):
        os.environ["PATH"] = bin_dir + os.pathsep + current
