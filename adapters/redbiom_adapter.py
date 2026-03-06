"""
redbiom_adapter.py — Redbiom (Qiita search layer) adapter.

Redbiom provides fast feature-centric search across all public Qiita data:
  https://github.com/biocore/redbiom

Query by ASV / OTU, taxon name, or metadata field. Used here to pull
all Qiita samples that contain a target taxon (e.g., known nifH-containing
genera) regardless of the originating study.

Usage:
  adapter = RedbiomAdapter(config)
  samples = adapter.search_by_taxon("Azospirillum")
"""

from __future__ import annotations
import json
import logging
import shutil
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)

_REDBIOM_URL = "https://redbiom.qiita.ucsd.edu"
_DEFAULT_CONTEXT = "Deblur_2021.09-Illumina-16S-V4-150nt-780653"


class RedbiomAdapter:
    """Redbiom search adapter — fast feature-centric SRA/Qiita cross-query.

    Requires the ``redbiom`` Python package:
        pip install redbiom
    or via conda:
        conda install -c conda-forge redbiom
    """
    SOURCE = "redbiom"

    def __init__(self, config: dict):
        self.config = config
        self._context = config.get("redbiom_context", _DEFAULT_CONTEXT)
        self._server = config.get("redbiom_server", _REDBIOM_URL)

    def _redbiom_available(self) -> bool:
        return shutil.which("redbiom") is not None

    def _cli(self, *args: str, timeout: int = 120) -> str | None:
        """Run redbiom CLI and return stdout, or None if unavailable/failed."""
        if not self._redbiom_available():
            return None
        try:
            import os as _os
            env = {**_os.environ, "REDBIOM_HOST": self._server}
            result = subprocess.run(
                ["redbiom", *args],
                capture_output=True, text=True, timeout=timeout, check=True,
                env=env,
            )
            return result.stdout.strip()
        except subprocess.CalledProcessError as exc:
            logger.debug("redbiom CLI error: %s", exc.stderr)
            return None
        except FileNotFoundError:
            return None

    def search_by_taxon(self, taxon_name: str, context: str | None = None) -> list[str]:
        """Return sample IDs containing the specified taxon in Qiita.

        Uses ``redbiom summarize features-contained-by-taxon`` CLI.
        Falls back to empty list if redbiom not installed.
        """
        ctx = context or self._context
        if not self._redbiom_available():
            logger.warning(
                "redbiom not installed — cannot search by taxon '%s'. "
                "Install with: pip install redbiom",
                taxon_name,
            )
            return []

        out = self._cli(
            "search", "taxa",
            f"--context={ctx}",
            "--taxonomy", taxon_name,
        )
        if not out:
            return []

        # Output is newline-separated sample IDs
        sample_ids = [line.strip() for line in out.splitlines() if line.strip()]
        logger.info(
            "Redbiom found %d samples containing taxon '%s'",
            len(sample_ids), taxon_name,
        )
        return sample_ids

    def fetch_samples(
        self,
        sample_ids: list[str],
        outdir: str,
        context: str | None = None,
    ) -> str:
        """Fetch BIOM subset for given sample IDs via redbiom. Returns file path.

        Writes sample IDs to a temp file, runs ``redbiom fetch samples``.
        Returns empty string if redbiom not installed or fetch fails.
        """
        if not sample_ids:
            logger.warning("fetch_samples called with empty sample_ids list")
            return ""

        out_dir = Path(outdir)
        out_dir.mkdir(parents=True, exist_ok=True)
        ctx = context or self._context

        out_biom = out_dir / "redbiom_subset.biom"
        if out_biom.exists():
            return str(out_biom)

        if not self._redbiom_available():
            logger.warning("redbiom not installed — cannot fetch samples")
            return ""

        # Write sample IDs to temp file
        ids_file = out_dir / "sample_ids.txt"
        ids_file.write_text("\n".join(sample_ids))

        try:
            import os as _os
            env = {**_os.environ, "REDBIOM_HOST": self._server}
            subprocess.run(
                [
                    "redbiom", "fetch", "samples",
                    f"--context={ctx}",
                    f"--sample-set={ids_file}",
                    f"--output={out_biom}",
                    "--resolve-ambiguities=most-reads",
                ],
                check=True, timeout=600, capture_output=True, text=True,
                env=env,
            )
            logger.info("Redbiom BIOM saved to %s", out_biom)
            return str(out_biom)
        except subprocess.CalledProcessError as exc:
            logger.error("redbiom fetch failed: %s", exc.stderr)
            return ""
