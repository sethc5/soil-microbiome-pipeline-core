"""
adapters/__init__.py — Adapter registry for soil microbiome data sources.

Usage:
    from adapters import get_adapter, ADAPTER_REGISTRY
    adapter = get_adapter("sra", config={"biome": "soil"})
    for sample in adapter.search():
        print(sample["sample_id"])

Available adapters:
    sra      — NCBI SRA (16S + shotgun FASTQ via sra-tools)
    mgnify   — EBI MGnify REST API
    emp      — Earth Microbiome Project (EMP release 1 BIOM)
    agp      — American Gut Project (ENA study ERP012803)
    local    — Local BIOM / FASTQ files
    qiita    — Qiita REST API
    redbiom  — Redbiom taxon-centric Qiita search
    neon     — NEON soil 16S amplicon data portal
"""

from __future__ import annotations

from adapters.agp_adapter import AGPAdapter
from adapters.emp_adapter import EMPAdapter
from adapters.local_biom_adapter import LocalBIOMAdapter
from adapters.mgnify_adapter import MGnifyAdapter
from adapters.ncbi_sra_adapter import NCBISRAAdapter
from adapters.neon_adapter import NEONAdapter
from adapters.qiita_adapter import QiitaAdapter
from adapters.redbiom_adapter import RedbiomAdapter

ADAPTER_REGISTRY: dict[str, type] = {
    "sra": NCBISRAAdapter,
    "ncbi_sra": NCBISRAAdapter,
    "mgnify": MGnifyAdapter,
    "emp": EMPAdapter,
    "agp": AGPAdapter,
    "local": LocalBIOMAdapter,
    "qiita": QiitaAdapter,
    "redbiom": RedbiomAdapter,
    "neon": NEONAdapter,
}


def get_adapter(source: str, config: dict | None = None) -> object:
    """Factory function: instantiate adapter by source name.

    Args:
        source: One of the keys in ADAPTER_REGISTRY (case-insensitive).
        config: Config dict passed to the adapter constructor.

    Returns:
        Adapter instance.

    Raises:
        ValueError: If source is not a known adapter name.
    """
    key = source.lower().strip()
    cls = ADAPTER_REGISTRY.get(key)
    if cls is None:
        known = ", ".join(sorted(ADAPTER_REGISTRY))
        raise ValueError(
            f"Unknown adapter source {source!r}. Known sources: {known}"
        )
    return cls(config or {})


__all__ = [
    "ADAPTER_REGISTRY",
    "get_adapter",
    "AGPAdapter",
    "EMPAdapter",
    "LocalBIOMAdapter",
    "MGnifyAdapter",
    "NCBISRAAdapter",
    "NEONAdapter",
    "QiitaAdapter",
    "RedbiomAdapter",
]
