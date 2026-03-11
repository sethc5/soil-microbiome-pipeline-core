"""
adapters/__init__.py — Adapter registry for soil microbiome data sources.
"""

from __future__ import annotations

from .agp_adapter import AGPAdapter
from .emp_adapter import EMPAdapter
from .local_biom_adapter import LocalBIOMAdapter
from .mgnify_adapter import MGnifyAdapter
from .ncbi_sra_adapter import NCBISRAAdapter
from .neon_adapter import NEONAdapter
from .qiita_adapter import QiitaAdapter
from .redbiom_adapter import RedbiomAdapter

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
    """Factory function: instantiate adapter by source name."""
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
