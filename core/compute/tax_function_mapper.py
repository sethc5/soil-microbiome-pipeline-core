"""
compute/tax_function_mapper.py -- T0 taxonomy-to-function mapping.

Maps genus/phylum-level taxonomy to functional ecological groups using
FaProTax (preferred) or a bundled minimal lookup table (fallback).

Functional groups covered (FaProTax primary + soil-specific extensions):
  nitrogen_fixation             nifH carriers, Rhizobium, Frankia, ...
  nitrification                 Nitrosomonas, Thaumarchaeota, ...
  denitrification               Pseudomonas, Paracoccus, Bradyrhizobium, ...
  anammox                       Candidatus Brocadia, ...
  methanogenesis                Methanobacterium, Methanosarcina, ...
  methanotrophy                 Methylobacter, Methylococcus, ...
  sulfate_reduction             Desulfovibrio, Desulfobacter, ...
  sulfur_oxidation              Thiobacillus, Sulfolobus, ...
  iron_reduction                Geobacter, Shewanella, ...
  manganese_reduction           Geobacter (dual), ...
  cellulose_degradation         Cytophaga, Cellulomonas, ...
  chitin_degradation            Micromonospora, Streptomyces, ...
  lignin_degradation            Phanerochaete, Trametes, ...
  phosphate_solubilization      Bacillus, Penicillium, Aspergillus, ...
  mycorrhizal                   Glomus, Rhizophagus, ...
  plant_pathogen                Fusarium, Phytophthora, ...
  ectomycorrhizal               Amanita, Pisolithus, Suillus, ...
  heavy_metal_resistance        Cupriavidus, ...
  aromatic_degradation          Rhodococcus, Burkholderia, ...
  hydrogen_oxidation            Knallgasb., Hydrogenophaga, ...

Usage:
  from core.compute.tax_function_mapper import map_taxonomy_to_function
  result = map_taxonomy_to_function({"Nitrosomonas": 0.05, "Glomus": 0.12})
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Minimal bundled lookup  (genus -> list of functional groups)
# Used when faprotax package is not installed.
# ---------------------------------------------------------------------------

_GENUS_FUNCTION_LOOKUP: dict[str, list[str]] = {
    # Nitrogen fixation
    "Rhizobium":           ["nitrogen_fixation"],
    "Bradyrhizobium":      ["nitrogen_fixation", "denitrification"],
    "Sinorhizobium":       ["nitrogen_fixation"],
    "Mesorhizobium":       ["nitrogen_fixation"],
    "Frankia":             ["nitrogen_fixation"],
    "Azospirillum":        ["nitrogen_fixation"],
    "Azotobacter":         ["nitrogen_fixation"],
    "Cyanobacterium":      ["nitrogen_fixation"],
    "Anabaena":            ["nitrogen_fixation"],
    "Nostoc":              ["nitrogen_fixation"],
    "Herbaspirillum":      ["nitrogen_fixation"],
    "Gluconacetobacter":   ["nitrogen_fixation"],
    "Burkholderia":        ["nitrogen_fixation", "aromatic_degradation"],
    "Azoarcus":            ["nitrogen_fixation", "aromatic_degradation"],
    # Nitrification
    "Nitrosomonas":        ["nitrification"],
    "Nitrosospira":        ["nitrification"],
    "Nitrospira":          ["nitrification"],
    "Nitrospina":          ["nitrification"],
    "Nitrobacter":         ["nitrification"],
    "Nitrococcus":         ["nitrification"],
    "Nitrosovibrio":       ["nitrification"],
    "Nitrosococcus":       ["nitrification"],
    "Nitrosopumilus":      ["nitrification"],
    "Nitrososphaera":      ["nitrification"],
    # Denitrification
    "Pseudomonas":         ["phosphate_solubilization", "denitrification", "aromatic_degradation"],
    "Paracoccus":          ["denitrification"],
    "Thauera":             ["denitrification", "aromatic_degradation"],
    "Dechloromonas":       ["denitrification"],
    "Rhodanobacter":       ["denitrification"],
    # Anammox
    "Brocadia":            ["anammox"],
    "Jettenia":            ["anammox"],
    "Scalindua":           ["anammox"],
    "Kuenenia":            ["anammox"],
    # Methanogenesis
    "Methanobacterium":    ["methanogenesis"],
    "Methanobrevibacter":  ["methanogenesis"],
    "Methanosarcina":      ["methanogenesis"],
    "Methanosaeta":        ["methanogenesis"],
    "Methanospirillum":    ["methanogenesis"],
    "Methanococcus":       ["methanogenesis"],
    "Methanothermobacter": ["methanogenesis"],
    # Methanotrophy
    "Methylobacter":       ["methanotrophy"],
    "Methylococcus":       ["methanotrophy"],
    "Methylosinus":        ["methanotrophy"],
    "Methylocapsa":        ["methanotrophy"],
    "Methylocystis":       ["methanotrophy"],
    "Methylovulum":        ["methanotrophy"],
    # Sulfate reduction
    "Desulfovibrio":       ["sulfate_reduction"],
    "Desulfobacter":       ["sulfate_reduction"],
    "Desulfobacterium":    ["sulfate_reduction"],
    "Desulfonema":         ["sulfate_reduction"],
    "Desulfosporosinus":   ["sulfate_reduction"],
    "Desulfuromusa":       ["sulfate_reduction"],
    # Sulfur oxidation
    "Thiobacillus":        ["sulfur_oxidation", "denitrification"],
    "Sulfolobus":          ["sulfur_oxidation"],
    "Acidithiobacillus":   ["sulfur_oxidation"],
    "Sulfurimonas":        ["sulfur_oxidation"],
    # Iron / manganese reduction
    "Geobacter":           ["iron_reduction", "manganese_reduction", "aromatic_degradation"],
    "Shewanella":          ["iron_reduction"],
    "Desulfuromonas":      ["iron_reduction"],
    "Anaeromyxobacter":    ["iron_reduction"],
    # Cellulose / chitin degradation
    "Cytophaga":           ["cellulose_degradation"],
    "Cellulomonas":        ["cellulose_degradation"],
    "Clostridium":         ["cellulose_degradation", "fermentation"],
    "Trichoderma":         ["cellulose_degradation"],
    "Streptomyces":        ["chitin_degradation", "cellulose_degradation"],
    "Micromonospora":      ["chitin_degradation"],
    "Chitinophaga":        ["chitin_degradation"],
    # Lignin / aromatic degradation (C sequestration)
    "Phanerochaete":       ["lignin_degradation"],
    "Trametes":            ["lignin_degradation"],
    "Pleurotus":           ["lignin_degradation"],
    "Ganoderma":           ["lignin_degradation"],
    "Rhodococcus":         ["aromatic_degradation", "heavy_metal_resistance"],
    # Phosphate solubilization
    "Bacillus":            ["phosphate_solubilization"],
    "Penicillium":         ["phosphate_solubilization"],
    "Aspergillus":         ["phosphate_solubilization"],
    "Enterobacter":        ["phosphate_solubilization"],
    # Mycorrhizal
    "Glomus":              ["mycorrhizal"],
    "Rhizophagus":         ["mycorrhizal"],
    "Diversispora":        ["mycorrhizal"],
    "Funneliformis":       ["mycorrhizal"],
    "Claroideoglomus":     ["mycorrhizal"],
    # Ectomycorrhizal
    "Amanita":             ["ectomycorrhizal"],
    "Pisolithus":          ["ectomycorrhizal"],
    "Suillus":             ["ectomycorrhizal"],
    "Laccaria":            ["ectomycorrhizal"],
    "Cenococcum":          ["ectomycorrhizal"],
    "Hebeloma":            ["ectomycorrhizal"],
    # Plant pathogens
    "Fusarium":            ["plant_pathogen", "chitin_degradation"],
    "Phytophthora":        ["plant_pathogen"],
    "Rhizoctonia":         ["plant_pathogen"],
    "Pythium":             ["plant_pathogen"],
    "Verticillium":        ["plant_pathogen"],
    "Sclerotinia":         ["plant_pathogen"],
    # Heavy metal resistance
    "Cupriavidus":         ["heavy_metal_resistance", "aromatic_degradation"],
    "Ralstonia":           ["heavy_metal_resistance"],
    # Hydrogen oxidation
    "Hydrogenophaga":      ["hydrogen_oxidation"],
    "Knallgasbacterium":   ["hydrogen_oxidation"],
}


# ---------------------------------------------------------------------------
# Phylum-level fallback (coarser mapping)
# ---------------------------------------------------------------------------

_PHYLUM_FUNCTION_LOOKUP: dict[str, list[str]] = {
    "Proteobacteria":    ["nitrogen_fixation", "nitrification", "denitrification",
                          "sulfur_oxidation"],
    "Firmicutes":        ["cellulose_degradation", "fermentation"],
    "Actinobacteria":    ["chitin_degradation", "cellulose_degradation"],
    "Bacteroidetes":     ["cellulose_degradation"],
    "Chloroflexi":       ["cellulose_degradation"],
    "Acidobacteria":     ["cellulose_degradation"],
    "Euryarchaeota":     ["methanogenesis"],
    "Thaumarchaeota":    ["nitrification"],
    "Crenarchaeota":     ["nitrification"],
    "Ascomycota":        ["cellulose_degradation", "plant_pathogen", "phosphate_solubilization"],
    "Basidiomycota":     ["lignin_degradation", "ectomycorrhizal", "plant_pathogen"],
    "Glomeromycota":     ["mycorrhizal"],
    "Mucoromycota":      ["mycorrhizal"],
    "Chytridiomycota":   ["chitin_degradation"],
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def map_taxonomy_to_function(
    taxonomy: dict[str, float] | Any,
    normalize: bool = True,
    use_faprotax: bool = True,
    abundance_threshold: float = 0.001,
) -> dict[str, Any]:
    """
    Map genus/phylum-level taxonomy to functional ecological groups.

    Parameters
    ----------
    taxonomy          : dict of {taxon_name: relative_abundance} or a
                        DataFrame with columns ['taxon', 'rel_abundance'].
    normalize         : if True, normalise abundance scores to sum to 1.
    use_faprotax      : attempt to import faprotax package first; fall back
                        to bundled lookup if not available.
    abundance_threshold: ignore taxa below this relative abundance.

    Returns
    -------
    dict: {
        function_name: {
            present           bool
            score             float  (summed relative abundances of supporting taxa)
            supporting_taxa   list[str]
        }
    }
    """
    taxonomy_dict = _coerce_taxonomy(taxonomy)
    if not taxonomy_dict:
        return _empty_function_profile()

    # Filter below threshold
    taxonomy_dict = {
        k: v for k, v in taxonomy_dict.items()
        if v >= abundance_threshold
    }

    # Try FaProTax
    if use_faprotax:
        try:
            return _faprotax_mapping(taxonomy_dict, normalize)
        except ImportError:
            logger.debug("faprotax package not installed; using bundled lookup.")
        except Exception as exc:
            logger.warning("faprotax mapping failed (%s); using bundled lookup.", exc)

    # Bundled genus-level lookup
    return _bundled_mapping(taxonomy_dict, normalize)


def get_functional_summary(function_profile: dict) -> dict[str, Any]:
    """
    Produce a condensed summary of the function profile for DB storage / reporting.

    Returns top functional groups sorted by score, plus key indicator flags.
    """
    if not function_profile:
        return {"top_functions": [], "n_functions_detected": 0}

    present = {k: v for k, v in function_profile.items() if v.get("present", False)}
    sorted_fns = sorted(
        [{"function": k, "score": v["score"], "n_taxa": len(v["supporting_taxa"])}
         for k, v in present.items()],
        key=lambda x: x["score"],
        reverse=True,
    )
    return {
        "top_functions":          sorted_fns[:20],
        "n_functions_detected":   len(present),
        "has_n_cycling":          any(
            function_profile.get(g, {}).get("present", False)
            for g in ["nitrogen_fixation", "nitrification", "denitrification", "anammox"]
        ),
        "has_c_cycling":          any(
            function_profile.get(g, {}).get("present", False)
            for g in ["methanogenesis", "methanotrophy", "cellulose_degradation", "lignin_degradation"]
        ),
        "has_mycorrhizal":        function_profile.get("mycorrhizal", {}).get("present", False),
        "has_pathogens":          function_profile.get("plant_pathogen", {}).get("present", False),
    }


# ---------------------------------------------------------------------------
# Backend implementations
# ---------------------------------------------------------------------------

def _faprotax_mapping(taxonomy_dict: dict[str, float], normalize: bool) -> dict:
    """Map using the faprotax Python package (FAPROTAX >= 1.2.7)."""
    import faprotax  # type: ignore[import]

    # FaProTax expects OTU table style input; build minimal table
    # faprotax.collapse_table(table, groups, sample_names, ...)
    # Use the functional_groups() helper if available, else raw lookup dict
    if hasattr(faprotax, "functional_groups"):
        fg = faprotax.functional_groups()
    else:
        # Older API: use the main FAPROTAX database dict directly
        db = faprotax.load_db()
        fg = db

    result = _empty_function_profile()
    for taxon, abund in taxonomy_dict.items():
        # Lookup by genus name (last word in multi-part names)
        genus = taxon.strip().split()[-1] if " " in taxon else taxon
        for fn_group, genera_list in fg.items():
            fn_key = fn_group.lower().replace(" ", "_")
            if genus in genera_list or taxon in genera_list:
                if fn_key not in result:
                    result[fn_key] = {"present": False, "score": 0.0, "supporting_taxa": []}
                result[fn_key]["present"]  = True
                result[fn_key]["score"]   += abund
                result[fn_key]["supporting_taxa"].append(taxon)

    if normalize:
        _normalise_scores(result)
    return result


def _bundled_mapping(taxonomy_dict: dict[str, float], normalize: bool) -> dict:
    """Map using the bundled genus-level lookup table."""
    result = _empty_function_profile()

    for taxon, abund in taxonomy_dict.items():
        genus = taxon.strip().split()[-1] if " " in taxon else taxon

        # Genus-level match
        fns = _GENUS_FUNCTION_LOOKUP.get(genus) or _GENUS_FUNCTION_LOOKUP.get(genus.capitalize())
        if not fns:
            # Phylum-level fallback
            fns = _PHYLUM_FUNCTION_LOOKUP.get(taxon) or _PHYLUM_FUNCTION_LOOKUP.get(genus)

        if fns:
            for fn in fns:
                if fn not in result:
                    result[fn] = {"present": False, "score": 0.0, "supporting_taxa": []}
                result[fn]["present"]  = True
                result[fn]["score"]   += abund
                result[fn]["supporting_taxa"].append(taxon)

    if normalize:
        _normalise_scores(result)
    return result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _empty_function_profile() -> dict:
    all_groups = [
        "nitrogen_fixation", "nitrification", "denitrification", "anammox",
        "methanogenesis", "methanotrophy", "sulfate_reduction", "sulfur_oxidation",
        "iron_reduction", "manganese_reduction", "cellulose_degradation",
        "chitin_degradation", "lignin_degradation", "phosphate_solubilization",
        "mycorrhizal", "plant_pathogen", "ectomycorrhizal",
        "heavy_metal_resistance", "aromatic_degradation", "hydrogen_oxidation",
    ]
    return {
        g: {"present": False, "score": 0.0, "supporting_taxa": []}
        for g in all_groups
    }


def _normalise_scores(profile: dict) -> None:
    """Normalise score values to sum to 1 across all groups (in-place)."""
    total = sum(v["score"] for v in profile.values())
    if total > 0:
        for v in profile.values():
            v["score"] /= total


def _coerce_taxonomy(taxonomy: Any) -> dict[str, float]:
    """Accept dict or DataFrame-like input, return a plain dict."""
    if isinstance(taxonomy, dict):
        return {str(k): float(v) for k, v in taxonomy.items()}
    # DataFrame support
    try:
        import pandas as pd  # type: ignore[import]
        if isinstance(taxonomy, pd.DataFrame):
            if "taxon" in taxonomy.columns and "rel_abundance" in taxonomy.columns:
                return dict(zip(taxonomy["taxon"], taxonomy["rel_abundance"]))
            elif taxonomy.shape[1] == 2:
                return dict(zip(taxonomy.iloc[:, 0], taxonomy.iloc[:, 1].astype(float)))
    except ImportError:
        pass
    logger.warning("Could not parse taxonomy input type %s; returning empty.", type(taxonomy))
    return {}
