"""
Fetch gap-fill references from Semantic Scholar for topics identified
in STRATEGIC_ASSESSMENT.md and REBUILD_PLAN.md that have no coverage
in the existing references/ library.

Rate-limited to 1 request/second per API key terms.
"""

import time
import os
import sys
import requests

API_KEY = os.environ.get("S2_API_KEY", "")
if not API_KEY:
    sys.exit("Error: S2_API_KEY environment variable not set. "
             "Export your Semantic Scholar API key before running.")
BASE_URL = "https://api.semanticscholar.org/graph/v1/paper/search"
FIELDS = "title,abstract,authors,year,externalIds,citationCount,openAccessPdf,venue"
OUT_DIR = os.path.join(os.path.dirname(__file__), "..", "references")

# ── Gap-fill queries mapped to strategic assessment / rebuild plan gaps ──
QUERIES = [
    # Gap 1: Fungi are second-class citizens
    ("fungal_ecology_its",
     "Soil Fungal Ecology, ITS Methods & Mycorrhizal Networks",
     "soil fungal ecology ITS amplicon mycorrhizal network arbuscular", 10,
     "Gap 1 — fungi/ITS track, Phase 0.1 schema, Phase 1.5 tax_profiler, Phase 7.1 C-seq config"),

    ("amf_phosphorus_aggregates",
     "Arbuscular Mycorrhizal Fungi: Phosphorus Acquisition & Soil Aggregation",
     "arbuscular mycorrhizal fungi AMF phosphorus uptake soil aggregate formation", 8,
     "Gap 1 — AMF is primary P-acquisition mechanism, critical for C-sequestration application"),

    # Gap 2: Archaea absent
    ("soil_archaea_aoa",
     "Soil Archaea: Ammonia-Oxidizing Archaea (AOA) & Methanogens",
     "ammonia oxidizing archaea Thaumarchaeota soil nitrification AOA amoA", 10,
     "Gap 2 — archaeal amoA split, Phase 1.4 gene scanner, methanogen relevance for wetland app"),

    # Gap 3: Metatranscriptomics path
    ("metatranscriptomics_soil",
     "Soil Metatranscriptomics: Gene Expression vs. Gene Presence",
     "metatranscriptomics soil gene expression RNA metagenome active microbiome", 8,
     "Gap 3 — mrna_to_dna_ratio column, expression-based functional validation"),

    # Gap 4: Rhizosphere vs. bulk soil
    ("rhizosphere_ecology",
     "Rhizosphere Microbiome Assembly & Root Exudate Effects",
     "rhizosphere microbiome assembly root exudate plant selection soil", 10,
     "Gap 4 — sampling_fraction field, rhizosphere/bulk distinction for PGP application"),

    # Gap 5: MAG quality / CheckM
    ("mag_quality_checkm",
     "Metagenome-Assembled Genomes (MAGs): Quality Standards & CheckM",
     "metagenome assembled genome MAG quality CheckM completeness contamination binning", 8,
     "Gap 5 — CheckM integration Phase 3.2, genome_quality.py, T1 confidence"),

    # Compositional data analysis (T0.25 ML pitfall)
    ("compositional_data_microbiome",
     "Compositional Data Analysis for Microbiome (CLR, ALR, ILR Transforms)",
     "compositional data analysis microbiome centered log ratio transform OTU", 8,
     "T0.25 pitfall — CLR transform required before ML on OTU tables, Phase 2.4"),

    # CarveMe / automated model reconstruction
    ("genome_scale_model_reconstruction",
     "Automated Genome-Scale Metabolic Model Reconstruction (CarveMe, ModelSEED)",
     "CarveMe genome scale metabolic model reconstruction automated gap filling", 8,
     "Phase 3.4 model_builder.py — CarveMe and ModelSEED for automated GEM construction"),

    # SRA metadata / MIxS standards
    ("metadata_standards_mixs",
     "Microbiome Metadata Standards: MIxS, ENVO & Sample Annotation",
     "minimum information sequence MIxS metadata standard microbiome sample annotation ENVO", 8,
     "Easy Win #1 — metadata normalization, Phase 0.4, institution need #5"),

    # Soil pH as community driver
    ("soil_ph_community_driver",
     "Soil pH as Primary Driver of Microbial Community Structure",
     "soil pH microbial community structure bacterial diversity driver determinant", 8,
     "Core design decision — pH as load-bearing metadata, T0 filter, FBA constraints"),

    # Bioremediation / hydrocarbon degradation
    ("bioremediation_hydrocarbon",
     "Soil Bioremediation: Microbial Hydrocarbon Degradation & Bioaugmentation",
     "bioremediation hydrocarbon degradation soil microbiome alkane PAH bioaugmentation", 10,
     "Phase 7.4 — third pipeline instantiation, bioremediation config.yaml"),

    # Competitive exclusion / niche theory for establishment
    ("niche_competition_inoculant",
     "Microbial Niche Competition & Inoculant Establishment in Soil",
     "microbial competition niche exclusion inoculant establishment colonization soil", 8,
     "Phase 4.3 establishment_predictor.py — competitive exclusion theory basis"),

    # UNITE database / fungal taxonomy
    ("unite_fungal_taxonomy",
     "UNITE Database & Fungal Taxonomic Classification from ITS",
     "UNITE database ITS fungal taxonomy classification barcode molecular identification", 6,
     "Gap 1 — UNITE is the ITS taxonomy reference database, Phase 1.5 tax_profiler"),
]


def s2_search(query: str, limit: int) -> list[dict]:
    headers = {"x-api-key": API_KEY}
    params = {"query": query, "limit": limit, "fields": FIELDS}
    resp = requests.get(BASE_URL, headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json().get("data", [])


def safe_abstract(paper: dict) -> str:
    ab = paper.get("abstract") or ""
    if not ab:
        return "_No abstract available._"
    return ab[:600].rstrip() + ("…" if len(ab) > 600 else "")


def doi_link(paper: dict) -> str:
    doi = (paper.get("externalIds") or {}).get("DOI", "")
    if doi:
        return f"https://doi.org/{doi}"
    pid = paper.get("paperId", "")
    return f"https://www.semanticscholar.org/paper/{pid}" if pid else ""


def pdf_link(paper: dict) -> str:
    oa = paper.get("openAccessPdf") or {}
    return oa.get("url", "")


def author_str(paper: dict) -> str:
    authors = paper.get("authors") or []
    names = [a.get("name", "") for a in authors[:4]]
    if len(paper.get("authors") or []) > 4:
        names.append("et al.")
    return ", ".join(names)


def write_topic_file(stem: str, heading: str, papers: list[dict],
                     rationale: str) -> None:
    path = os.path.join(OUT_DIR, f"{stem}.md")
    lines = [
        f"# {heading}",
        "",
        f"_Auto-fetched from Semantic Scholar — {time.strftime('%Y-%m-%d')}_",
        "",
        f"**Why this topic:** {rationale}",
        "",
        "---",
        "",
    ]
    for i, p in enumerate(papers, 1):
        title = p.get("title", "Untitled")
        year = p.get("year", "n.d.")
        venue = p.get("venue", "") or ""
        authors = author_str(p)
        abstract = safe_abstract(p)
        link = doi_link(p)
        pdf = pdf_link(p)
        citations = p.get("citationCount", 0) or 0

        lines.append(f"## {i}. {title}")
        lines.append("")
        if authors:
            lines.append(f"**Authors:** {authors}")
        meta_parts = []
        if year:
            meta_parts.append(str(year))
        if venue:
            meta_parts.append(venue)
        if meta_parts:
            lines.append(f"**Published:** {' · '.join(meta_parts)}")
        lines.append(f"**Citations:** {citations:,}")
        if link:
            lines.append(f"**Link:** {link}")
        if pdf:
            lines.append(f"**PDF:** {pdf}")
        lines.append("")
        lines.append(f"**Abstract:** {abstract}")
        lines.append("")
        lines.append("---")
        lines.append("")

    with open(path, "w") as f:
        f.write("\n".join(lines))
    print(f"  -> wrote {path} ({len(papers)} papers)")


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    all_meta = []

    for stem, heading, query, n, rationale in QUERIES:
        print(f"Querying: {heading[:60]}…")
        try:
            papers = s2_search(query, n)
        except Exception as e:
            print(f"  ERROR: {e}")
            papers = []
        write_topic_file(stem, heading, papers, rationale)
        all_meta.append((stem, heading, len(papers), rationale))
        time.sleep(1.1)

    # Append to INDEX.md
    index_path = os.path.join(OUT_DIR, "INDEX.md")
    with open(index_path, "r") as f:
        existing = f.read()

    new_section = [
        "",
        "---",
        "",
        "## Gap-Fill References (Strategic Assessment)",
        "",
        f"_Added {time.strftime('%Y-%m-%d')} — topics identified as gaps in STRATEGIC_ASSESSMENT.md and REBUILD_PLAN.md_",
        "",
        "| File | Topic | Papers | Addresses |",
        "|------|-------|--------|-----------|",
    ]
    for stem, heading, count, rationale in all_meta:
        new_section.append(
            f"| [{stem}.md]({stem}.md) | {heading} | {count} | {rationale.split(' — ')[0]} |"
        )

    new_section += [
        "",
        "---",
        "",
        "## Gap-Fill → Module Mapping",
        "",
        "| Module / Phase | Gap-fill reference(s) |",
        "|---------------|----------------------|",
        "| Phase 0.1: Schema (fungi, archaea) | `fungal_ecology_its.md`, `amf_phosphorus_aggregates.md`, `soil_archaea_aoa.md` |",
        "| Phase 0.4: MetadataNormalizer | `metadata_standards_mixs.md` |",
        "| Phase 1.4: functional_gene_scanner.py | `soil_archaea_aoa.md` (archaeal amoA split) |",
        "| Phase 1.5: tax_profiler.py (ITS) | `fungal_ecology_its.md`, `unite_fungal_taxonomy.md` |",
        "| Phase 2.4: functional_predictor.py | `compositional_data_microbiome.md` (CLR transform) |",
        "| Phase 3.1: genome_fetcher.py | `mag_quality_checkm.md` |",
        "| Phase 3.2: genome_quality.py | `mag_quality_checkm.md` |",
        "| Phase 3.4: model_builder.py | `genome_scale_model_reconstruction.md` |",
        "| Phase 4.3: establishment_predictor.py | `niche_competition_inoculant.md` |",
        "| Phase 7.1: C-seq config | `fungal_ecology_its.md`, `amf_phosphorus_aggregates.md` |",
        "| Phase 7.4: bioremediation config | `bioremediation_hydrocarbon.md` |",
        "| T0 filter design | `soil_ph_community_driver.md` |",
        "| Gap 3: metatranscriptomics | `metatranscriptomics_soil.md` |",
        "| Gap 4: rhizosphere | `rhizosphere_ecology.md` |",
        "",
    ]

    with open(index_path, "w") as f:
        f.write(existing + "\n".join(new_section))
    print(f"\nDone. Index updated at {index_path}")


if __name__ == "__main__":
    main()
