"""
Fetch relevant academic references from Semantic Scholar and save to knowledge/.
Rate-limited to 1 request/second per API key terms.
"""

import time
import json
import os
import re
import sys
import requests

API_KEY = os.environ.get("S2_API_KEY", "")
if not API_KEY:
    sys.exit("Error: S2_API_KEY environment variable not set. "
             "Export your Semantic Scholar API key before running.")
BASE_URL = "https://api.semanticscholar.org/graph/v1/paper/search"
FIELDS = "title,abstract,authors,year,externalIds,citationCount,openAccessPdf,venue"
OUT_DIR = os.path.join(os.path.dirname(__file__), "..", "knowledge")

QUERIES = [
    # (output_filename_stem, topic_heading, query_string, n_results)
    ("nitrogen_fixation",         "Biological Nitrogen Fixation in Soil",
     "biological nitrogen fixation soil microbiome nifH", 12),

    ("carbon_sequestration",      "Soil Carbon Sequestration & Microbial Drivers",
     "soil carbon sequestration microbial community organic matter", 12),

    ("metagenomics_methods",      "Soil Metagenomics Methods (16S, Shotgun, QIIME2, HUMAnN3)",
     "soil metagenomics 16S amplicon shotgun sequencing diversity", 12),

    ("metabolic_modeling_fba",    "Community Flux Balance Analysis & Metabolic Modeling",
     "community flux balance analysis FBA microbial metabolic model", 10),

    ("functional_prediction",     "Functional Prediction from Metagenomes (PICRUSt2, HUMAnN3)",
     "PICRUSt2 HUMAnN3 functional prediction metagenome", 8),

    ("keystone_taxa",             "Keystone Taxa & Microbial Interaction Networks",
     "keystone taxa soil microbial network interaction ecology", 10),

    ("community_dynamics_sim",    "Microbial Community Dynamics & Stability",
     "microbial community dynamics stability perturbation soil", 10),

    ("plant_growth_promoting",    "Plant Growth Promoting Rhizobacteria",
     "plant growth promoting rhizobacteria PGPR soil inoculant", 10),

    ("soil_health_indicators",    "Soil Health Biological Indicators",
     "soil health biological indicators enzyme activity diversity", 10),

    ("pathogen_suppression",      "Soil Suppressiveness & Pathogen Suppression",
     "soil suppressive pathogen suppression microbiome Fusarium Rhizoctonia", 8),

    ("bioinoculants_amendments",  "Bioinoculants, Biochar & Organic Amendments",
     "bioinoculant biochar compost amendment soil microbiome establishment", 8),

    ("land_management",           "Land Management & Soil Microbiome",
     "tillage cover crop land management soil microbiome agriculture", 10),

    ("dfba_agent_simulation",     "Dynamic FBA & Agent-Based Microbial Simulation",
     "dynamic FBA dFBA agent based simulation microbial community", 8),

    ("diversity_metrics",         "Microbial Diversity Metrics & Ecology",
     "Shannon diversity Chao1 richness beta diversity soil microbiome ecology", 8),

    ("earth_microbiome",          "Earth Microbiome Project & Global Surveys",
     "Earth Microbiome Project EMP global soil microbiome survey", 8),
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
    # Trim to 600 chars to keep files readable
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


def write_topic_file(stem: str, heading: str, papers: list[dict]) -> None:
    path = os.path.join(OUT_DIR, f"{stem}.md")
    lines = [
        f"# {heading}",
        "",
        f"_Auto-fetched from Semantic Scholar — {time.strftime('%Y-%m-%d')}_",
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

    for stem, heading, query, n in QUERIES:
        print(f"Querying: {heading[:55]}…")
        try:
            papers = s2_search(query, n)
        except Exception as e:
            print(f"  ERROR: {e}")
            papers = []
        write_topic_file(stem, heading, papers)
        all_meta.append((stem, heading, len(papers)))
        time.sleep(1.1)   # Respect 1 req/sec rate limit

    # Write index
    index_lines = [
        "# References Index",
        "",
        f"_Built {time.strftime('%Y-%m-%d')} via Semantic Scholar API_",
        "",
        "| File | Topic | Papers |",
        "|------|-------|--------|",
    ]
    for stem, heading, count in all_meta:
        index_lines.append(f"| [{stem}.md]({stem}.md) | {heading} | {count} |")

    index_lines += [
        "",
        "---",
        "",
        "## Usage in Pipeline",
        "",
        "These files provide curated literature backing for each major module:",
        "",
        "| Module | Reference file(s) |",
        "|--------|-------------------|",
        "| `compute/tax_profiler.py` | `metagenomics_methods.md` |",
        "| `compute/community_fba.py` | `metabolic_modeling_fba.md`, `dfba_agent_simulation.md` |",
        "| `compute/functional_predictor.py` | `functional_prediction.md` |",
        "| `compute/keystone_analyzer.py` | `keystone_taxa.md` |",
        "| `compute/stability_analyzer.py` | `community_dynamics_sim.md` |",
        "| `compute/establishment_predictor.py` | `bioinoculants_amendments.md` |",
        "| `compute/diversity_metrics.py` | `diversity_metrics.md` |",
        "| `compute/intervention_screener.py` | `land_management.md`, `bioinoculants_amendments.md` |",
        "| `compute/amendment_effect_model.py` | `bioinoculants_amendments.md` |",
        "| `taxa_enrichment.py` | `earth_microbiome.md`, `nitrogen_fixation.md` |",
        "| `validate_pipeline.py` | `soil_health_indicators.md` |",
        "| Application: N-fixation | `nitrogen_fixation.md` |",
        "| Application: Carbon seq | `carbon_sequestration.md` |",
        "| Application: Pathogen suppression | `pathogen_suppression.md` |",
        "| Application: PGPR / inoculants | `plant_growth_promoting.md` |",
        "",
    ]

    with open(os.path.join(OUT_DIR, "INDEX.md"), "w") as f:
        f.write("\n".join(index_lines))
    print(f"\nDone. Index written to {OUT_DIR}/INDEX.md")


if __name__ == "__main__":
    main()
