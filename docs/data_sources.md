# Data Sources & Access Patterns

**Last updated**: 2026-03-09  
Reference for what data lives where, why, and whether it should be local or remote.

---

## Permanently Resident on Server

| Data | Location | Size | Why downloaded | Would API/stream work? |
|------|----------|------|----------------|----------------------|
| SILVA 16S FASTA + BLAST DB | `/data/pipeline/ref/` | 1.5 GB | vsearch requires local indexed DB; queried thousands of times per run | No — vsearch is a local binary |
| NCBI taxonomy dump (`rankedlineage.dmp`, `nodes.dmp`, etc.) | `/data/pipeline/ref/taxdump/` | ~150 MB | Maps taxon IDs → lineage strings offline; Entrez API too slow at scale | No — 9k lookups/run would blow rate limits |
| Synthetic genome-scale metabolic models (21 XML) | `/data/pipeline/models/` | 168 MB | COBRApy FBA runs locally in tight loops; per-community, per-taxa | No — no public API for per-organism FBA |
| Proteome FAA files (21 genera, 42 files) | `/data/pipeline/proteome_cache/` | ~200 MB | Genome annotation — BV-BRC fetch-and-cache pattern | Yes, but re-fetching 42 proteomes each run is too slow |
| PICRUSt2 reference DB | `miniforge3/envs/bioinfo/` | 12 GB | PICRUSt2 CLI requires local reference tree + marker genes | No — PICRUSt2 has no remote API |
| MGnify FTP taxonomy summaries | Parsed → SQLite only | ~0 MB (27 KB each, not kept) | Ingested and discarded — only the parsed phylum profile hits DB | Yes — already optimal |

---

## Transient (Download → Process → Delete)

| Data | Source | Per-sample size | Cleanup | Why not keep |
|------|--------|----------------|---------|--------------|
| NEON shotgun FASTQs | `storage.neonscience.org` | 10–15 GB | `shutil.rmtree(workdir)` after vsearch | Only the taxonomy vector matters; raw reads add no DB value |
| NCBI SRA FASTQs | `ftp.ncbi.nlm.nih.gov` | 1–15 GB | Same pattern | Same |
| vsearch/fastp intermediate files | workdir under staging | <1 GB | Deleted per-sample | Derived artifacts |

> **Known bug**: the 16S retry job's BONA/DEJU Alaska timeout failures left 38 FASTQs
> undeleted (394 GB as of 2026-03-09) because `shutil.rmtree` only runs on the success
> path. A cron-based cleanup of staging dirs untouched for >4h would prevent this.
> See `scripts/process_neon_16s.py` lines 325, 335.

---

## Called On-Demand (No Local Copy)

| Source | Adapter | What's fetched | Access pattern | Rate limit |
|--------|---------|---------------|----------------|------------|
| NEON metadata API `data.neonscience.org/api/v0` | `adapters/neon_adapter.py` | Sample metadata, FASTQ URLs, site info | Once per ingest run | None documented |
| NCBI Entrez `eutils.ncbi.nlm.nih.gov` | `adapters/ncbi_sra_adapter.py`, `compute/genome_fetcher.py` | SRA search results, genome accessions | Per-ingest + per-genus genome lookup | 3/sec unauthenticated, 10/sec with API key (`NCBI_API_KEY` env) |
| BV-BRC `bv-brc.org` | `compute/genome_fetcher.py` | Reference genome FASTA (then cached locally) | Once per new genus, then served from `proteome_cache/` | None documented |
| ENA portal `ebi.ac.uk/ena/portal/api` | `scripts/ingest_mgnify_ftp.py` (optional) | lat/lon/country per ERR run | Per-run; skipped with `--no-ena-meta` | Not WAF-blocked from Hetzner |
| EBI FTP `ftp.ebi.ac.uk` | `scripts/ingest_mgnify_ftp.py` | SSU taxonomy TXT (27 KB/run) | Per-run, parse-and-discard | None — accessible from Hetzner ✓ |

---

## Adapters Written but Not Yet Run

| Adapter | Source | Data type | What it would pull | Blocker |
|---------|--------|-----------|-------------------|---------|
| `adapters/agp_adapter.py` | ENA study ERP012803 (American Gut Project) | 16S amplicon, soil subset | Sample metadata via ENA TSV, FASTQ URLs | Not prioritized |
| `adapters/emp_adapter.py` | EMP FTP / Qiita project 164 | 16S OTU BIOM table | Pre-computed OTU table — one ~500 MB download | Needs Qiita access token |
| `adapters/qiita_adapter.py` | `qiita.ucsd.edu` REST API | 16S/shotgun study metadata | Study list → sample-level metadata | Requires Qiita account token |
| `adapters/redbiom_adapter.py` | `redbiom.qiita.ucsd.edu` | Qiita feature-centric search | All samples containing target taxa across all Qiita studies | Requires redbiom CLI or direct API |
| `adapters/local_biom_adapter.py` | Local disk | BIOM / FASTQ / taxonomy TSV | Private datasets | No private data yet |

---

## Blocked Sources

| Source | Issue | Workaround |
|--------|-------|-----------|
| MGnify REST API `www.ebi.ac.uk/metagenomics/api/v1` | EBI WAF drops connections from Hetzner ASN 24940 | Use `ftp.ebi.ac.uk` — not WAF-blocked; `scripts/ingest_mgnify_ftp.py` |

---

## Not-Yet-Downloaded Reference Data (future)

| Data | Source | Size | When needed |
|------|--------|------|-------------|
| AGORA2 genome-scale models | Zenodo AGORA2 release | ~1 GB | Real T1 FBA for classified genera (replaces synthetic models) |
| GTDB-Tk reference data | `data.ace.uq.edu.au/` | 85 GB | Only if doing MAG annotation from shotgun assemblies |
| CheckM database | `data.ace.uq.edu.au/` | 1.5 GB | Same — only with MAG assembly |
| EMP BIOM table | EMP FTP | ~500 MB | Community-level fingerprints for ~28k environmental samples |

---

## Verdict: Local vs. Remote

| Asset | Decision | Rationale |
|-------|----------|-----------|
| SILVA + BLAST DB | ✅ Keep local | No alternative; vsearch requires local index |
| NCBI taxdump | ✅ Keep local | Volume of lookups exceeds safe API rate |
| 21 synthetic XML models | ✅ Keep local | FBA is local CPU compute; no remote FBA API |
| Proteome FAA cache | ✅ Keep local | Cache-on-first-use is the right pattern |
| PICRUSt2 ref (12 GB) | ✅ Keep local | CLI binary, no remote option |
| NEON/SRA FASTQs | ⚠️ Transient only | Fix failure-path cleanup; never archive raw reads |
| MGnify FTP files | ✅ Parse-and-discard | Already optimal |
| EMP BIOM table | 🟡 One-time download | Small enough to keep; pull when running EMP adapter |
| AGORA2 models | 🟡 Download when needed | ~1 GB; pull before first real T1 FBA run |
| GTDB-Tk ref | 🟡 Defer | 85 GB; only download if MAG assembly becomes part of scope |
