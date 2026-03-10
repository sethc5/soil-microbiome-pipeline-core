# BUILD.md — Full Production Build Plan
## soil-microbiome-pipeline-core → nitrogen-fixation-pipeline (first instantiation)

**Written:** 2026-03-06  
**Server:** hetzner2 · `$HETZNER2_HOST` · Xeon W-2295 36c/252GB · Ubuntu 24.04  
**Repo commit at planning:** `b557dee`

---

## Current State (as of 2026-03-06)

| Component | Status |
|---|---|
| 4-tier funnel (T0→T0.25→T1→T2) | ✅ Implemented |
| DB schema (9 tables, env metadata first-class) | ✅ |
| Synthetic bootstrap (220K communities) | ✅ Complete |
| dFBA + climate projections (synthetic) | ✅ Running |
| T1 CarveMe + COBRApy FBA | ✅ Running |
| Intervention screening | ✅ Implemented |
| Correlation, ranking, spatial, taxa enrichment | ✅ Written (standalone modules) |
| Findings generator + intervention report | ✅ |
| Receipt system, config validation | ✅ |
| 8 data source adapters (NCBI, MGnify, EMP, NEON, etc.) | ✅ Written |
| Server: hardened, provisioned, pipeline deployed | ✅ |
| **Real data ingested** | ❌ Never run |
| **Validation against published BNF measurements** | ❌ Reference files missing |
| **ingest.py wired into orchestrator** | ❌ Not in run_full_pipeline.sh |
| **fetch_references.py wired before T1** | ❌ Not in orchestrator |
| **QIIME2, Bracken, CheckM, PICRUSt2, MMseqs2, Prokka on hetzner2** | ❌ Missing |
| **nitrogen-fixation-pipeline instantiation repo** | ❌ Not created |
| **Results API** | ❌ Not built |
| **Continuous ingestion schedule** | ❌ Not configured |

---

## Build Phases

### Phase A — Missing Tool Stack on hetzner2
*Blocking: real metagenome processing at T0/T1 fails without these.*

```bash
# A1. Conda + bioinformatics stack
wget -qO /tmp/miniforge.sh https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-x86_64.sh
bash /tmp/miniforge.sh -b -p /opt/miniforge
/opt/miniforge/bin/conda init bash

# A2. QIIME2 (amplicon 16S processing)
conda create -n qiime2-2024.10 -c qiime2 -c conda-forge qiime2 -y

# A3. Bioinformatics tools via apt/conda
conda install -n base -c bioconda -c conda-forge \
  bracken kraken2 \
  picrust2 \
  prokka \
  checkm-genome \
  mmseqs2 \
  megahit metabat2 \
  -y

# A4. antiSMASH (biosynthetic gene clusters)
pip install antismash
download-antismash-databases

# A5. PICRUSt2 databases
download-db.sh  # picrust2 databases

# A6. Kraken2 + Bracken standard database (~70GB)
mkdir -p /data/pipeline/databases/kraken2
kraken2-build --standard --db /data/pipeline/databases/kraken2 --threads 36
bracken-build -d /data/pipeline/databases/kraken2 -t 36

# A7. DIAMOND DB for CarveMe (UniProt TREMBL)
mkdir -p /data/pipeline/databases/diamond
wget -P /data/pipeline/databases/ \
  ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/uniprot_sprot.fasta.gz
diamond makedb --in /data/pipeline/databases/uniprot_sprot.fasta.gz \
  -d /data/pipeline/databases/diamond/swissprot --threads 36
```

**Est. time:** 4-6 hours (Kraken2 DB build dominates)  
**Disk:** ~120 GB for databases

---

### Phase B — Reference Validation Data
*Blocking: Phase 9 (validation) always skipped without these files.*

```bash
# B1. Download EMP soil samples with published BNF measurements
# Source: Smercina et al. 2019, Nitrogen fixation dataset from EMP project
# DOI: 10.1128/mSystems.00119-19
# EMP accession EMPE00013 — dryland wheat soils with acetylene reduction assay data

python scripts/ingest.py mgnify \
  --biome-lineage "root:Environmental:Terrestrial:Agricultural soil" \
  --min-depth 10000 \
  --limit 500 \
  --db /data/pipeline/db/soil_microbiome.db

# B2. Known high-BNF community reference set
# Built from T0.25-passed synthetic communities pending real data;
# replaced with real validated set from EMP/MGnify once ingested.
mkdir -p reference/
python scripts/build_reference_set.py \
  --db /data/pipeline/db/soil_microbiome.db \
  --output reference/high_bnf_communities.biom \
  --n-communities 200 \
  --min-bnf-score 0.7

# B3. BNF measurement CSV (acetylene reduction assay values)
# Pulled from Smercina et al. supplementary data or EMP metadata
python scripts/fetch_bnf_measurements.py \
  --output reference/bnf_measurements.csv
```

**Deliverable:** `reference/high_bnf_communities.biom` + `reference/bnf_measurements.csv`  
**Unlocks:** Validation (Phase 9) — Spearman r check between ML score and measured BNF

---

### Phase C — Wire ingest.py into Orchestrator
*Critical: without real data the pipeline runs only synthetic communities.*

Add to `run_full_pipeline.sh` before Phase 0 (synthetic bootstrap):

```bash
# Phase -2: Real data ingestion (NCBI SRA + MGnify)
# Pulls soil metagenomes with nifH genes + required metadata.
# Idempotent — skips samples already in DB.
# Skip with --skip-ingest if already done or no network access.

if [[ "$SKIP_INGEST" == "false" ]]; then
  run_phase "Real Data Ingestion (NCBI SRA)" \
    python scripts/ingest.py sra \
      --config "$CONFIG" \
      --db "$DB" \
      --workers "$WORKERS" \
      --log "$LOG_DIR/ingest_sra.log"

  run_phase "Real Data Ingestion (MGnify)" \
    python scripts/ingest.py mgnify \
      --biome-lineage "root:Environmental:Terrestrial:Agricultural soil" \
      --db "$DB" \
      --min-depth 50000 \
      --workers "$WORKERS" \
      --log "$LOG_DIR/ingest_mgnify.log"
fi
```

Also add `--skip-ingest` / `SKIP_INGEST=false` to arg parser.

---

### Phase D — Wire fetch_references.py before T1
*Blocking: T1 CarveMe model construction needs reference proteomes.*

Add to `run_full_pipeline.sh` between Phase 1 (populate tables) and Phase 4 (T1 FBA):

```bash
# Phase 1b: Fetch reference genomes for T1 model construction
# Downloads representative proteomes from BV-BRC for unique genera.
# Creates /data/pipeline/models/<genus>.xml (CarveMe GEMs).

run_phase "Fetch Reference Genomes (BV-BRC)" \
  python scripts/fetch_references.py \
    --db "$DB" \
    --models-dir /data/pipeline/models \
    --workers "$WORKERS" \
    --gap-fill

# Gap-fill any models that failed initial construction
run_phase "Gap-fill Reference Models" \
  python scripts/fetch_references_gapfill.py \
    --models-dir /data/pipeline/models \
    --workers "$WORKERS"
```

---

### Phase E — Consolidate Duplicate Analysis Modules
*Currently `analysis_pipeline.py` reimplements correlation, ranking, and spatial inline,
while the standalone modules (`correlation_scanner.py`, `rank_candidates.py`,
`spatial_analysis.py`, `taxa_enrichment.py`) exist but are never called.*

Refactor `scripts/analysis_pipeline.py`:

```python
# Replace inline implementations with calls to canonical modules:
import sys; sys.path.insert(0, str(Path(__file__).parent.parent))
from correlation_scanner import scan as scan_correlations
from rank_candidates import rank as rank_communities
from spatial_analysis import analyze as analyze_spatial
from taxa_enrichment import enrich as enrich_taxa
```

Remove ~200 lines of duplicated logic from `analysis_pipeline.py`.  
All modules share the same `SoilDB` interface — swap is clean.

---

### Phase F — FastAPI Results API
*Enables other projects (cytools, meridian) and collaborators to query ranked candidates.*

New file: `api/main.py`

```python
from fastapi import FastAPI, Query
from db_utils import SoilDB

app = FastAPI(title="Soil Microbiome Pipeline API", version="1.0")
DB = "/data/pipeline/db/soil_microbiome.db"

@app.get("/candidates")
def get_candidates(
    target_id: str = "nitrogen_fixation_dryland_wheat",
    top_n: int = Query(50, le=1000),
    min_t2_stability: float = 0.0,
    soil_ph_min: float = None,
    soil_ph_max: float = None,
):
    """Return top ranked T2-passed communities with their best intervention."""
    ...

@app.get("/interventions/{community_id}")
def get_interventions(community_id: int):
    """Return all screened interventions for a community."""
    ...

@app.get("/findings")
def get_findings(limit: int = 100):
    """Return findings table entries."""
    ...

@app.get("/stats")
def get_stats():
    """DB row counts for all tables."""
    ...
```

Systemd service: `pipeline-api.service` on port 8000, behind nginx on 80/443.

---

### Phase G — Continuous Ingestion Schedule
*Without a recurring schedule, the pipeline is a one-shot run.*

Install via `provision_server.sh` Phase 13 (or run manually):

```bash
# Weekly ingest on Sunday 02:00
cat > /etc/systemd/system/pipeline-ingest.service << 'EOF'
[Unit]
Description=Soil pipeline weekly metagenome ingestion

[Service]
Type=oneshot
User=deploy
WorkingDirectory=/opt/pipeline
Environment="PATH=/opt/pipeline/.venv/bin:/usr/local/bin:/usr/bin:/bin"
ExecStart=/opt/pipeline/.venv/bin/python scripts/ingest.py both \
  --db /data/pipeline/db/soil_microbiome.db \
  --workers 36 \
  --log /var/log/pipeline/ingest-weekly.log
EOF

cat > /etc/systemd/system/pipeline-ingest.timer << 'EOF'
[Unit]
Description=Weekly soil pipeline ingestion

[Timer]
OnCalendar=Sun *-*-* 02:00:00
Persistent=true

[Install]
WantedBy=timers.target
EOF

systemctl daemon-reload
systemctl enable --now pipeline-ingest.timer
```

---

### Phase H — nitrogen-fixation-pipeline Instantiation
*The README's first named deliverable. Requires Phase A-D complete.*

```bash
# Create instantiation repo
mkdir -p ~/dev/nitrogen-fixation-pipeline
cd ~/dev/nitrogen-fixation-pipeline
git init

# Symlink core (or submodule)
# config.yaml — dryland wheat, nifH target, 5.5-7.5 pH
# nitrogen_landscape.db — instantiation-specific DB
# FINDINGS.md — generated output
# reference/ — BNF validation data

# Push to GitHub
gh repo create sethc5/nitrogen-fixation-pipeline --public
git remote add origin git@github.com:sethc5/nitrogen-fixation-pipeline.git
```

**Files needed:**
- `config.yaml` — from `config.example.yaml`, pre-filled for dryland wheat
- `nitrogen_landscape.db` — separate DB from the core template DB
- `FINDINGS.md` — generated after first real run

---

## Execution Order & Sequencing

```
NOW (parallel with current synthetic run):
  Phase A — Install tool stack on hetzner2 (background, long)

AFTER current run completes:
  Phase C — Wire ingest.py into orchestrator
  Phase D — Wire fetch_references.py before T1
  Phase E — Consolidate analysis modules
  commit → push → pull on server

THEN:
  Phase B — Fetch reference validation data (needs ingest wired first)
  Phase F — FastAPI results API
  Phase G — Ingestion timer
  Phase H — nitrogen-fixation-pipeline repo

ONGOING:
  Weekly ingest pulls new SRA/MGnify samples
  Monthly: review FINDINGS.md, promote top communities to field trial list
```

---

## Success Metrics

| Milestone | Definition |
|---|---|
| Tool stack complete | `qiime2`, `bracken`, `picrust2`, `prokka`, `checkm`, `mmseqs2` all respond on hetzner2 |
| First real data run | ≥1,000 real SRA/MGnify soil samples through T0 |
| Validation passes | Spearman r > 0.6, known community recovery test passes Phase 9 |
| T1 on real data | ≥100 communities with metabolic models built from real metagenomes |
| T2 on real data | ≥20 communities with intervention recommendations from real data |
| nitrogen-fixation-pipeline live | Repo exists, config.yaml committed, FINDINGS.md generated |
| API live | `http://hetzner2:8000/candidates` returns ranked communities |
| Continuous ingestion | Timer runs weekly, DB grows automatically |

---

## Open Scientific Questions (inform future config iterations)

1. **Which soil type × climate zone combination has the highest latent BNF potential?** — will emerge from T0.25 ML scores across real metagenomes once ingested
2. **Is pH or organic matter the stronger predictor of nifH community abundance?** — will emerge from correlation scanner on ≥10K real samples
3. **Which diazotrophs establish in sandy loam vs clay at pH 6-7?** — T2 intervention screener answer once real establishment probability model is calibrated
4. **Do EMP communities cluster geographically for BNF function?** — spatial analysis answer once real lat/lon data flows from NCBI metadata
5. **What is the CarveMe model quality distribution across soil genera?** — currently unknown; CheckM completeness scores will quantify this at T1

---

## Notes

- All database paths: `/data/pipeline/db/soil_microbiome.db` (symlinked from `/opt/pipeline/db`)
- All log paths: `/var/log/pipeline/`
- All model files: `/data/pipeline/models/`
- Reference files: `/opt/pipeline/reference/` (committed to repo, small files only)
- Raw FASTQ: never persist — process to OTU table and delete immediately
- Aspera client preferred over HTTP for SRA bulk download (10× faster)
- MGnify rate limit: 100 req/min — ingest.py queues requests automatically
