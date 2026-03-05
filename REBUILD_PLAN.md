# Rebuild Plan: soil-microbiome-pipeline-core

_March 5, 2026 — comprehensive implementation plan incorporating strategic assessment findings_

---

## Current State

**2 of 44 Python files have working code.** `config_schema.py` (Pydantic validation) and `receipt_system.py` (JSON receipt writer) are the only functional modules. `db_utils.py` has a working connection manager and DDL but all 6 CRUD methods raise `NotImplementedError`. The remaining 41 files are scaffolds (class/function signatures with docstrings, all raising `NotImplementedError`). The architecture and API surface are well-designed — this is a build-out, not a redesign.

**What stays as-is:**
- `config_schema.py` — working, will be extended with new fields
- `receipt_system.py` — working, no changes needed
- `config.example.yaml` — working, will be extended
- Directory structure — correct, only additions needed
- The 4-tier funnel architecture — correct
- SQLite + receipts approach — correct

**What changes:**
- Schema gets new columns (strategic assessment gaps 1–4, 8–9)
- 6 CRUD methods in `db_utils.py` get implemented
- 41 scaffold/stub modules get real implementations
- 5 new modules added (metadata normalizer, CheckM wrapper, ITS handling, expression ratio, confidence tracker)
- 2 new config examples added (carbon sequestration, bioremediation)
- PATRIC references migrated to BV-BRC throughout
- Config schema extended for fungi, archaea, rhizosphere, uncertainty

---

## Phase 0: Foundation (Week 1)

_Goal: a working database layer, schema with all strategic assessment columns, and one real data adapter — so that every subsequent phase can store and query results immediately._

### 0.1 — Schema updates (Day 1, morning)

Add columns identified in strategic assessment to `db_utils.py` SCHEMA_SQL:

**`samples` table — add:**
```
site_id              TEXT,          -- links repeat visits (Gap 8)
visit_number         INTEGER,       -- temporal ordering (Gap 8)
sampling_fraction    TEXT,          -- 'rhizosphere', 'endosphere', 'bulk' (Gap 4)
```

**`communities` table — add:**
```
fungal_bacterial_ratio  REAL,       -- F:B ratio (Gap 1)
has_amoa_bacterial      BOOLEAN,    -- split bacterial vs archaeal amoA (Gap 2)
has_amoa_archaeal       BOOLEAN,    -- archaeal AOA (Gap 2)
has_laccase             BOOLEAN,    -- lignin degradation / C sequestration
has_peroxidase          BOOLEAN,    -- lignin degradation / C sequestration
its_profile             TEXT,       -- JSON: ITS fungal profile (Gap 1)
mrna_to_dna_ratio       REAL,       -- expression ratio when paired data exists (Gap 3)
```

**`runs` table — add:**
```
t1_genome_completeness_mean  REAL,  -- mean CheckM completeness of models used (Gap 5/9)
t1_genome_contamination_mean REAL,  -- mean CheckM contamination (Gap 5/9)
t1_model_confidence          TEXT,  -- 'high', 'medium', 'low' derived from genome quality (Gap 9)
t1_flux_lower_bound          REAL,  -- FVA lower bound (Gap 9)
t1_flux_upper_bound          REAL,  -- FVA upper bound (Gap 9)
t2_confidence                TEXT,  -- propagated from T1 confidence (Gap 9)
```

Remove `has_amoa` (replaced by split bacterial/archaeal flags). Keep all existing columns.

### 0.2 — Implement db_utils.py CRUD (Day 1, afternoon)

Implement all 6 stub methods:
- `upsert_sample()` — INSERT OR REPLACE using dict keys matching column names
- `upsert_community()` — INSERT OR REPLACE, return community_id
- `insert_run()` — INSERT, return run_id
- `update_run()` — UPDATE by run_id with arbitrary column dict
- `insert_finding()` — INSERT, return finding_id
- `insert_intervention()` — INSERT, return intervention_id

Add new query methods:
- `get_samples_by_site(site_id)` — for time-series analysis
- `get_runs_by_tier(tier, target_id)` — for tier-specific result retrieval
- `get_t1_confidence_distribution(target_id)` — for uncertainty reporting
- `count_by_tier(target_id)` — funnel summary stats

Write unit tests in `tests/test_db_utils.py` using in-memory SQLite.

### 0.3 — Config schema extensions (Day 2)

Extend `config_schema.py`:

**`T0Filters` — add:**
```python
required_sampling_fraction: list[str] | None = None   # ['rhizosphere', 'bulk']
min_fungal_bacterial_ratio: float | None = None        # for C-seq application
required_its_data: bool = False                        # require ITS for fungi-focused apps
```

**`T1Filters` — change:**
```python
genome_db: str = "bv-brc"          # was "patric" — Gap 6
min_genome_completeness: float = 70.0    # CheckM threshold — Gap 5
max_genome_contamination: float = 10.0   # CheckM threshold — Gap 5
```

**`T2Filters` — add:**
```python
propagate_confidence: bool = True   # carry T1 uncertainty into T2 output — Gap 9
```

**New top-level optional:**
```python
class FungalConfig(BaseModel):
    its_database: str = "unite"         # UNITE for ITS taxonomy
    amf_database: str | None = "maarjam"  # MaarjAM for AMF
    include_its_track: bool = False     # enable parallel ITS processing
```

### 0.4 — SRA metadata normalization module (Days 2–4)

New file: `compute/metadata_normalizer.py`

This is the #1 easy win from the strategic assessment. Implement:

```python
class MetadataNormalizer:
    """Normalize inconsistent SRA/MGnify metadata to canonical schema columns."""
    
    PH_ALIASES: dict        # 'pH', 'ph', 'acidity', 'soil_reaction', 'reaction', ...
    TEXTURE_ALIASES: dict   # 'soil_texture', 'textural_class', 'USDA_texture', ...
    LAND_USE_ALIASES: dict  # 'land_use', 'landuse', 'land_use_type', ...
    
    def normalize_sample(raw: dict) -> dict:
        """Map raw SRA/MGnify metadata dict to canonical samples table columns."""
    
    def parse_ph(value: str | float) -> float | None:
        """Handle: '6.5', '6.5-7.0' (take midpoint), 'slightly acidic', None."""
    
    def parse_texture(value: str) -> str | None:
        """Normalize to USDA texture classes: sand, loamy_sand, sandy_loam, ..."""
    
    def parse_coordinates(lat, lon) -> tuple[float, float] | None:
        """Handle DMS, decimal degrees, missing, swapped lat/lon."""
    
    def parse_depth(value: str) -> float | None:
        """Handle: '0-10cm', '10', '0-10 cm', 'topsoil'."""
    
    def detect_sampling_fraction(metadata: dict) -> str:
        """Infer rhizosphere/bulk/endosphere from env_material, description fields."""
```

Ship with a synonym table as a YAML/JSON data file: `compute/metadata_synonyms.yaml` — this becomes the community-improvable artifact that other soil researchers contribute to.

Write tests in `tests/test_metadata_normalizer.py` with real SRA metadata edge cases.

### 0.5 — NEON adapter implementation (Days 4–7)

Implement `adapters/neon_adapter.py` — the #3 easy win, highest-quality labeled data source.

```python
class NEONAdapter:
    NEON_API_BASE = "https://data.neonscience.org/api/v0"
    SOIL_CHEM_PRODUCT = "DP1.10078.001"   # Soil chemical properties
    SOIL_MICRO_PRODUCT = "DP1.10081.001"  # Soil microbe marker gene sequences
    
    def iter_sites() -> list[dict]:
        """List all NEON terrestrial sites with soil microbiome data."""
    
    def iter_samples(site_ids: list[str] = None, 
                     date_range: tuple = None) -> Iterator[dict]:
        """Yield normalized sample dicts ready for db.upsert_sample().
        Maps NEON fields to canonical schema including site_id, visit_number."""
    
    def get_soil_chemistry(site_id: str) -> pd.DataFrame:
        """Paired soil chemistry measurements for validation."""
    
    def download_sequence_data(sample_id: str, output_dir: Path) -> Path:
        """Download 16S amplicon FASTQ for a NEON sample."""
```

Key: NEON has `siteID` and `collectDate` fields that map directly to the new `site_id` and `visit_number` schema columns — this is the forcing function for time-series support.

### 0.6 — Phase 0 integration test (Day 7)

Write `tests/test_phase0_integration.py`:
1. Create in-memory SoilDB
2. Load 10 NEON samples via NEONAdapter
3. Normalize metadata via MetadataNormalizer
4. Upsert into DB
5. Query back, verify all columns populated including site_id, visit_number, sampling_fraction
6. Verify receipt written for the batch

**Phase 0 deliverable:** A working data ingestion pipeline — NEON API → normalized metadata → SQLite — with schema that addresses gaps 1–4, 8–9 from strategic assessment.

---

## Phase 1: T0 Compute Layer (Weeks 2–3)

_Goal: all T0 filters operational with real data. A NEON sample batch can be processed through T0 and results stored in the DB._

### 1.1 — quality_filter.py (Days 8–9)

```python
def run_quality_filter(sample: dict, config: T0Filters) -> dict:
    """Returns {pass: bool, reject_reasons: [...], metrics: {...}}"""
    # Checks: sequencing depth, observed OTUs, contamination detection
    # Uses: BIOM table stats, simple heuristics
```

No external tool dependency — pure Python on BIOM table metadata.

### 1.2 — diversity_metrics.py (Days 9–10)

```python
def compute_alpha_diversity(biom_table, phylogenetic_tree=None) -> dict:
    """Returns {shannon, simpson, chao1, observed_otus, pielou_evenness, faith_pd}"""
    # Uses: scikit-bio (skbio.diversity.alpha)
```

Depends only on `scikit-bio`. faith_pd requires a phylogenetic tree (optional — skip if no tree provided).

### 1.3 — metadata_validator.py (Days 10–11)

Extend existing scaffold. This module now delegates to `MetadataNormalizer` for parsing and adds validation logic:

```python
def validate_sample_metadata(sample: dict, config: T0Filters) -> dict:
    """Returns {pass: bool, reject_reasons: [...]}"""
    # pH in range, texture in allowed set, climate zone match
    # NEW: sampling_fraction check, fungal_bacterial_ratio check
    
def texture_class_from_fractions(clay_pct, sand_pct, silt_pct) -> str:
    """USDA soil texture triangle classification."""
    
def climate_zone_from_coords(lat, lon) -> str:
    """Koppen-Geiger lookup from coordinates. Uses static raster or API."""
```

### 1.4 — functional_gene_scanner.py (Days 11–13)

```python
SUPPORTED_GENES = {
    "nifH": {"hmm": "nifH.hmm", "description": "nitrogenase reductase"},
    "dsrAB": {"hmm": "dsrAB.hmm", "description": "sulfite reductase"},
    "mcrA": {"hmm": "mcrA.hmm", "description": "methyl-coenzyme M reductase"},
    "amoA_bacterial": {"hmm": "amoA_bact.hmm", "description": "bacterial ammonia monooxygenase"},
    "amoA_archaeal": {"hmm": "amoA_arch.hmm", "description": "archaeal ammonia monooxygenase"},
    "mmoX": {"hmm": "mmoX.hmm", "description": "methane monooxygenase"},
    "laccase": {"hmm": "laccase.hmm", "description": "lignin degradation"},
    "peroxidase": {"hmm": "peroxidase.hmm", "description": "lignin degradation"},
}

def scan_functional_genes(sequences_path: Path, 
                          target_genes: list[str],
                          method: str = "mmseqs2") -> dict:
    """Returns {gene_name: {present: bool, abundance: float, hits: int}}
    
    Splits amoA into bacterial/archaeal (addresses Gap 2).
    Uses mmseqs2 for fast homology search against curated HMM profiles.
    """
```

Key change from scaffold: bacterial vs. archaeal amoA are separate entries (Gap 2). Adds laccase and peroxidase for carbon sequestration application.

### 1.5 — tax_profiler.py (Days 13–14)

```python
def profile_taxonomy(sample_path: Path, 
                     sequencing_type: str,
                     method: str = "auto") -> dict:
    """Route to QIIME2 (16S/ITS), Bracken+Kraken2 (shotgun), or MetaPhlAn (metatranscriptome).
    
    Returns {phylum_profile: dict, top_genera: list, 
             fungal_bacterial_ratio: float | None,
             its_profile: dict | None}
    
    NEW: computes fungal_bacterial_ratio when 16S data available (Gap 1).
    NEW: routes ITS data to QIIME2 with UNITE database (Gap 1).
    """
```

### 1.6 — tax_function_mapper.py promoted to T0 (Day 14)

Move FaProTax mapping from T0.25 to T0 as a cheap second filter (Easy Win #2):

```python
def map_taxonomy_to_function(phylum_profile: dict, 
                              top_genera: list,
                              target_functions: list[str]) -> dict:
    """FaProTax-based mapping. Returns {function: {present: bool, taxa: [...], score: float}}
    
    Runs in <1 second. Catches communities with no FaProTax-identifiable 
    nitrogen cyclers even if PCR-detected nifH is present.
    """
```

### 1.7 — pipeline_core.py T0 loop (Days 14–15)

Implement the T0 section of the main pipeline loop:

```python
def run_t0_batch(samples: list[dict], config: PipelineConfig, db: SoilDB) -> list[int]:
    """Process a batch through T0 filters. Returns list of passing community_ids."""
    # 1. quality_filter
    # 2. metadata_validator
    # 3. diversity_metrics
    # 4. functional_gene_scanner
    # 5. tax_profiler
    # 6. tax_function_mapper (promoted from T0.25)
    # 7. Store results in DB
    # 8. Write receipt
```

Wire up `concurrent.futures.ProcessPoolExecutor` with configurable `workers` count. Default batch_size updated to 8,000 for the i9-9900K (Easy Win #9).

### 1.8 — Phase 1 validation (Day 15)

Run T0 on 100 NEON samples end-to-end:
- Ingest via NEONAdapter → normalize → filter → compute diversity → scan genes → profile taxonomy → store
- Verify funnel stats: how many pass, why do rejects fail?
- Confirm receipt output matches expectations

**Phase 1 deliverable:** `python pipeline_core.py --config config.yaml --tier 0` works on real NEON data and populates the DB with diversity metrics, functional gene profiles, and metadata.

---

## Phase 2: T0.25 Compute Layer (Weeks 3–4)

_Goal: ML prediction and similarity search operational. Requires T0 results as training/input data._

### 2.1 — picrust2_runner.py (Days 16–18)

```python
def run_picrust2(biom_table_path: Path,
                 output_dir: Path,
                 nproc: int = 2) -> dict:
    """Wrapper around PICRUSt2 CLI.
    Returns {pathway_abundance: Path, gene_family_abundance: Path, metrics: dict}
    
    Hardware note: run 8 parallel instances at 2 threads each on i9-9900K.
    """
```

Subprocess wrapper calling `picrust2_pipeline.py`. The main work is output parsing and error handling.

### 2.2 — humann3_shortcut.py (Days 18–19)

```python
def run_humann3(fastq_path: Path,
                output_dir: Path,
                nproc: int = 4) -> dict:
    """Wrapper around HUMAnN3 for shotgun metagenomes.
    Returns {gene_families: Path, pathway_abundance: Path, pathway_coverage: Path}
    """
```

### 2.3 — community_similarity.py (Days 19–21)

```python
class CommunitySimilaritySearch:
    """Bray-Curtis + optional UniFrac similarity against reference DB."""
    
    @classmethod
    def from_biom(cls, reference_biom_path: Path) -> "CommunitySimilaritySearch":
        """Load reference community database (e.g. high_bnf_communities.biom)."""
    
    def query(self, query_biom: Path, top_k: int = 5) -> list[dict]:
        """Returns [{reference_id, similarity_score, method}, ...]"""
```

Uses `scikit-bio` for Bray-Curtis, optional `unifrac` package for phylogenetic similarity.

### 2.4 — functional_predictor.py (Days 21–24)

```python
class FunctionalPredictor:
    """RF/GBM functional outcome prediction from OTU table + metadata features."""
    
    def train(self, X: pd.DataFrame, y: pd.Series, 
              model_type: str = "gradient_boost") -> None:
        """Train on labeled samples (e.g. NEON paired BNF measurements)."""
    
    def predict(self, X: pd.DataFrame) -> pd.DataFrame:
        """Returns DataFrame with columns: [predicted_score, uncertainty]"""
    
    def save(self, path: Path) -> None
    
    @classmethod
    def load(cls, path: Path) -> "FunctionalPredictor"
```

Key: must apply CLR (centered log-ratio) transform to compositional OTU data before training (Pitfall: compositional data bias). Uses `scikit-learn` RandomForestRegressor or GradientBoostingRegressor.

### 2.5 — pipeline_core.py T0.25 loop (Days 24–25)

```python
def run_t025_batch(community_ids: list[int], config: PipelineConfig, db: SoilDB) -> list[int]:
    """Process T0 survivors through T0.25. Returns list of passing community_ids."""
    # 1. Run PICRUSt2 / HUMAnN3 (based on sequencing_type)
    # 2. Run community similarity search against reference DB
    # 3. Run ML functional prediction
    # 4. Update run records in DB with t025_* columns
    # 5. Write receipt
```

**Phase 2 deliverable:** `python pipeline_core.py --config config.yaml --tier 025` works. ML models can be trained on NEON labeled data and applied to predict functional scores for new samples.

---

## Phase 3: T1 Metabolic Modeling Layer (Weeks 5–7)

_Goal: community FBA operational for T0.25 survivors. This is the scientific core._

### 3.1 — genome_fetcher.py with BV-BRC (Days 26–28)

Migrate from PATRIC to BV-BRC (Easy Win #5):

```python
class GenomeFetcher:
    BV_BRC_API = "https://www.bv-brc.org/api"  # was PATRIC
    
    def fetch(self, taxon_id: str, preferred_source: str = "bv-brc") -> Path:
        """Fetch best available genome. Tries BV-BRC first, falls back to NCBI RefSeq."""
    
    def _fetch_bv_brc(self, taxon_id: str) -> Path:
        """BV-BRC SOLR API query for representative genome."""
    
    def _fetch_ncbi_refseq(self, taxon_id: str) -> Path:
        """Fallback: NCBI Datasets API for RefSeq genome."""
    
    def _nearest_phylogenetic_neighbor(self, taxon_id: str) -> str:
        """For taxa with no reference genome, find nearest neighbor with one."""
```

### 3.2 — CheckM integration (Days 28–29)

New file: `compute/genome_quality.py` (Easy Win #4):

```python
def assess_genome_quality(genome_path: Path) -> dict:
    """Run CheckM on genome. Returns {completeness: float, contamination: float, 
     quality_tier: 'high'|'medium'|'low', strain_heterogeneity: float}
    
    Thresholds (from config):
      high:   ≥90% complete, ≤5% contamination
      medium: ≥70% complete, ≤10% contamination  
      low:    below medium thresholds — flag as low-confidence for FBA
    """
    
def batch_assess(genome_dir: Path, nproc: int = 8) -> pd.DataFrame:
    """Run CheckM lineage_wf on a directory of genomes. Returns quality DataFrame."""
```

### 3.3 — genome_annotator.py (Days 29–30)

```python
def annotate_genome(genome_path: Path, 
                    output_dir: Path,
                    quality: dict = None) -> Path:
    """Run Prokka annotation. Returns path to GBK file.
    
    If quality['quality_tier'] == 'low', log warning but proceed — 
    downstream FBA will carry the low-confidence flag.
    """
```

### 3.4 — model_builder.py (Days 30–33)

```python
def build_metabolic_model(genome_gbk: Path,
                          genome_quality: dict,
                          method: str = "carveme") -> cobra.Model:
    """Build genome-scale metabolic model via CarveMe.
    
    Attaches genome_quality metadata to model.notes for downstream confidence tracking.
    Gap-fills against universal template. Validates biomass production.
    Returns None if model fails validation (cannot produce biomass).
    """
```

### 3.5 — community_fba.py (Days 33–37)

The scientific core of the pipeline:

```python
def run_community_fba(member_models: list[cobra.Model],
                       environmental_constraints: dict,
                       target_pathway: str,
                       config: T1Filters) -> dict:
    """Run COBRApy community FBA.
    
    Returns {
        target_flux: float,
        flux_lower_bound: float,     # from FVA — Gap 9
        flux_upper_bound: float,     # from FVA — Gap 9
        feasible: bool,
        member_fluxes: dict,         # per-organism flux contributions
        model_confidence: str,       # aggregated from member genome qualities — Gap 9
        genome_completeness_mean: float,
        genome_contamination_mean: float,
        walltime_s: float,
    }
    
    Steps:
    1. Combine individual models into community model (shared metabolite pools)
    2. Set environmental constraints from sample metadata (pH-adjusted bounds)
    3. Run FBA maximizing community biomass
    4. Run FVA on target pathway reactions (flux variability → uncertainty bounds)
    5. Compute aggregate model confidence from member genome qualities
    """
```

### 3.6 — keystone_analyzer.py (Days 37–39)

```python
def identify_keystone_taxa(community_model,
                            target_pathway: str,
                            member_models: list,
                            knockout_threshold: float = 0.2) -> dict:
    """Sequential single-knockout analysis.
    
    Returns {
        keystone_taxa: [{taxon_id, name, flux_contribution_pct, 
                         knockout_effect, functional_role}],
        metabolic_exchanges: [{source, target, metabolite, flux}],
    }
    """
```

### 3.7 — metabolic_exchange.py (Day 39)

```python
def analyze_metabolic_exchanges(community_model,
                                  member_models: list) -> dict:
    """Cross-feeding interaction network analysis.
    Returns edge list of metabolite exchanges between community members.
    """
```

### 3.8 — pipeline_core.py T1 loop (Days 39–40)

```python
def run_t1_batch(community_ids: list[int], config: PipelineConfig, db: SoilDB) -> list[int]:
    """Process T0.25 survivors through T1 metabolic modeling."""
    # 1. Get community composition from DB
    # 2. Select representative organisms (top by abundance, cover functional guilds)
    # 3. Fetch genomes (BV-BRC/NCBI)
    # 4. Assess genome quality (CheckM)
    # 5. Annotate genomes (Prokka)
    # 6. Build metabolic models (CarveMe)
    # 7. Run community FBA with environmental constraints from sample metadata
    # 8. Run keystone analysis
    # 9. Store results with confidence metadata
    # 10. Write receipt with FBA cost tracking
```

**Phase 3 deliverable:** `python pipeline_core.py --config config.yaml --tier 1` produces metabolic flux predictions with confidence bounds and keystone taxa identification. Every T1 result carries genome quality metadata. BV-BRC replaces all PATRIC references.

---

## Phase 4: T2 Dynamics & Intervention (Weeks 8–10)

_Goal: dFBA dynamics and intervention screening operational for T1 survivors._

### 4.1 — dfba_runner.py (Days 41–46)

```python
def run_dfba(community_model,
             environmental_timeseries: pd.DataFrame,
             simulation_days: int = 45,        # reduced from 90 — Easy Win #10
             dt_hours: float = 6.0,
             perturbations: list[dict] = None) -> dict:
    """Dynamic FBA simulation.
    
    Returns {
        flux_timeseries: pd.DataFrame,  # target flux over time
        biomass_timeseries: pd.DataFrame,
        stability_score: float,
        perturbation_responses: list[dict],
        walltime_s: float,
    }
    
    Uses scipy.integrate for ODE integration with COBRApy FBA at each timestep.
    Hardware: budget 2–8 hours per community on i9-9900K. Max 2 parallel jobs.
    """
```

### 4.2 — stability_analyzer.py (Days 46–48)

```python
def compute_stability_score(flux_timeseries: pd.DataFrame,
                             perturbation_responses: list[dict]) -> dict:
    """Compute community resilience and resistance metrics.
    
    Returns {
        stability_score: float,        # 0-1 composite
        resistance: float,             # magnitude of flux change under perturbation
        resilience: float,             # rate of recovery after perturbation
        functional_redundancy: float,  # how many taxa can substitute for keystones
    }
    """
```

### 4.3 — establishment_predictor.py (Days 48–50)

```python
def predict_establishment(community_model,
                           inoculant_model: cobra.Model,
                           environmental_constraints: dict,
                           community_composition: dict) -> dict:
    """Predict whether an inoculant can establish in the community.
    
    Based on competitive exclusion theory — inoculant establishes if it fills 
    a functional niche not already occupied at saturation.
    
    Returns {
        establishment_prob: float,
        niche_overlap: float,        # with existing community members
        competitive_advantage: float,
        limiting_resources: list[str],
    }
    """
```

### 4.4 — amendment_effect_model.py (Days 50–52)

```python
AMENDMENT_DEFAULTS = {
    "biochar": {"ph_delta": [0.5, 1.5], "moisture_retention_delta": [0.05, 0.15], ...},
    "compost": {"ph_delta": [-0.2, 0.3], "organic_matter_delta": [0.5, 2.0], ...},
}

def compute_amendment_effect(amendment_type: str,
                              rate: float,
                              current_soil: dict,
                              community_model=None) -> dict:
    """Translate amendment to soil parameter changes. 
    Optionally re-run FBA with adjusted constraints.
    
    Returns {
        adjusted_soil: dict,          # predicted post-amendment soil parameters
        predicted_flux_change: float, # if community_model provided
        taxa_affected: list[dict],    # which community members benefit/suffer
    }
    """
```

### 4.5 — intervention_screener.py (Days 52–55)

```python
def screen_interventions(community_id: int,
                          community_model,
                          config: T2Filters,
                          db: SoilDB) -> list[dict]:
    """Screen all configured interventions against a community.
    
    For each bioinoculant: add to model → dFBA → establishment + effect
    For each amendment: adjust soil params → re-run FBA → effect
    For each management practice: adjust temporal constraints → dFBA → effect
    
    Returns ranked list of interventions with predicted effects and confidence.
    Confidence propagated from T1 model quality (Gap 9).
    """
```

### 4.6 — pipeline_core.py T2 loop (Days 55–56)

```python
def run_t2_batch(community_ids: list[int], config: PipelineConfig, db: SoilDB) -> list[int]:
    """Process T1 survivors through T2 dynamics + intervention screening."""
    # Max 2 parallel workers (dFBA is memory-intensive)
    # Each run: dFBA → stability → intervention screen → establishment prediction
    # Confidence field propagated from T1
    # Receipt includes dynamics walltime tracking
```

**Phase 4 deliverable:** Full 4-tier pipeline operational end-to-end. `python pipeline_core.py --config config.yaml` runs samples from NEON through T0→T0.25→T1→T2 and produces ranked communities with intervention recommendations carrying confidence metadata.

---

## Phase 5: Adapters & Data Ingestion (Weeks 10–12)

_Goal: all 8 data adapters operational. Pipeline can ingest from any configured source._

### 5.1 — ncbi_sra_adapter.py (Days 57–60)

```python
class NCBISRAAdapter:
    def search(self, query: dict) -> list[str]:
        """Search SRA using Entrez API. Returns list of accession IDs."""
    
    def download_metadata(self, accessions: list[str]) -> list[dict]:
        """Bulk metadata retrieval. Passes through MetadataNormalizer."""
    
    def download_fastq(self, accession: str, output_dir: Path, 
                       method: str = "aws") -> Path:
        """Download via AWS S3 (preferred) or Aspera. Avoid HTTP for bulk."""
```

Integrates `MetadataNormalizer` at the adapter level so all SRA data enters the pipeline normalized.

### 5.2 — mgnify_adapter.py (Days 60–62)

```python
class MGnifyAdapter:
    MGNIFY_API_BASE = "https://www.ebi.ac.uk/metagenomics/api/v1"
    
    def search_samples(self, biome_lineage: str, ...) -> list[dict]
    def get_analysis(self, analysis_id: str) -> dict
    def get_taxonomic_profile(self, analysis_id: str) -> dict
    
    # Implements request queuing: max 100 requests/minute (Known Pitfall)
```

### 5.3 — emp_adapter.py (Day 62)

```python
class EMPAdapter:
    def download_biom(self, empo_3: str) -> Path
    def iter_soil_samples(self, empo_3: str = "Soil (non-saline)") -> Iterator[dict]
```

### 5.4 — Remaining adapters (Days 63–66)

- `qiita_adapter.py` — Qiita public study search + BIOM retrieval
- `agp_adapter.py` — American Gut Project (limited soil relevance, low priority)
- `local_biom_adapter.py` — Local file ingestion (BIOM / FASTQ / FASTA)
- `redbiom_adapter.py` — Redbiom search layer for Qiita

### 5.5 — adapters/__init__.py registry (Day 66)

```python
ADAPTER_REGISTRY = {
    "sra": NCBISRAAdapter,
    "mgnify": MGnifyAdapter,
    "emp": EMPAdapter,
    "qiita": QiitaAdapter,
    "neon": NEONAdapter,
    "agp": AGPAdapter,
    "local": LocalBIOMAdapter,
    "redbiom": RedbiomAdapter,
}

def get_adapter(source: str, **kwargs):
    """Factory: return configured adapter instance by source name."""
```

**Phase 5 deliverable:** `python pipeline_core.py --config config.yaml` automatically routes to the correct adapter(s) based on `sequence_source` config. All adapters normalize metadata through `MetadataNormalizer`.

---

## Phase 6: Analysis & Reporting (Weeks 12–14)

_Goal: all reporting modules produce real output. The pipeline generates actionable documents._

### 6.1 — rank_candidates.py (Days 67–68)

```python
def rank(config_path: str, top_n: int = 50):
    """Score and rank communities by composite metric:
    Weighted combination of t025_function_score, t1_target_flux, 
    t2_stability_score, t1_model_confidence.
    
    Outputs ranked table to results/ directory.
    """
```

### 6.2 — taxa_enrichment.py (Days 68–69)

```python
def enrich(config_path: str):
    """Which genera/families/phyla are enriched in high-performing vs low-performing 
    communities? Mann-Whitney U test with FDR correction.
    
    Outputs enrichment table with effect sizes and p-values.
    """
```

### 6.3 — spatial_analysis.py (Days 69–71)

```python
def analyze(config_path: str):
    """Geographic distribution of top communities. 
    Generates world map colored by functional score using geopandas + matplotlib.
    
    Also: time-series plots for NEON sites with multiple visits (uses site_id).
    """
```

Easy Win #8 — the first spatial map.

### 6.4 — correlation_scanner.py (Days 71–72)

```python
def scan(config_path: str):
    """Automated pattern detection across accumulated DB:
    - Taxonomic enrichment in top performers
    - Metadata correlations (pH vs functional score, etc.)
    - Geographic clustering
    - Keystone taxa consistency across studies
    - Intervention success rate by soil type
    - Loser analysis (good metadata, failing T1)
    """
```

### 6.5 — findings_generator.py (Days 72–74)

```python
def generate(config_path: str):
    """Write FINDINGS.md from correlation scanner output.
    
    CRITICAL: includes confidence caveats prominently (Gap 10).
    Every finding carries: statistical support, sample size, 
    model confidence distribution of supporting samples.
    """
```

### 6.6 — intervention_report.py (Days 74–75)

```python
def report(config_path: str, top_n: int = 20):
    """Generate actionable field recommendations document.
    
    For each top intervention:
    - Soil context where it applies
    - Predicted effect size with confidence interval (Gap 9)
    - Organism or amendment details
    - Known limitations / caveats
    - Estimated cost per hectare
    
    Header includes prominent disclaimer about prediction confidence (Gap 10).
    """
```

### 6.7 — validate_pipeline.py (Days 75–76)

```python
def validate(config_path: str, 
             reference_communities: str,
             measured_function: str):
    """Known community recovery test.
    
    1. Load reference communities with measured function values
    2. Process through T0-T0.25
    3. Compute Spearman correlation: measured vs predicted function score
    4. Report pass/fail against threshold (rho > 0.6)
    5. FBA flux calibration if T1 data available
    """
```

**Phase 6 deliverable:** `python findings_generator.py --config config.yaml` produces a real FINDINGS.md. `python intervention_report.py --config config.yaml --top 20` produces actionable recommendations with confidence intervals. First spatial map generated.

---

## Phase 7: Second Instantiation + Batch Operations (Weeks 14–16)

_Goal: carbon sequestration config demonstrates generality. Batch runner operational for Hetzner deployment._

### 7.1 — carbon_sequestration config.yaml (Day 77)

Easy Win #7. Create `configs/carbon_sequestration.yaml`:

```yaml
project:
  name: "carbon-sequestration-pipeline"
  application: "carbon_sequestration"

target:
  target_function: "soil_organic_carbon_accumulation"
  target_flux:
    carbon_sequestration:
      min: 0.1
      units: "g_C_per_kg_soil_per_year"
  off_targets:
    - "methane_production"
    - "nitrous_oxide_production"

filters:
  t0:
    required_functional_genes: ["laccase", "peroxidase"]
    min_fungal_bacterial_ratio: 0.3    # NEW: fungi critical for C-seq
    required_its_data: true             # NEW: must have ITS data
```

This forces the fungi/ITS code path to be exercised and validated.

### 7.2 — batch_runner.py (Days 78–80)

```python
def launch(config_path: str, tier: str = "full",
           remote: bool = False, 
           ssh_host: str = None):
    """Launch batch pipeline job.
    
    Local: run with ProcessPoolExecutor, checkpoint every N samples.
    Remote: SSH to Hetzner, rsync config + code, launch with nohup, 
            tail receipts for progress.
    """
```

### 7.3 — merge_receipts.py (Day 80)

```python
def merge(receipts_dir: str = "receipts/", list_only: bool = False):
    """Ingest receipts from remote runs. 
    Reconcile with local DB. Report FBA cost accounting.
    """
```

### 7.4 — bioremediation config.yaml (Day 81)

Third instantiation — bioremediation-focused:

```yaml
project:
  name: "bioremediation-pipeline"
  application: "bioremediation"

target:
  target_function: "hydrocarbon_degradation"
  target_flux:
    alkane_degradation:
      min: 0.01
      units: "mmol_per_g_soil_per_day"

filters:
  t0:
    required_functional_genes: ["alkB"]
    # inverted context: want contaminated soils
```

**Phase 7 deliverable:** Pipeline proven generic across 3 applications (N-fixation, C-sequestration, bioremediation). Batch runner operational for Hetzner deployment.

---

## Phase 8: Hardening & Documentation (Weeks 16–18)

### 8.1 — Test suite (Days 82–87)

```
tests/
  test_db_utils.py          ← Phase 0 (already written)
  test_metadata_normalizer.py ← Phase 0 (already written)
  test_quality_filter.py
  test_diversity_metrics.py
  test_functional_gene_scanner.py
  test_tax_profiler.py
  test_functional_predictor.py
  test_community_fba.py      ← most important: known model → expected flux
  test_keystone_analyzer.py
  test_pipeline_integration.py  ← end-to-end small-data test
  test_adapters.py            ← mock API responses
```

Priority: `test_community_fba.py` using a known simple 2-organism community model (e.g., Escherichia coli + Bacteroides thetaiotaomicron) where expected cross-feeding flux is documented in literature. This is the single most important test in the entire repo — if community FBA gives wrong answers on a known model, everything downstream is wrong.

### 8.2 — README update (Day 87)

Update `soil-microbiome-README.md` to reflect:
- PATRIC → BV-BRC throughout
- Schema additions (fungi, archaea, rhizosphere, time-series, confidence)
- FaProTax promoted to T0
- CheckM requirement at T1
- Hardware-specific guidance section
- Confidence/uncertainty caveats in the key design decisions section
- Updated tool stack table

### 8.3 — HGT-aware nifH filtering (Day 88)

Address Gap 7 by adding a post-filter in `functional_gene_scanner.py`:

```python
def validate_nifh_functional(nifh_hits: list[dict], 
                              taxonomy: dict) -> list[dict]:
    """Cross-reference nifH hits with known functional diazotroph taxonomy.
    Flag hits from lineages where nifH is known to be non-functional or 
    acquired via HGT without regulatory context.
    
    Uses curated list of verified diazotroph lineages from literature.
    """
```

Not a perfect solution — but converts false positives from silent to flagged.

### 8.4 — Confidence propagation audit (Day 89)

Walk through every DB write path and verify:
- T1 results carry `t1_model_confidence` derived from CheckM quality
- T2 results carry `t2_confidence` propagated from T1
- Findings generator includes confidence distribution in every finding
- Intervention report headers include confidence caveats

### 8.5 — Storage management utilities (Day 90)

```python
# scripts/storage_manager.py
def cleanup_fastq(staging_dir: Path, max_age_days: int = 7):
    """Delete FASTQ files older than max_age_days after confirmed OTU processing."""

def estimate_storage(db_path: Path) -> dict:
    """Report current storage use: DB size, OTU tables, T2 outputs, staging."""
```

Critical for the 2×500GB SSD constraint.

**Phase 8 deliverable:** Test suite, updated documentation, HGT-aware filtering, confidence audit complete, storage management for hardware constraints.

---

## New Files Summary

Files to create (not in current repo):

| File | Phase | Purpose |
|------|-------|---------|
| `compute/metadata_normalizer.py` | 0 | SRA metadata normalization (Easy Win #1) |
| `compute/metadata_synonyms.yaml` | 0 | Synonym tables for metadata fields |
| `compute/genome_quality.py` | 3 | CheckM wrapper (Easy Win #4) |
| `configs/carbon_sequestration.yaml` | 7 | Second instantiation (Easy Win #7) |
| `configs/bioremediation.yaml` | 7 | Third instantiation |
| `scripts/storage_manager.py` | 8 | Disk management for 2×500GB constraint |
| `tests/test_db_utils.py` | 0 | DB unit tests |
| `tests/test_metadata_normalizer.py` | 0 | Normalizer edge case tests |
| `tests/test_quality_filter.py` | 8 | T0 filter tests |
| `tests/test_diversity_metrics.py` | 8 | Diversity computation tests |
| `tests/test_functional_gene_scanner.py` | 8 | Gene scanner tests |
| `tests/test_community_fba.py` | 8 | FBA correctness tests (most critical) |
| `tests/test_keystone_analyzer.py` | 8 | Keystone analysis tests |
| `tests/test_pipeline_integration.py` | 8 | End-to-end integration test |
| `tests/test_adapters.py` | 8 | Adapter tests with mock APIs |

---

## Files Modified (Existing Scaffolds → Real Implementation)

All 41 existing scaffold/stub Python files get implemented across Phases 0–7. No file is deleted.

---

## Dependency Changes to requirements.txt

Add:
```
checkm-genome          # genome quality assessment (Gap 5)
unifrac                # phylogenetic beta diversity (optional)
```

Update comment:
```
# BV-BRC API (was PATRIC) — no Python package, uses requests
```

---

## Timeline Summary

| Phase | Weeks | Focus | Key Deliverable |
|-------|-------|-------|-----------------|
| 0 | 1 | Foundation: schema, DB CRUD, normalizer, NEON adapter | Data flows from NEON → normalized → SQLite |
| 1 | 2–3 | T0 compute layer | `--tier 0` works on real NEON data |
| 2 | 3–4 | T0.25 compute layer | ML prediction + PICRUSt2 operational |
| 3 | 5–7 | T1 metabolic modeling | Community FBA with confidence bounds |
| 4 | 8–10 | T2 dynamics + intervention | Full 4-tier pipeline end-to-end |
| 5 | 10–12 | All 8 data adapters | Multi-source ingestion |
| 6 | 12–14 | Reporting + analysis | FINDINGS.md, intervention reports, spatial maps |
| 7 | 14–16 | Second/third instantiation + batch ops | Carbon seq + bioremediation configs, Hetzner batch |
| 8 | 16–18 | Hardening | Tests, docs, HGT filter, confidence audit |

**Total: ~18 weeks to full implementation from current scaffold state.**

Each phase produces a working, testable increment. No phase depends on institutional support — the "needs institution" items (field validation, curated Acidobacteria models, commercial pathway) are explicitly out of scope for this plan and documented as future collaboration targets.

---

## Strategic Assessment Items → Phase Mapping

| Strategic Assessment Item | Type | Addressed In |
|---------------------------|------|--------------|
| Gap 1: Fungi second-class | Gap | Phase 0.1 (schema), Phase 1.5 (tax_profiler ITS), Phase 7.1 (C-seq config) |
| Gap 2: Archaea absent | Gap | Phase 0.1 (schema), Phase 1.4 (gene scanner split amoA) |
| Gap 3: No metatranscriptomic path | Gap | Phase 0.1 (schema column), Phase 1.5 (routing) |
| Gap 4: Rhizosphere conflation | Gap | Phase 0.1 (schema), Phase 0.4 (normalizer detection) |
| Gap 5: MAG quality optional | Gap | Phase 3.2 (CheckM), Phase 3.5 (confidence in FBA) |
| Gap 6: PATRIC → BV-BRC | Gap | Phase 3.1 (genome_fetcher), Phase 8.2 (README) |
| Gap 7: HGT for nifH | Gap | Phase 8.3 (HGT-aware filter) |
| Gap 8: Time-series invisible | Gap | Phase 0.1 (schema), Phase 0.5 (NEON mapping) |
| Gap 9: No uncertainty propagation | Gap | Phase 0.1 (schema), Phase 3.5 (FBA), Phase 8.4 (audit) |
| Gap 10: T2 confidence | Gap | Phase 6.5 (findings caveats), Phase 6.6 (report header) |
| Win 1: SRA metadata normalization | Easy Win | Phase 0.4 |
| Win 2: FaProTax at T0 | Easy Win | Phase 1.6 |
| Win 3: NEON adapter first | Easy Win | Phase 0.5 |
| Win 4: CheckM integration | Easy Win | Phase 3.2 |
| Win 5: BV-BRC migration | Easy Win | Phase 3.1 |
| Win 6: site_id + visit_number | Easy Win | Phase 0.1 |
| Win 7: Carbon seq config | Easy Win | Phase 7.1 |
| Win 8: First spatial map | Easy Win | Phase 6.3 |
| Win 9: Tune batch sizes | Easy Win | Phase 1.7 |
| Win 10: Reduce T2 defaults | Easy Win | Phase 4.1 |
