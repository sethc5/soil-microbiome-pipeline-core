# Database Schema Reference (v3)

SQLite database — schema v3 (Modular). DDL source: [`db_utils.py`](../db_utils.py).  
This version decouples functional traits into a dynamic `annotations` table.

---

## Tables

| Table | Purpose |
|-------|---------|
| `samples` | Normalized sample metadata and environmental context |
| `communities` | Diversity metrics and taxonomic profiles |
| `annotations` | **New v3 Table**: Dynamic functional traits (nifH, alkB, etc.) |
| `targets` | Pipeline run target definitions and Intent configurations |
| `runs` | Funnel execution results (T1/T2) |

---

## `samples`

| Column | Type | Description |
|--------|------|-------------|
| `sample_id` | TEXT PK | Stable identifier |
| `source` | TEXT | `sra`, `mgnify`, `neon`, `local` |
| `site_id` | TEXT | Stable site identifier |
| `visit_number` | INTEGER | Visit index |
| `latitude` | REAL | |
| `longitude` | REAL | |
| `soil_ph` | REAL | |
| `soil_texture` | TEXT | |
| `organic_matter_pct` | REAL | |
| `climate_zone` | TEXT | |
| `land_use` | TEXT | |
| `sampling_date` | TEXT | |
| `metadata_json` | TEXT (JSON) | Catch-all for extra source fields |

---

## `communities`

| Column | Type | Description |
|--------|------|-------------|
| `community_id` | INTEGER PK | Auto-increment |
| `sample_id` | TEXT FK | |
| `phylum_profile` | TEXT (JSON) | Phylum -> relative abundance |
| `top_genera` | TEXT (JSON) | Top 50 genera |
| `otu_table_path` | TEXT | |
| `shannon_diversity` | REAL | |
| `pielou_evenness` | REAL | |

---

## `annotations` (Dynamic Traits)

Replaces hardcoded gene columns from v2.

| Column | Type | Description |
|--------|------|-------------|
| `annotation_id` | INTEGER PK | |
| `community_id` | INTEGER FK | |
| `trait_name` | TEXT | `nifH`, `alkB`, `laccase`, etc. |
| `value` | REAL | Relative abundance or similar metric |
| `is_present` | BOOLEAN | Presence flag |
| `method` | TEXT | `mmseqs2`, `keyword`, `community_data` |
| `meta_json` | TEXT (JSON) | Extra flags (e.g., `hgt_flagged`) |

---

## `runs`

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | INTEGER PK | |
| `community_id` | INTEGER FK | |
| `target_id` | TEXT FK | |
| `tier_reached` | INTEGER | 0, 1, 2 |
| `t1_pass` | BOOLEAN | |
| `t1_flux` | REAL | Predicted flux |
| `t1_confidence` | TEXT | `high`, `medium`, `low` |
| `t2_pass` | BOOLEAN | |
| `t2_stability` | REAL | |
| `t2_best_intervention` | TEXT | |
