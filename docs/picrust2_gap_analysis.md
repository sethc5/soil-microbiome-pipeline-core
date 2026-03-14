# PICRUSt2 Gap Analysis — updated 2026-03-14

## Status: UNBLOCKED (pending 237K backfill completion)

The original blocker (ASV data discarded after classification) was resolved by
the Pitfall #4 fix in commit `c8d9fc8`. The `otu_profile` column now stores
`{NR_accession: read_count}` for every processed sample.

**Backfill running:** 237K samples, PID 658581, `/tmp/otu_backfill_full.log`.
ETA: 22-30 hours from 2026-03-14 10:02 CST. Check with:
```bash
ssh -i ~/.ssh/id_ed25519_personal deploy@144.76.222.125 \
  'tail -5 /tmp/otu_backfill_full.log && \
   python3 -c "
import sqlite3
conn=sqlite3.connect(\"/data/pipeline/db/soil_microbiome.db\")
n=conn.execute(\"SELECT COUNT(*) FROM communities WHERE otu_profile IS NOT NULL AND otu_profile != \"{}\"\").fetchone()[0]
print(f\"Samples with otu_profile: {n}\")
"'
```

---

## Confirmed Compatibility Path

**Our accession format:** `NR_134097.1` (NCBI RefSeq RNA)  
**PICRUSt2 internal format:** IMG assembly IDs (`2040502012`, `2140918011`, ...)  
→ **NR_ IDs are NOT directly compatible with PICRUSt2's prebuilt marker tables.**

### Required path: `place_seqs.py` (open-reference placement)

PICRUSt2 has a reference tree at:
`/home/deploy/miniforge3/envs/picrust2/lib/python3.12/site-packages/picrust2/default_files/prokaryotic/pro_ref/`
  - `pro_ref.fna` — reference 16S sequences
  - `pro_ref.tre` — reference phylogeny (RAxML)
  - `pro_ref.hmm` — HMM profile for alignment

Our `16S_ref.fasta` (27,277 NCBI 16S sequences) contains the actual sequences
for each NR_ accession. `place_seqs.py` accepts FASTA → places on `pro_ref.tre`.

**Confirmed workflow:**

```bash
# 1. Extract unique NR_ sequences from 16S_ref.fasta
# (needed once — amortized across all 237K samples)
python3 /opt/pipeline/apps/bnf/scripts/run_picrust2.py --extract-refs \
  --ref /data/pipeline/ref/16S_ref.fasta \
  --db /data/pipeline/db/soil_microbiome.db \
  --out /data/pipeline/staging/picrust2/

# 2. place_seqs.py (phylogenetic placement of our 16S refs on pro_ref.tre)
place_seqs.py -s /data/pipeline/staging/picrust2/unique_refs.fna \
              -o /data/pipeline/staging/picrust2/placed.jplace \
              -t prokaryotic --processes 36

# 3. Build OTU table (NR_ accessions × sample_ids)
# 4. predict_metagenomes.py → KO abundances
# 5. Extract K00531 (nifH) per sample
# 6. Write to DB column: runs.nifh_k00531 (float, nullable)
```

**Script to write:** `apps/bnf/scripts/run_picrust2.py` (not yet written)

---

## Alternative: BNF Genus Fraction (simpler, taxonomy-based)

Once backfill completes, `top_genera` will contain real vsearch-derived genus
names for all samples. A faster alternative to PICRUSt2:

```python
BNF_GENERA = {
    "Azotobacter", "Azospirillum", "Rhizobium", "Sinorhizobium",
    "Mesorhizobium", "Bradyrhizobium", "Azorhizobium", "Ensifer",
    "Frankia", "Anabaena", "Nostoc", "Trichodesmium", "Crocosphaera",
    "Herbaspirillum", "Gluconacetobacter", "Burkholderia", "Paenibacillus",
    "Clostridium", "Desulfovibrio", "Methylocystis", "Methylosinus",
}
# bnf_genus_fraction = sum(top_genera[g] for g in top_genera if g in BNF_GENERA)
```

- Pros: No PICRUSt2, immediately usable from DB, interpretable
- Cons: Less mechanistic than nifH gene prediction; limited to top 10 genera
- Cite: Peoples et al. (2009), Vitousek et al. (2013) for BNF genus list

---

## Timeline

| Date | Action |
|---|---|
| 2026-03-12 | Gap documented — ASV data discarded, PICRUSt2 unusable |
| 2026-03-14 01:00 | Pitfall #4 fix — otu_profile column added, 39 sites backfilled |
| 2026-03-14 10:02 | 237K full backfill launched (PID 658581) |
| ~2026-03-15 12:00 | Backfill expected complete |
| Next session | Write run_picrust2.py + place_seqs.py run + nifH feature + LOSO v5 |

## Expected nifH LOSO Impact

The current LOSO r=0.155 is limited by feature granularity (phylum-level only).
nifH predicted abundance varies 10-100x across NEON sites based on biome:
- Tropical/subtropical (GUAN, PUUM): high BNF — cyanobacteria, Frankia
- Warm-season grassland (KONZ, CLBJ): moderate — Azospirillum, Rhizobium
- Alpine/arctic (NIWO, BARR): low BNF — short season, frozen soils

If PICRUSt2 nifH correctly captures this gradient, Spearman r vs published
BNF rates should be ≥ 0.35, providing LOSO lift from 0.155 → ~0.30+.
If not (insufficient read depth at 10K subsample), document limitation and
re-run with SUBSAMPLE_N=50,000 for top 2 sites per biome.
