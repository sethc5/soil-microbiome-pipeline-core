# PICRUSt2 Gap Analysis — 2026-03-12

## The Problem

`core/compute/picrust2_runner.py` requires:
1. `asv_table_biom` — per-ASV count table (BIOM format)
2. `rep_seqs_fasta` — representative 16S sequences for each ASV (FASTA)

`scripts/ingest/process_neon_16s.py` produces only:
- `phylum_profile` — aggregated phylum-level relative abundances (stored in DB)
- `top_genera` — aggregated genus-level relative abundances (stored in DB)

**ASV-level data is discarded** after vsearch classification. PICRUSt2 cannot
run from phylum percentages — it needs the actual sequence counts per ASV.

## What Needs to Happen (Next Session)

### Option A — Check if NEON BIOM files exist on server
NEON data portal provides pre-processed BIOM files alongside raw FASTQs.
If `ingest_neon_biom.py` downloaded them, they may still be in staging.

```bash
ssh -i ~/.ssh/id_ed25519_personal deploy@144.76.222.125 \
  'find /data/pipeline/staging -name "*.biom" -o -name "*.biom.gz" | head -20'
```

If BIOM files exist:
1. Run PICRUSt2 on the BIOM files (one per NEON site collection event)
2. Extract nifH pathway (KEGG K00531 / MetaCyc PWY-5345) per sample
3. Write nifH abundance to `runs.t025_nifh_abundance` in DB
4. Retrain RF v3 with nifH as 65th feature
5. Re-run LOSO CV (target r ≥ 0.45 with nifH)

### Option B — Re-process with ASV-saving pipeline
If BIOM files are gone, `process_neon_16s.py` needs to be modified to:
1. Save ASV-level UC output (not just aggregated profiles)
2. Write a per-sample BIOM file to staging
3. Then chain PICRUSt2 on each BIOM

This would require re-processing ~237K samples (same vsearch step, just different
output format). Runtime: similar to original ingestion run.

### Option C — NEON BIOM download
NEON API provides processed amplicon BIOM files directly. Endpoint:
  `https://data.neonscience.org/api/v0/data/DP1.10081.001/{site}/{date}`

These are DADA2-processed ASV tables — exactly what PICRUSt2 needs.
`scripts/ingest/ingest_neon_biom.py` already handles this — it may need
to be run again if BIOMs weren't kept.

## nifH Feature Plan

Once PICRUSt2 is run:
- KEGG pathway K00531 (nitrogenase Mo-Fe protein alpha chain) = nifH proxy
- Or MetaCyc PWY-5345 (nitrogen fixation I)
- Add as feature: `runs.t025_nifh_pathway_abundance` (float, nullable)
- In retrain script: add nifH abundance alongside phyla + env features
- Expected: LOSO r increases from 0.155 toward 0.45+

## Key Check for Next Session

```bash
# 1. Check sync first
bash scripts/ops/check_sync.sh

# 2. Check BIOM file availability
ssh -i ~/.ssh/id_ed25519_personal deploy@144.76.222.125 \
  'find /data/pipeline/staging -name "*.biom*" 2>/dev/null | wc -l && \
   ls /data/pipeline/staging/ | head -20'

# 3. Check PICRUSt2 installation
ssh -i ~/.ssh/id_ed25519_personal deploy@144.76.222.125 \
  'which picrust2_pipeline.py 2>/dev/null || which picrust2_pipeline 2>/dev/null || echo NOT_FOUND'
```
