# Session Log — soil-microbiome-pipeline-core
> Newest first. Each entry: commit range, what was done, key numbers, next action.
> Rule: append before closing every session. Read last 2 entries at session start.

---

## 2026-03-14 10:30 CST — 5b7fa48→(next)
**Done:** Confirmed PICRUSt2 compatibility gap: NR_ accessions NOT directly compatible with PICRUSt2 IMG IDs — must use place_seqs.py path. Updated picrust2_gap_analysis.md with confirmed workflow (place_seqs.py + 16S_ref.fasta extraction + predict_metagenomes.py). BNF genus fraction documented as simpler alternative. Both require backfill to complete.
**Key metrics:** PICRUSt2 in picrust2 conda env v2.6.3; backfill PID 658581 running; 39/47 sites have otu_profile today
**Blocked by:** 237K backfill (PID 658581, ETA ~2026-03-15 12:00 CST)
**Next:** Check backfill completion (tail /tmp/otu_backfill_full.log); write run_picrust2.py; run place_seqs.py on unique NR_ refs; compute nifH per sample; LOSO v5

## 2026-03-14 10:02 CST — 6b0d23b→f7a6fd0
**Done:** Verified backfill results (39 sites OK, NR_ accession format confirmed). Added --backfill-otu flag to process_neon_16s.py — finds t0_pass=1 samples missing otu_profile, safe (no t0_pass reset). Launched full 237K OTU backfill (PID 658568, /tmp/otu_backfill_full.log, 36 workers, ETA ~22-30h).
**Key metrics:** 39 sites have real otu_profile; 237,567 backfill pending; accessions=NR_ NCBI format (PICRUSt2 2.6.3 in picrust2 env)
**Blocked by:** Backfill running overnight — need ~22-30h to complete
**Next:** tail /tmp/otu_backfill_full.log (tomorrow); verify n_ok >200K; spot-check NR_ vs PICRUSt2 ref; write run_picrust2.py; build nifH feature; LOSO v5

## 2026-03-14 01:04 CST — ec1a396→c8d9fc8 (+ backfill running)
**Done:** Pitfall #4 fix complete + backfill launched. process_neon_16s.py modified to preserve OTU accession counts (otu_profile column, auto-added). Backfill: reset t0_pass=NULL for 47 samples (1/site) → 269 samples processed (script found all pending with notes). otu_profile column confirmed added to DB. First completions showing: KONA ✓ (14 phyla), KONZ ✓ (16 phyla). JGI shotgun correctly skipped. ETA ~35 min.  
**Key metrics:** Backfill PID 655428, /tmp/otu_backfill.log; otu_profile column added to communities table  
**Blocked by:** Backfill running (background) — need to wait for completion, then validate OTU format vs PICRUSt2 expectation  
**Next:** `tail /tmp/otu_backfill.log` → check n_ok; spot-check otu_profile JSON; write run_picrust2.py script to build BIOM from otu_profile + run PICRUSt2 on 47 pilot samples

## 2026-03-14 00:40 CST — c552965→(next commit)
**Done:** LOSO v4 (corrected labels) ran: r=0.1123 vs baseline 0.1552 (Δ=-0.043, within SE=0.15 for n=47 — NOT statistically significant). CLEAN NEGATIVE RESULT: label quality is NOT a binding constraint. Feature granularity IS the bottleneck — phylum-level 16S cannot distinguish within-biome BNF variation. Corrected labels retained (more scientifically accurate). STATUS.md updated.  
**Key metrics:** LOSO v4 r=0.1123 (not sig different from v2 r=0.1552); label correction experiment ruled out label quality as bottleneck  
**Blocked by:** Feature granularity ceiling — phylum-level 16S + 5 env vars = LOSO r ~0.15 max  
**Next:** Fix process_neon_16s.py to save OTU-level counts → enables real genus features + PICRUSt2 nifH. This is the confirmed highest-ROI next step.

## 2026-03-14 00:25 CST — 1a88433→ec2daf5
**Done:** SESSION_LOG.md + Rule 10 added; 8 unmapped NEON sites corrected in fetch_published_bnf.py (TOOL 0.7→2.0 kg/ha/yr, UNDE/STEI/TREE 0.7→1.1-1.2, TALL 0.7→1.0, UKFS 0.7→1.3, YELL 0.7→0.9, STER 0.7→0.6); bnf_measurements.csv rebuilt on server (all 47 sites now mapped); LOSO v4 launched (PID 650980, /tmp/loso_v4.log, ~8 min)  
**Key metrics:** bnf_measurements.csv: 237,567 samples, 47/47 sites mapped (was 39/47 properly mapped); unique label values will increase from 21 to ~29+  
**Blocked by:** LOSO v4 results not yet available (running)  
**Next:** Check /tmp/loso_v4.log → loso_report_v4labels.json → compare r to 0.1552; if improved, retrain v4 model with new labels; update STATUS.md

---

## 2026-03-14 00:10 CST — cae7129→1a88433
**Done:** v3 training completed overnight (CV R²=0.462 ≈ v2 0.448 — feature engineering confirmed not bottleneck); top_genera provenance confirmed non-useful (FBA-derived, Pitfall #9); LOSO per-site analysis — spearman_r field fixed in loso_report.json on server; label quality bottleneck identified  
**Key metrics:** LOSO r=0.1552 confirmed, v3 CV R²=0.462, 21 unique labels/47 sites  
**Blocked by:** Biome-averaged labels — 11 sites share rate=0.085 (WI forest, CO prairie, AK boreal, NC forest, ND prairie — identical label despite different biomes). Within-label noise is as much a bottleneck as n_labelled_sites.  
**Largest LOSO misses:** HEAL +0.50, GRSM +0.47 (over-predicted), GUAN -0.46, PUUM -0.45, BARR -0.28 (under-predicted)  
**Next:** Site-specific BNF literature search for 11 tied-label sites → differentiate labels → re-run LOSO

---

## 2026-03-13 11:10 CST — 0fb832b→cae7129
**Done:** Investigated genus-level features for RF v3; found top_genera is 26 BNF-curated genera with uniform ~45% frequency — likely FBA-derived not real vsearch (Pitfall #9); wrote retrain_bnf_surrogate_v3.py with expanded env features (N, P, moisture, bulk_density); launched v3 training on server (PID 637631)  
**Key metrics:** top_genera: 26 genera, 95% sample coverage, 0 parsed as real taxonomy  
**Blocked by:** 3/4 new env features 0% populated in DB — v3 will ≈ v2  
**Next:** Check v3 results (expected ≈ v2); pivot to label quality

---

## 2026-03-13 10:49 CST — 481902d→0fb832b
**Done:** Date stamps updated (3-12→3-13); STATUS.md completed steps + accurate next priorities; .clinerules Rule 9 state updated  
**Next:** Query DB for top_genera coverage → genus features for v3

---

## 2026-03-12 (multiple commits) — initial→481902d
**Done:** Full session from scratch — git sync check established (check_sync.sh); v2 model retrained on real labels (CV R²=0.448, ROC-AUC=0.807); LOSO CV run (r=0.155, 474s); pH-stratified enrichment (no phyla enriched ≥2 pH bins); GROUND_TRUTH_progress.md + GROUND_TRUTH_pitfalls.md written (8 pitfalls documented); STATUS.md created; .clinerules Rules 1-9 finalized  
**Key metrics:** LOSO r=0.155, CV R²=0.448, ROC-AUC=0.807, 237,567 samples, 47 sites  
**Resolved:** Circular validation (v1), label leakage (r=0.87 trap), unit mismatch, git drift  
**Next:** Feature engineering experiments (proved not bottleneck in subsequent sessions)
