# Session Log — soil-microbiome-pipeline-core
> Newest first. Each entry: commit range, what was done, key numbers, next action.
> Rule: append before closing every session. Read last 2 entries at session start.

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
