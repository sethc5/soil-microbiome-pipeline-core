# Contributing to soil-microbiome-pipeline-core

## Highest Value Areas

1. **New application instantiations** — carbon sequestration and bioremediation have the clearest pipeline structure next after nitrogen fixation.
2. **Metabolic model quality** — CarveMe model construction for Acidobacteria and Verrucomicrobia (notoriously hard to model in soil).
3. **Database adapters** — NEON data portal, JGI IMG/M, EBI MGnify improvements.
4. **T2 dynamics engines** — iDynoMiCS integration for spatially explicit community modeling.
5. **Bug documentation** — SRA metadata normalization issues especially. Add rows to the pitfalls table in the README before adding workarounds.
6. **Validation datasets** — curated datasets with both metagenome data and measured functional outcomes (BNF assays, SOC measurements, disease suppression trials).

## Dev Setup

```bash
git clone https://github.com/sethc5/soil-microbiome-pipeline-core.git
cd soil-microbiome-pipeline-core
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
conda install -c bioconda qiime2 picrust2 mmseqs2 prokka bracken kraken2 humann3
```

## Before Submitting a PR

- Run `python validate_pipeline.py` against the reference validation set
- Add/update docstrings for any new compute or adapter module
- If you change the DB schema, add a migration step to `db_utils.py`
- Document any new pitfall in the README pitfalls table

## Code Style

- Black formatting, 99-char line length
- Type hints on all public functions
- `raise NotImplementedError` stubs preferred over silent pass

## License

PolyForm Noncommercial 1.0.0 — all contributions must be compatible. Commercial use requires a separate licence from the maintainer.
