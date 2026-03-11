"""
scripts/patch_diazotroph_models.py — Add Mo-nitrogenase reaction to
diazotroph SBML models so that T1 FBA can optimise genuine NH3 efflux
instead of falling back to biomass growth as a proxy.

Background
----------
AGORA2-derived soil models contain no N2-fixation pathway because AGORA2
was built for human gut microbiomes.  This script adds three objects to
each diazotroph SBML file:

  n2_c         — dinitrogen metabolite (cytoplasm, freely diffuses)
  EX_n2_e      — atmospheric N2 exchange  (lb=-1000, ub=0; uptake only)
  NITROGENASE_MO — Mo-nitrogenase stoichiometry (irreversible):
                   N2 + 16 ATP + 8 NADPH + 10 H⁺
                   → 2 NH4⁺ + H2 + 16 ADP + 16 Pi + 8 NADP⁺
                   (Burgess & Lowe 1996, Chem Rev 96:2983)

The presence of NITROGENASE_MO is used by t1_fba_batch.py as a flag to
switch the FBA objective from community biomass to NH4-efflux, producing
t1_target_flux values in mmol NH4 / gDW / h that are directly comparable
to the BNF config threshold (0.01) and to Reed 2011 field measurements.

Usage
-----
  python scripts/patch_diazotroph_models.py --models-dir /data/pipeline/models
  python scripts/patch_diazotroph_models.py --models-dir /data/pipeline/models --dry-run
"""
from __future__ import annotations

import logging
import shutil
import sys
from pathlib import Path

import cobra
import cobra.io
import typer

app = typer.Typer(add_completion=False)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Genera that carry confirmed Mo-nitrogenase (nifHDK cluster)
# Sources: Dixon & Kahn 2004 (Nat Rev Microbiol), Dos Santos et al. 2012
# ---------------------------------------------------------------------------
DIAZOTROPH_GENERA = {
    "Azospirillum",
    "Azotobacter",
    "Bradyrhizobium",
    "Rhizobium",
    "Sinorhizobium",
    "Mesorhizobium",
    "Azoarcus",
    "Herbaspirillum",
    "Gluconacetobacter",
    "Frankia",
    "Burkholderia",       # some strains only (B. vietnamiensis)
    "Nitrospira",         # commamox carries nifH in some clades
}

# Mo-nitrogenase stoichiometry (canonical, cytoplasm compartment)
# N2 + 16 ATP + 8 NADPH + 10 H⁺  →  2 NH4⁺ + H2 + 16 ADP + 16 Pi + 8 NADP⁺
# Burgess & Lowe 1996; Seefeldt et al. 2009
NITROGENASE_STOICH: dict[str, float] = {
    "n2_c":    -1.0,
    "atp_c":   -16.0,
    "nadph_c": -8.0,
    "h_c":     -10.0,
    "nh4_c":   +2.0,
    "h2_c":    +1.0,
    "adp_c":   +16.0,
    "pi_c":    +16.0,
    "nadp_c":  +8.0,
}

REACTION_ID   = "NITROGENASE_MO"
REACTION_NAME = "Mo-nitrogenase (N2 → 2NH4+); Burgess & Lowe 1996"
EXCHANGE_ID   = "EX_n2_e"
N2_MET_ID     = "n2_c"


def _patch_model(sbml_path: Path, dry_run: bool) -> str:
    """
    Load model, add nitrogenase pathway, overwrite SBML in place.

    Returns a short status string for logging.
    """
    genus = sbml_path.stem

    model = cobra.io.read_sbml_model(str(sbml_path))
    met_ids = {m.id for m in model.metabolites}
    rxn_ids = {r.id for r in model.reactions}

    # ── Skip if already patched ─────────────────────────────────────────────
    if REACTION_ID in rxn_ids:
        return f"{genus}: already patched — skipped"

    # ── Validate required cofactors exist ───────────────────────────────────
    required = {"atp_c", "nadph_c", "h_c", "nh4_c", "adp_c", "pi_c", "nadp_c"}
    missing = required - met_ids
    if missing:
        return f"{genus}: SKIPPED — missing cofactor metabolites: {missing}"

    # h2_c may be absent in some models (Herbaspirillum); add if needed
    if "h2_c" not in met_ids:
        h2 = cobra.Metabolite(
            "h2_c", name="Hydrogen", formula="H2", charge=0,
            compartment=model.metabolites.get_by_id("h_c").compartment,
        )
        model.add_metabolites([h2])

    # ── Add n2_c metabolite ──────────────────────────────────────────────────
    if N2_MET_ID not in met_ids:
        n2_c = cobra.Metabolite(
            N2_MET_ID, name="Dinitrogen", formula="N2", charge=0,
            compartment=model.metabolites.get_by_id("h_c").compartment,
        )
        model.add_metabolites([n2_c])
    else:
        n2_c = model.metabolites.get_by_id(N2_MET_ID)

    # ── Add EX_n2_e: atmospheric N2 supply (uptake only) ────────────────────
    # Standard COBRA exchange convention: {n2_c: -1} with lb=-1000 means
    # the model can take up N2 at up to 1000 mmol/gDW/h (atmospheric supply).
    # ub=0 prevents the model from secreting N2.
    if EXCHANGE_ID not in rxn_ids:
        ex_n2 = cobra.Reaction(
            EXCHANGE_ID,
            name="N2 exchange (atmospheric supply)",
            lower_bound=-1000.0,
            upper_bound=0.0,
        )
        ex_n2.add_metabolites({n2_c: -1.0})
        model.add_reactions([ex_n2])

    # ── Build NITROGENASE_MO reaction ───────────────────────────────────────
    nitr = cobra.Reaction(
        REACTION_ID,
        name=REACTION_NAME,
        lower_bound=0.0,    # irreversible (ΔG° ≈ -200 kJ/mol under physiological conditions)
        upper_bound=1000.0,
    )
    stoich_objs = {
        model.metabolites.get_by_id(mid): coeff
        for mid, coeff in NITROGENASE_STOICH.items()
    }
    nitr.add_metabolites(stoich_objs)
    # Notes annotation for audit trail
    nitr.notes["references"] = (
        "Burgess & Lowe 1996, Chem Rev 96:2983; "
        "Seefeldt et al. 2009, Annu Rev Biochem 78:701"
    )
    model.add_reactions([nitr])

    if dry_run:
        return f"{genus}: DRY-RUN — would add {REACTION_ID} + {EXCHANGE_ID}"

    # ── Back up original and overwrite ──────────────────────────────────────
    backup = sbml_path.with_suffix(".xml.bak")
    if not backup.exists():
        shutil.copy2(sbml_path, backup)

    cobra.io.write_sbml_model(model, str(sbml_path))

    n_rxns  = len(model.reactions)
    n_mets  = len(model.metabolites)
    return f"{genus}: patched ✓  ({n_rxns} rxns, {n_mets} mets)"


@app.command()
def main(
    models_dir: Path = typer.Option(
        Path("/data/pipeline/models"),
        help="Directory containing genus-level SBML files (*.xml)",
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Show what would be done without writing files",
    ),
    genera: list[str] = typer.Option(
        [], "--genus", help="Patch only these genera (default: all diazotrophs)",
    ),
) -> None:
    """
    Patch diazotroph SBML models with Mo-nitrogenase reaction and N2-exchange.

    Run before t1_fba_batch.py --real-mode so that T1 FBA targets genuine
    NH3 efflux (mmol/gDW/h) rather than biomass growth rate.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        stream=sys.stderr,
    )

    target_genera = set(genera) if genera else DIAZOTROPH_GENERA
    sbml_files = sorted(models_dir.glob("*.xml"))

    if not sbml_files:
        logger.error("No *.xml files found in %s", models_dir)
        raise SystemExit(1)

    patched = skipped = failed = 0
    for sbml in sbml_files:
        genus = sbml.stem
        if genus not in target_genera:
            continue
        try:
            status = _patch_model(sbml, dry_run)
            logger.info(status)
            if "patched ✓" in status:
                patched += 1
            else:
                skipped += 1
        except Exception as exc:
            logger.error("%s: FAILED — %s", genus, exc, exc_info=True)
            failed += 1

    logger.info(
        "Done — patched: %d, skipped/already-done: %d, failed: %d",
        patched, skipped, failed,
    )

    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    app()
