#!/usr/bin/env python
"""
scripts/benchmark_solvers.py — Compare GLPK vs HiGHS (hybrid) solver performance.

Runs FBA + FVA on a set of community models using each solver and reports:
- Mean/median solve time
- Success rate
- Flux consistency (same solution from both solvers)

Usage:
    # Quick benchmark (5 communities, single worker)
    python scripts/benchmark_solvers.py --n-communities 5 --workers 1

    # Full benchmark (100 communities)
    python scripts/benchmark_solvers.py --n-communities 100 --workers 4

    # Test single solver
    python scripts/benchmark_solvers.py --solver hybrid --n-communities 50
"""
from __future__ import annotations

import json
import logging
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any, Dict, List
from concurrent.futures import ProcessPoolExecutor, as_completed

import typer

_PROJ_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJ_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJ_ROOT))

from db_utils import _db_connect

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = typer.Typer(help="Benchmark GLPK vs HiGHS solver performance")


def _load_genus_model(sbml_path: str):
    """Load SBML model with minimal logging."""
    import cobra
    import logging
    logging.getLogger("cobra.io.sbml").setLevel(logging.ERROR)
    logging.getLogger("libsbml").setLevel(logging.ERROR)
    return cobra.io.read_sbml_model(sbml_path)


def _merge_community_models(member_models: list, max_size: int = 20):
    """Merge models with compartment namespacing."""
    import cobra
    
    models = [m for m in member_models if m is not None][:max_size]
    if not models:
        return None
    if len(models) == 1:
        return models[0].copy()

    community = models[0].copy()
    community.id = "community_model"

    added_exchange_ids = {
        rxn.id for rxn in community.reactions if rxn.id.startswith("EX_")
    }

    for i, m in enumerate(models[1:], start=1):
        suffix = f"__org{i}"
        rxns_to_add = []
        for rxn in m.reactions:
            if rxn.id.startswith("EX_"):
                if rxn.id not in added_exchange_ids:
                    added_exchange_ids.add(rxn.id)
                    rxns_to_add.append(rxn.copy())
                continue

            new_rxn = rxn.copy()
            new_rxn.id = f"{rxn.id}{suffix}"
            new_metabolites = {}
            for met, coeff in rxn.metabolites.items():
                if met.id.endswith("_e"):
                    if community.metabolites.has_id(met.id):
                        new_metabolites[community.metabolites.get_by_id(met.id)] = coeff
                    else:
                        new_metabolites[met.copy()] = coeff
                else:
                    new_met_id = f"{met.id}{suffix}"
                    if community.metabolites.has_id(new_met_id):
                        new_metabolites[community.metabolites.get_by_id(new_met_id)] = coeff
                    else:
                        new_met = met.copy()
                        new_met.id = new_met_id
                        new_metabolites[new_met] = coeff

            new_rxn.add_metabolites(new_metabolites, combine=False)
            rxns_to_add.append(new_rxn)

        community.add_reactions(rxns_to_add)

    return community


def _apply_bnf_minimal_medium(model):
    """Apply N-limited minimal medium constraints."""
    # Close all exchanges
    for rxn in model.reactions:
        if rxn.id.startswith("EX_") and rxn.lower_bound < 0:
            rxn.lower_bound = 0.0

    # Re-open inorganic whitelist
    whitelist = [
        "EX_h2o_e", "EX_h_e", "EX_co2_e", "EX_hco3_e", "EX_o2_e",
        "EX_pi_e", "EX_ppi_e", "EX_so4_e", "EX_h2s_e", "EX_fe2_e",
        "EX_fe3_e", "EX_mg2_e", "EX_k_e", "EX_na1_e", "EX_ca2_e",
        "EX_zn2_e", "EX_mn2_e", "EX_cu2_e", "EX_mobd_e", "EX_cobalt2_e",
        "EX_cl_e", "EX_sel_e", "EX_ni2_e", "EX_n2_e"
    ]
    for rxn_id in whitelist:
        try:
            rxn = model.reactions.get_by_id(rxn_id)
            rxn.lower_bound = -1000.0
        except KeyError:
            continue

    # Open carbon source
    for c_id in ["EX_glc__D_e", "EX_sucr_e", "EX_fru_e", "EX_ac_e"]:
        try:
            rxn = model.reactions.get_by_id(c_id)
            rxn.lower_bound = -10.0
            break
        except KeyError:
            continue


def _benchmark_community(
    community_id: int,
    genera_json: str,
    model_dir: str,
    solver: str,
    run_fva: bool = True
) -> Dict[str, Any]:
    """Run FBA (+ optional FVA) on a single community."""
    import cobra
    
    t0 = time.perf_counter()
    result = {
        "community_id": community_id,
        "solver": solver,
        "success": False,
        "fba_time": 0.0,
        "fva_time": 0.0,
        "total_time": 0.0,
        "flux": 0.0,
        "fva_min": 0.0,
        "fva_max": 0.0,
        "error": None
    }

    try:
        genera = json.loads(genera_json or "[]")
        genus_names = [
            g.get("name", g) if isinstance(g, dict) else str(g)
            for g in genera
        ][:10]

        member_models = []
        for gname in genus_names:
            lookup = gname if gname != "Burkholderia_non_bnf" else "Burkholderia"
            sbml = Path(model_dir) / f"{lookup}.xml"
            if not sbml.exists():
                continue
            try:
                m = _load_genus_model(str(sbml))
                m.solver = solver
                member_models.append(m)
            except Exception:
                pass

        if not member_models:
            result["error"] = "no_models"
            result["total_time"] = time.perf_counter() - t0
            return result

        community = _merge_community_models(member_models, max_size=20)
        if community is None:
            result["error"] = "merge_failed"
            result["total_time"] = time.perf_counter() - t0
            return result

        community.solver = solver
        _apply_bnf_minimal_medium(community)

        # FBA
        t_fba = time.perf_counter()
        solution = community.optimize()
        result["fba_time"] = time.perf_counter() - t_fba

        if solution.status != "optimal":
            result["error"] = f"infeasible ({solution.status})"
            result["total_time"] = time.perf_counter() - t0
            return result

        # Get nitrogenase flux
        target_rxns = [
            r for r in community.reactions
            if r.id == "NITROGENASE_MO" or r.id.startswith("NITROGENASE_MO__org")
        ]
        if target_rxns:
            result["flux"] = sum(
                abs(solution.fluxes.get(r.id, 0.0)) for r in target_rxns
            ) / len(target_rxns)

        # FVA
        if run_fva and target_rxns:
            t_fva = time.perf_counter()
            try:
                fva_result = cobra.flux_analysis.flux_variability_analysis(
                    community,
                    reaction_list=target_rxns,
                    fraction_of_optimum=0.9,
                    processes=1
                )
                result["fva_time"] = time.perf_counter() - t_fva
                result["fva_min"] = float(fva_result["minimum"].mean())
                result["fva_max"] = float(fva_result["maximum"].mean())
            except Exception as exc:
                result["fva_time"] = time.perf_counter() - t_fva
                result["error"] = f"fva_failed: {exc}"

        result["success"] = True
        result["total_time"] = time.perf_counter() - t0

    except Exception as exc:
        result["error"] = str(exc)
        result["total_time"] = time.perf_counter() - t0

    return result


def _worker_benchmark(
    batch: List[tuple],
    model_dir: str,
    solver: str,
    run_fva: bool
) -> List[Dict[str, Any]]:
    """Process a batch of communities."""
    results = []
    for community_id, genera_json, _ in batch:
        result = _benchmark_community(
            community_id, genera_json, model_dir, solver, run_fva
        )
        results.append(result)
    return results


@app.command()
def run(
    db: str = typer.Option(
        "/data/pipeline/db/soil_microbiome.db",
        "--db", "-d",
        help="Path to SQLite database"
    ),
    model_dir: str = typer.Option(
        "/data/pipeline/models",
        "--model-dir", "-m",
        help="Path to SBML model directory"
    ),
    n_communities: int = typer.Option(
        50,
        "--n-communities", "-n",
        help="Number of communities to benchmark"
    ),
    workers: int = typer.Option(
        4,
        "--workers", "-w",
        help="Number of parallel workers"
    ),
    solver: str = typer.Option(
        "both",
        "--solver", "-s",
        help="Solver to test: glpk, hybrid, or both"
    ),
    run_fva: bool = typer.Option(
        True,
        "--fva/--no-fva",
        help="Run FVA in addition to FBA"
    ),
):
    """Run solver benchmark."""
    
    valid_solvers = {"glpk", "hybrid", "both"}
    if solver not in valid_solvers:
        typer.echo(f"Invalid solver: {solver}. Choose from: {valid_solvers}")
        raise typer.Exit(1)

    solvers_to_test = ["glpk", "hybrid"] if solver == "both" else [solver]

    # Connect to DB
    conn = _db_connect(db)
    cursor = conn.cursor()

    # Fetch communities with T1-pass (have models)
    cursor.execute("""
        SELECT c.community_id, c.top_genera, '{}'
        FROM communities c
        JOIN runs r ON c.community_id = r.community_id
        WHERE r.t1_pass = 1
        LIMIT ?
    """, (n_communities,))
    
    communities = cursor.fetchall()
    conn.close()

    if not communities:
        typer.echo("No T1-pass communities found. Exiting.")
        raise typer.Exit(1)

    typer.echo(f"Benchmarking {len(communities)} communities with solvers: {solvers_to_test}")
    typer.echo(f"Workers: {workers}, FVA: {run_fva}")
    typer.echo("-" * 60)

    all_results = {}

    for test_solver in solvers_to_test:
        typer.echo(f"\nRunning {test_solver.upper()}...")
        
        # Split into batches
        batch_size = max(1, len(communities) // workers)
        batches = [
            communities[i:i + batch_size]
            for i in range(0, len(communities), batch_size)
        ]

        results = []
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = [
                pool.submit(_worker_benchmark, batch, model_dir, test_solver, run_fva)
                for batch in batches
            ]
            for future in as_completed(futures):
                results.extend(future.result())

        all_results[test_solver] = results

        # Summary stats
        successful = [r for r in results if r["success"]]
        failed = [r for r in results if not r["success"]]
        
        if successful:
            times = [r["total_time"] for r in successful]
            fba_times = [r["fba_time"] for r in successful]
            fva_times = [r["fva_time"] for r in successful if r["fva_time"] > 0]
            
            typer.echo(f"\n  {test_solver.upper()} Summary:")
            typer.echo(f"    Success: {len(successful)}/{len(results)} ({100*len(successful)/len(results):.1f}%)")
            typer.echo(f"    Total time: {sum(times):.2f}s (mean: {sum(times)/len(times):.3f}s)")
            typer.echo(f"    FBA time: {sum(fba_times):.2f}s (mean: {sum(fba_times)/len(fba_times):.3f}s)")
            if fva_times:
                typer.echo(f"    FVA time: {sum(fva_times):.2f}s (mean: {sum(fva_times)/len(fva_times):.3f}s)")
            typer.echo(f"    Mean flux: {sum(r['flux'] for r in successful)/len(successful):.3f}")
        else:
            typer.echo(f"  No successful runs with {test_solver}")

        if failed:
            errors = {}
            for r in failed:
                err = r.get("error", "unknown")[:50]
                errors[err] = errors.get(err, 0) + 1
            typer.echo(f"  Failures: {dict(list(errors.items())[:5])}")

    # Comparison
    if len(solvers_to_test) == 2:
        typer.echo("\n" + "=" * 60)
        typer.echo("COMPARISON")
        typer.echo("=" * 60)
        
        glpk_results = {r["community_id"]: r for r in all_results["glpk"] if r["success"]}
        hybrid_results = {r["community_id"]: r for r in all_results["hybrid"] if r["success"]}
        
        common_ids = set(glpk_results.keys()) & set(hybrid_results.keys())
        
        if common_ids:
            glpk_times = [glpk_results[cid]["total_time"] for cid in common_ids]
            hybrid_times = [hybrid_results[cid]["total_time"] for cid in common_ids]
            
            speedups = [g/h for g, h in zip(glpk_times, hybrid_times) if h > 0]
            
            typer.echo(f"\n  Common communities: {len(common_ids)}")
            typer.echo(f"  GLPK mean time: {sum(glpk_times)/len(glpk_times):.3f}s")
            typer.echo(f"  HiGHS mean time: {sum(hybrid_times)/len(hybrid_times):.3f}s")
            typer.echo(f"  Speedup: {sum(speedups)/len(speedups):.2f}x (mean)")
            typer.echo(f"  Speedup: {max(speedups):.2f}x (max)")
            
            # Flux consistency
            glpk_fluxes = [glpk_results[cid]["flux"] for cid in common_ids]
            hybrid_fluxes = [hybrid_results[cid]["flux"] for cid in common_ids]
            flux_diffs = [abs(g - h) / max(g, h, 1e-6) for g, h in zip(glpk_fluxes, hybrid_fluxes)]
            typer.echo(f"  Flux difference: {sum(flux_diffs)/len(flux_diffs)*100:.2f}% (mean)")


if __name__ == "__main__":
    app()
