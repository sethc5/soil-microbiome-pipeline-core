"""
scripts/climate_dfba.py — Climate scenario sensitivity analysis via dFBA ODE.

Reruns the BNF community ODE from dfba_batch.py under six IPCC-aligned
climate scenarios for every T2-passed community in the DB. Stores per-scenario
stability scores and BNF flux, then derives a climate sensitivity index.

Scientific basis:
  Scenarios follow IPCC AR6 RCP/SSP pathways for mid-continental agriculture
  (Masson-Delmotte et al. 2021). Temperature Δ applied via Q10 Arrhenius
  scaling; precipitation changes scale Monod kinetics through soil moisture.

  Scenario index:
    0  baseline   — current env params (reference; skip ODE, use existing T2)
    1  RCP2.6     — +1.5°C, precip ×0.97
    2  RCP4.5     — +2.0°C, precip ×0.92
    3  RCP6.0     — +3.0°C, precip ×0.85
    4  RCP8.5     — +4.5°C, precip ×0.72
    5  rewetting  — ±0°C,   precip ×1.40  (irrigation/restoration)

Output table: climate_projections
  community_id, scenario_id, scenario_name, stability_score, target_flux,
  sensitivity_index (relative Δ flux vs baseline), walltime_s

Usage:
  python scripts/climate_dfba.py \\
      --db /data/pipeline/db/soil_microbiome.db \\
      --workers 36 \\
      --sim-days 90

Est. runtime on 36 cores for 108K communities × 5 non-baseline scenarios:
  ~540K ODE runs × 1.5s each / 36 workers  ≈  3.5 hours
"""

from __future__ import annotations

import json
import logging
import math
import sqlite3
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

import numpy as np
import typer
from scipy.integrate import solve_ivp

_PROJ_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJ_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJ_ROOT))

logger = logging.getLogger(__name__)
app = typer.Typer(help="Climate scenario BNF sensitivity analysis", add_completion=False)

# ---------------------------------------------------------------------------
# Climate scenarios
# ---------------------------------------------------------------------------

SCENARIOS = [
    # id, name, delta_temp_c, precip_factor
    (1, "RCP2.6",    1.5, 0.97),
    (2, "RCP4.5",    2.0, 0.92),
    (3, "RCP6.0",    3.0, 0.85),
    (4, "RCP8.5",    4.5, 0.72),
    (5, "rewetting", 0.0, 1.40),
]

# ---------------------------------------------------------------------------
# ODE constants (mirrors dfba_batch.py)
# ---------------------------------------------------------------------------
MU_MAX_FIXER     = 0.045
MU_MAX_NITRIFIER = 0.030
MU_MAX_DECOMP    = 0.120
KS_DOC   = 0.15
KS_NH4   = 0.08
KS_NO3   = 0.10
KI_NH4   = 2.5
Y_FIXER  = 0.32
Y_NITRIF = 0.08
Y_DECOMP = 0.45
Q10 = 2.5
T_REF = 20.0


def _temperature_factor(t_day: float, mean_temp: float, amplitude: float = 8.0) -> float:
    temp = mean_temp + amplitude * math.sin(2 * math.pi * (t_day / 365 - 0.25))
    return Q10 ** ((temp - T_REF) / 10.0)


def _precipitation_factor(t_day: float, precip_mm_yr: float) -> float:
    base = precip_mm_yr / 800.0
    return max(0.05, min(1.5, base * (0.75 + 0.25 * math.sin(2 * math.pi * t_day / 90))))


def _run_ode(phylum_profile: dict, env: dict, perturbations: list[dict],
             sim_days: int = 90) -> dict:
    """Run the BNF ODE for one community+env; return {stability_score, target_flux}."""
    mean_temp   = float(env.get("temperature_c", 12.0))
    precip_mm   = float(env.get("precipitation_mm", 600.0))
    soil_ph     = float(env.get("soil_ph", 6.5))
    organic_matter = float(env.get("organic_matter_pct", 2.0))
    ph_factor_bnf = math.exp(-0.5 * ((soil_ph - 6.5) / 1.2) ** 2)

    pert_by_day: dict[int, dict] = {int(p.get("day", 0)): p for p in perturbations}
    state = {"drought_strength": 1.0, "fertilizer_boost": 1.0, "fertilizer_decay": 1.0}

    def rhs(t, y):
        B_fix, B_nit, B_dec, DOC, NH4, NO3, PO4 = [max(v, 0.0) for v in y]
        t_fac = _temperature_factor(t, mean_temp)
        p_fac = _precipitation_factor(t, precip_mm)
        day_int = int(t)
        if day_int in pert_by_day:
            p = pert_by_day.pop(day_int)
            ptype = p.get("type", "")
            sev = float(p.get("severity", 0.5))
            if ptype == "drought":
                state["drought_strength"] = 1.0 - sev * 0.8
            elif ptype == "rewetting":
                state["drought_strength"] = 1.0
            elif ptype == "fertilizer":
                state["fertilizer_boost"] = 1.0 + sev * 3.0
                state["fertilizer_decay"] = 0.95
        state["fertilizer_boost"] = max(1.0, state["fertilizer_boost"] * state["fertilizer_decay"])
        p_eff = p_fac * state["drought_strength"]

        nh4_suppression = KS_NH4 / (KS_NH4 + NH4 + 1e-9)
        mu_fix = MU_MAX_FIXER * nh4_suppression * ph_factor_bnf * t_fac * p_eff
        bnf_flux = mu_fix * B_fix / Y_FIXER
        mu_nit = max(0.0, MU_MAX_NITRIFIER * (NH4 / (KS_NH4 + NH4)) * (KI_NH4 / (KI_NH4 + NH4)) * t_fac * p_eff)
        n_avail = (NH4 + NO3 * state["fertilizer_boost"]) / (KS_NO3 + NH4 + NO3)
        mu_dec = MU_MAX_DECOMP * (DOC / (KS_DOC + DOC)) * n_avail * t_fac * p_eff

        mort_fix = 0.005 * B_fix
        mort_nit = 0.004 * B_nit
        mort_dec = 0.008 * B_dec
        doc_input = 0.002 * organic_matter * p_eff * t_fac
        doc_consumed = mu_dec * B_dec / Y_DECOMP
        doc_from_mort = 0.6 * (mort_fix + mort_nit + mort_dec)
        nh4_input = bnf_flux + doc_input * 0.05 * state["fertilizer_boost"]
        nh4_nitrified = mu_nit * B_nit / Y_NITRIF
        nh4_assimilation = mu_dec * B_dec * 0.12
        nh4_from_mort = 0.4 * (mort_fix + mort_nit + mort_dec) * 0.1

        return [
            mu_fix * B_fix - mort_fix,
            mu_nit * B_nit - mort_nit,
            mu_dec * B_dec - mort_dec,
            doc_input + doc_from_mort - doc_consumed,
            nh4_input + nh4_from_mort - nh4_nitrified - nh4_assimilation,
            nh4_nitrified - mu_dec * B_dec * 0.04,
            0.0005 * organic_matter * t_fac - mu_dec * B_dec * 0.01,
        ]

    proto = phylum_profile.get("Proteobacteria", 0.20)
    nitro = max(phylum_profile.get("Nitrospirae", 0.012) + phylum_profile.get("Thaumarchaeota", 0.02), 0.005)
    decomp = max(1.0 - proto - nitro, 0.01)
    y0 = [proto * 2.0, nitro * 2.0, decomp * 2.0,
          0.25 + organic_matter * 0.05, 0.05, 0.08, 0.02]

    try:
        sol = solve_ivp(rhs, t_span=(0.0, float(sim_days)), y0=y0,
                        method="RK45", max_step=0.25, rtol=1e-4, atol=1e-7)
        if not sol.success:
            return {"stability_score": 0.0, "target_flux": 0.0}
        B_fix_traj = sol.y[0]
        nh4_mean = float(np.mean(sol.y[4]))
        supp = 0.5 / (0.5 + nh4_mean + 1e-9)
        bnf_traj = B_fix_traj * MU_MAX_FIXER * supp * ph_factor_bnf / Y_FIXER
        target_flux = float(np.mean(bnf_traj))
        cv = bnf_traj.std() / bnf_traj.mean() if bnf_traj.mean() > 1e-12 else 1.0
        stability = float(max(0.0, 1.0 - min(cv, 1.0)))
        return {"stability_score": stability, "target_flux": target_flux}
    except Exception:
        return {"stability_score": 0.0, "target_flux": 0.0}


def _run_community_scenarios(community_id: int, profile_json: str, env_json: str,
                             baseline_flux: float, sim_days: int) -> list[dict]:
    """Run all 5 non-baseline scenarios for one community. Returns list of result dicts."""
    t0 = time.perf_counter()
    try:
        profile = json.loads(profile_json or "{}")
        env_base = json.loads(env_json or "{}")
    except Exception:
        profile, env_base = {}, {}

    perturbations = [
        {"type": "drought",    "day": 30, "severity": 0.55},
        {"type": "fertilizer", "day": 50, "severity": 0.40},
        {"type": "rewetting",  "day": 60, "severity": 0.80},
    ]

    results = []
    for scenario_id, scenario_name, delta_t, precip_fac in SCENARIOS:
        env_scen = dict(env_base)
        env_scen["temperature_c"] = float(env_base.get("temperature_c", 12.0)) + delta_t
        env_scen["precipitation_mm"] = float(env_base.get("precipitation_mm", 600.0)) * precip_fac

        res = _run_ode(profile, env_scen, list(perturbations), sim_days)
        flux = res["target_flux"]
        # Sensitivity index: (scenario_flux - baseline_flux) / baseline_flux
        sensitivity = (flux - baseline_flux) / (baseline_flux + 1e-12)

        results.append({
            "community_id":    community_id,
            "scenario_id":     scenario_id,
            "scenario_name":   scenario_name,
            "stability_score": res["stability_score"],
            "target_flux":     flux,
            "sensitivity_index": float(sensitivity),
            "walltime_s":      time.perf_counter() - t0,
            "error":           None,
        })
    return results


def _worker_batch(batch: list[tuple]) -> list[dict]:
    """Worker: list of (community_id, profile_json, env_json, baseline_flux, sim_days)."""
    results = []
    for community_id, profile_json, env_json, baseline_flux, sim_days in batch:
        results.extend(_run_community_scenarios(
            community_id, profile_json, env_json, baseline_flux, sim_days))
    return results


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS climate_projections (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    community_id    INTEGER NOT NULL,
    scenario_id     INTEGER NOT NULL,
    scenario_name   TEXT NOT NULL,
    stability_score REAL,
    target_flux     REAL,
    sensitivity_index REAL,
    walltime_s      REAL,
    UNIQUE(community_id, scenario_id)
);
CREATE INDEX IF NOT EXISTS idx_cp_community ON climate_projections(community_id);
CREATE INDEX IF NOT EXISTS idx_cp_scenario  ON climate_projections(scenario_id);
CREATE INDEX IF NOT EXISTS idx_cp_flux      ON climate_projections(target_flux);
"""


def _ensure_schema(db_path: str) -> None:
    conn = sqlite3.connect(db_path, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(_SCHEMA_SQL)
    conn.commit()
    conn.close()


def _fetch_communities(db_path: str, n_max: int) -> list[tuple]:
    """Load T2-passed communities not yet in climate_projections for all 5 scenarios."""
    conn = sqlite3.connect(db_path, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    # Communities that have < 5 scenarios already recorded
    rows = conn.execute(
        """SELECT c.community_id, c.phylum_profile,
                  json_object(
                    'soil_ph',           COALESCE(s.soil_ph, 6.5),
                    'organic_matter_pct',COALESCE(s.organic_matter_pct, 2.0),
                    'temperature_c',     COALESCE(s.temperature_c, 12.0),
                    'precipitation_mm',  COALESCE(s.precipitation_mm, 600.0)
                  ) AS env_json,
                  COALESCE(r.t1_target_flux, 0.001) AS baseline_flux
           FROM runs r
           JOIN communities c ON r.community_id = c.community_id
           JOIN samples s     ON r.sample_id = s.sample_id
           WHERE r.t2_pass = 1
             AND c.phylum_profile IS NOT NULL
             AND (SELECT COUNT(*) FROM climate_projections cp
                  WHERE cp.community_id = c.community_id) < 5
           ORDER BY r.t1_target_flux DESC
           LIMIT ?""",
        (n_max,)
    ).fetchall()
    conn.close()
    return [(r[0], r[1], r[2], float(r[3] or 0.001)) for r in rows]


def _write_results(db_path: str, results: list[dict]) -> int:
    conn = sqlite3.connect(db_path, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    n = 0
    for r in results:
        if r.get("error"):
            continue
        try:
            conn.execute(
                """INSERT OR IGNORE INTO climate_projections
                   (community_id, scenario_id, scenario_name,
                    stability_score, target_flux, sensitivity_index, walltime_s)
                   VALUES (?,?,?,?,?,?,?)""",
                (r["community_id"], r["scenario_id"], r["scenario_name"],
                 r["stability_score"], r["target_flux"],
                 r["sensitivity_index"], r["walltime_s"])
            )
            n += 1
        except Exception as exc:
            logger.debug("Write error cid=%s scen=%s: %s", r.get("community_id"), r.get("scenario_name"), exc)
    conn.commit()
    conn.close()
    return n


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

@app.command("run")
def run(
    db_path:      Path          = typer.Option(Path("/data/pipeline/db/soil_microbiome.db"), "--db"),
    n_communities:int           = typer.Option(120_000, "--n-communities", "-n"),
    workers:      int           = typer.Option(36,      "--workers", "-w"),
    batch_size:   int           = typer.Option(20,      "--batch-size"),   # smaller: 5 ODE per community
    sim_days:     int           = typer.Option(90,      "--sim-days"),
    log_path:     Optional[Path] = typer.Option(Path("/var/log/pipeline/climate_dfba.log"), "--log"),
):
    """Run climate scenario dFBA ODE on all T2-passed communities."""
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if log_path:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(str(log_path)))
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
                        handlers=handlers, force=True)

    logger.info("=== climate_dfba starting: %d scenarios × up to %d communities, %d workers ===",
                len(SCENARIOS), n_communities, workers)

    _ensure_schema(str(db_path))

    communities = _fetch_communities(str(db_path), n_communities)
    logger.info("Found %d communities needing climate projection", len(communities))
    if not communities:
        logger.info("All communities already have climate projections — nothing to do.")
        raise typer.Exit(0)

    # Build work batches
    def _chunks(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i : i + n]

    # Append sim_days to each tuple
    batches = [[(cid, pj, ej, bf, sim_days) for cid, pj, ej, bf in chunk]
               for chunk in _chunks(communities, batch_size)]

    logger.info("Submitting %d batches × %d communities (%d ODE runs total) to %d workers",
                len(batches), batch_size, len(communities) * len(SCENARIOS), workers)

    t_start = time.time()
    n_written = 0

    with ProcessPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_worker_batch, batch): i for i, batch in enumerate(batches)}
        for fut in as_completed(futures):
            batch_idx = futures[fut]
            try:
                batch_results = fut.result()
                n = _write_results(str(db_path), batch_results)
                n_written += n
                elapsed = time.time() - t_start
                rate = n_written / elapsed if elapsed > 0 else 0
                communities_done = n_written // len(SCENARIOS)
                if batch_idx % 50 == 0 or communities_done % 5000 < batch_size:
                    logger.info(
                        "Batch %5d/%d — %7d scenario-rows written (%5d communities, %.0f rows/s, %.1f min)",
                        batch_idx + 1, len(batches), n_written, communities_done, rate, elapsed / 60
                    )
            except Exception as exc:
                logger.error("Batch %d failed: %s", batch_idx, exc)

    elapsed = time.time() - t_start
    communities_done = n_written // len(SCENARIOS)
    logger.info("=== climate_dfba complete: %d scenario-rows, %d communities in %.1f min ===",
                n_written, communities_done, elapsed / 60)


if __name__ == "__main__":
    app()
