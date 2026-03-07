"""
scripts/dfba_batch.py — Batch dFBA-style ODE simulations over T0.25-passing
synthetic communities, no COBRApy dependency required.

Implements a simplified community metabolic ODE:
  - 3 functional guilds: N-fixers (BNF), Nitrifiers (AOB/AOA), Decomposers
  - Guild biomasses parameterized from phylum profile
  - Nutrient dynamics: DOC, NH4, NO3, PO4 pools with guild-specific fluxes
  - Seasonal cycle (temperature × precipitation modulation over 90 days)
  - 3 perturbation events: drought (day 30), fertilizer pulse (day 50),
    rewetting (day 60)

Scientific basis: simplified Lotka-Volterra + Monod kinetics for soil guilds,
following Hunt et al. (1987) and Manzoni & Porporato (2009) soil biogeochemistry.

Target flux: BNF rate (mmol N g-soil h-1), integrated over 90 days.

Uses: scipy.integrate.solve_ivp, numpy, concurrent.futures
Workers: configurable; designed for 36-core Hetzner box.

Usage:
  python scripts/dfba_batch.py run \\
      --db /data/pipeline/db/soil_microbiome.db \\
      --min-bnf 0.60 \\
      --n-communities 10000 \\
      --workers 36 \\
      --sim-days 90
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

from db_utils import _db_connect  # noqa: E402

logger = logging.getLogger(__name__)
app = typer.Typer(help="Batch dFBA ODE simulations for top BNF communities", add_completion=False, invoke_without_command=True)

# ---------------------------------------------------------------------------
# ODE model constants (Monod kinetics, soil typical values)
# ---------------------------------------------------------------------------

# Maximum specific growth rates (h-1)
MU_MAX_FIXER     = 0.045   # slow-growing; free-living diazotrophs
MU_MAX_NITRIFIER = 0.030   # AOB/AOA even slower
MU_MAX_DECOMP    = 0.120   # heterotrophs faster

# Half-saturation constants (mmol g-soil-1)
KS_DOC   = 0.15    # DOC for decomposers
KS_NH4   = 0.08    # NH4 for nitrifiers
KS_NO3   = 0.10    # NO3 as N-source
KI_NH4   = 2.5     # NH4 inhibition of nitrifiers at high conc
KS_N2    = 0.0     # N2 unlimited (atmospheric)

# Yield coefficients (g-biomass / mmol-substrate)
Y_FIXER   = 0.32
Y_NITRIF  = 0.08
Y_DECOMP  = 0.45

# N-fixation stoichiometry: 8e- + 16 ATP per N2 (nitrogenase)
# Cost factor reduces fixer growth when N is available (suppression)
N_SUPPRESS_KI = 0.5   # NH4 mM at which N-fixation is 50% suppressed

# Seasonal temperature modulation (Arrhenius Q10 ≈ 2.5)
Q10 = 2.5
T_REF = 20.0  # °C reference temperature


def _temperature_factor(t_day: float, mean_temp: float, amplitude: float = 8.0) -> float:
    """Sinusoidal temperature with Q10 to scale all rates."""
    temp = mean_temp + amplitude * math.sin(2 * math.pi * (t_day / 365 - 0.25))
    return Q10 ** ((temp - T_REF) / 10.0)


def _precipitation_factor(t_day: float, precip_mm_yr: float) -> float:
    """Soil moisture factor — simple sinusoidal with drought dip at day 30-50."""
    base = precip_mm_yr / 800.0  # normalise to typical range
    return max(0.05, min(1.5, base * (0.75 + 0.25 * math.sin(2 * math.pi * t_day / 90))))


def _build_odes(guild_init: dict, env: dict, perturbations: list[dict]):
    """
    Return an ODE RHS function for scipy.integrate.solve_ivp.

    State vector y = [B_fix, B_nit, B_dec, DOC, NH4, NO3, PO4]
      B_fix: N-fixer biomass (g / dm3 soil)
      B_nit: Nitrifier biomass
      B_dec: Decomposer (heterotroph) biomass
      DOC:   Dissolved organic carbon (mmol g-1)
      NH4:   Ammonium (mmol g-1)
      NO3:   Nitrate (mmol g-1)
      PO4:   Phosphate (mmol g-1)
    """
    mean_temp   = float(env.get("temperature_c", 12.0))
    precip_mm   = float(env.get("precipitation_mm", 600.0))
    soil_ph     = float(env.get("soil_ph", 6.5))
    organic_matter = float(env.get("organic_matter_pct", 2.0))

    # pH effect on BNF (optimum 6.0-7.0)
    ph_factor_bnf = math.exp(-0.5 * ((soil_ph - 6.5) / 1.2) ** 2)

    # Perturbation schedule
    pert_by_day: dict[int, dict] = {int(p.get("day", 0)): p for p in perturbations}

    # Active perturbation state
    state: dict = {"drought_active": False, "drought_strength": 1.0,
                   "fertilizer_boost": 1.0, "fertilizer_decay": 1.0}

    def rhs(t, y):
        B_fix, B_nit, B_dec, DOC, NH4, NO3, PO4 = y
        B_fix = max(B_fix, 0.0)
        B_nit = max(B_nit, 0.0)
        B_dec = max(B_dec, 0.0)
        DOC   = max(DOC, 0.0)
        NH4   = max(NH4, 0.0)
        NO3   = max(NO3, 0.0)
        PO4   = max(PO4, 0.0)

        t_fac  = _temperature_factor(t, mean_temp)
        p_fac  = _precipitation_factor(t, precip_mm)

        # Check perturbations at this time
        day_int = int(t)
        if day_int in pert_by_day:
            p = pert_by_day[day_int]
            ptype = p.get("type", "")
            sev   = float(p.get("severity", 0.5))
            if ptype == "drought":
                state["drought_strength"] = 1.0 - sev * 0.8
            elif ptype == "rewetting":
                state["drought_strength"] = 1.0
            elif ptype == "fertilizer":
                state["fertilizer_boost"] = 1.0 + sev * 3.0
                state["fertilizer_decay"] = 0.95  # decay per day
        # Decay fertilizer boost
        state["fertilizer_boost"] = max(1.0, state["fertilizer_boost"] * state["fertilizer_decay"])

        p_eff = p_fac * state["drought_strength"]

        # --- N-fixer growth (Monod, N2 unlimited, suppressed by NH4) ---
        n2_factor   = 1.0  # atmospheric N2 is unlimited
        nh4_suppression = KS_NH4 / (KS_NH4 + NH4 + 1e-9)  # suppress when NH4 high
        mu_fix = (MU_MAX_FIXER * n2_factor * nh4_suppression *
                  ph_factor_bnf * t_fac * p_eff)
        bnf_flux = mu_fix * B_fix / Y_FIXER  # N-fixed rate (mol N per unit time per vol)

        # --- Nitrifier growth (NH4 → NO3, inhibited at high NH4) ---
        mu_nit = (MU_MAX_NITRIFIER
                  * (NH4 / (KS_NH4 + NH4))
                  * (KI_NH4 / (KI_NH4 + NH4))  # substrate inhibition
                  * t_fac * p_eff)
        mu_nit = max(0.0, mu_nit)

        # --- Decomposer growth (DOC as C-source, N from NH4 or NO3) ---
        n_availability = (NH4 + NO3 * state["fertilizer_boost"]) / (KS_NO3 + NH4 + NO3)
        mu_dec = (MU_MAX_DECOMP
                  * (DOC / (KS_DOC + DOC))
                  * n_availability
                  * t_fac * p_eff)

        # --- Mortality / decay (linear) ---
        mort_fix = 0.005 * B_fix
        mort_nit = 0.004 * B_nit
        mort_dec = 0.008 * B_dec

        # --- DOC additions: plant root exudates + OM mineralisation ---
        doc_input = 0.002 * organic_matter * p_eff * t_fac  # soil OM release
        doc_consumed = mu_dec * B_dec / Y_DECOMP
        doc_from_mort = 0.6 * (mort_fix + mort_nit + mort_dec)  # necromass recycled

        # --- NH4 budget ---
        nh4_input = bnf_flux + doc_input * 0.05 * state["fertilizer_boost"]  # small mineral pool
        nh4_nitrified = mu_nit * B_nit / Y_NITRIF
        nh4_assimilation = mu_dec * B_dec * 0.12  # 12% N demand of heterotrophs
        nh4_from_mort = 0.4 * (mort_fix + mort_nit + mort_dec) * 0.1

        # --- Derivs ---
        dB_fix = mu_fix * B_fix - mort_fix
        dB_nit = mu_nit * B_nit - mort_nit
        dB_dec = mu_dec * B_dec - mort_dec
        dDOC   = doc_input + doc_from_mort - doc_consumed
        dNH4   = nh4_input + nh4_from_mort - nh4_nitrified - nh4_assimilation
        dNO3   = nh4_nitrified - mu_dec * B_dec * 0.04  # some denitrification
        dPO4   = 0.0005 * organic_matter * t_fac - mu_dec * B_dec * 0.01

        return [dB_fix, dB_nit, dB_dec, dDOC, dNH4, dNO3, dPO4]

    return rhs, bnf_flux if False else None  # bnf_flux defined inside closure


def _run_community_sim(community_id: int, phylum_profile: dict, env: dict,
                       sim_days: int = 90) -> dict:
    """
    Run 90-day dFBA ODE simulation for one community.

    Returns: community_id, stability_score, mean_bnf_flux, target_flux,
             perturbation_responses, walltime_s
    """
    t0 = time.perf_counter()

    proto = phylum_profile.get("Proteobacteria", 0.20)
    nitro = phylum_profile.get("Nitrospirae", 0.012) + phylum_profile.get("Thaumarchaeota", 0.02)
    nitro = max(nitro, 0.005)
    decomp = 1.0 - proto - nitro

    guild_init = {
        "B_fix":  proto * 2.0,    # g/dm3 — scale by phylum abundance
        "B_nit":  nitro * 2.0,
        "B_dec":  decomp * 2.0,
        "DOC":    0.25 + env.get("organic_matter_pct", 2.0) * 0.05,
        "NH4":    0.05,
        "NO3":    0.08,
        "PO4":    0.02,
    }

    perturbations = [
        {"type": "drought",     "day": 30, "severity": 0.55},
        {"type": "fertilizer",  "day": 50, "severity": 0.40},
        {"type": "rewetting",   "day": 60, "severity": 0.80},
    ]

    y0 = [guild_init["B_fix"], guild_init["B_nit"], guild_init["B_dec"],
          guild_init["DOC"], guild_init["NH4"], guild_init["NO3"], guild_init["PO4"]]

    rhs, _ = _build_odes(guild_init, env, perturbations)

    try:
        sol = solve_ivp(
            rhs,
            t_span=(0.0, float(sim_days)),
            y0=y0,
            method="RK45",
            dense_output=False,
            max_step=0.25,      # 6-hour max step
            rtol=1e-4,
            atol=1e-7,
        )
        if not sol.success:
            raise RuntimeError(f"ODE solver failed: {sol.message}")

        B_fix_traj = sol.y[0]
        biomass_mean = float(np.mean(B_fix_traj))

        # Approximate BNF flux from trajectory (proportional to fixer biomass)
        ph_factor = math.exp(-0.5 * ((env.get("soil_ph", 6.5) - 6.5) / 1.2) ** 2)
        nh4_mean = float(np.mean(sol.y[4]))
        nh4_suppress = 0.5 / (0.5 + nh4_mean + 1e-9)
        bnf_trajectory = B_fix_traj * MU_MAX_FIXER * nh4_suppress * ph_factor / Y_FIXER
        target_flux = float(np.mean(bnf_trajectory))

        # Stability: 1 - CV of BNF flux
        if bnf_trajectory.mean() > 1e-12:
            cv = bnf_trajectory.std() / bnf_trajectory.mean()
            stability_score = float(max(0.0, 1.0 - min(cv, 1.0)))
        else:
            stability_score = 0.0

        # Perturbation responses (flux at days 30, 50, 60)
        t_arr = sol.t
        pert_responses = []
        for day in [30, 50, 60]:
            idx = np.searchsorted(t_arr, day)
            idx = min(idx, len(B_fix_traj) - 1)
            pert_responses.append({"day": day, "bnf_flux": float(bnf_trajectory[idx])})

        return {
            "community_id":   community_id,
            "stability_score": stability_score,
            "target_flux":    target_flux,
            "walltime_s":     time.perf_counter() - t0,
            "perturbation_responses": pert_responses,
            "t2_pass":        stability_score >= 0.5 and target_flux > 0.001,
            "error":          None,
        }

    except Exception as exc:
        return {
            "community_id":   community_id,
            "stability_score": 0.0,
            "target_flux":    0.0,
            "walltime_s":     time.perf_counter() - t0,
            "perturbation_responses": [],
            "t2_pass":        False,
            "error":          str(exc),
        }


def _worker_batch(batch: list[tuple]) -> list[dict]:
    """Process a batch of (community_id, phylum_profile_json, env_json, sim_days)."""
    results = []
    for community_id, profile_json, env_json, sim_days in batch:
        try:
            profile = json.loads(profile_json or "{}")
            env = json.loads(env_json or "{}")
        except Exception:
            profile, env = {}, {}
        results.append(_run_community_sim(community_id, profile, env, sim_days))
    return results


def _fetch_communities(db_path: str, min_bnf: float, n_max: int) -> list[tuple]:
    """Load communities from DB for dFBA. Returns list of (cid, profile, env, sim_days)."""
    conn = _db_connect(db_path)
    rows = conn.execute(
        """SELECT c.community_id, c.phylum_profile,
                  json_object(
                    'soil_ph',           COALESCE(s.soil_ph, 6.5),
                    'organic_matter_pct',COALESCE(s.organic_matter_pct, 2.0),
                    'clay_pct',          COALESCE(s.clay_pct, 25.0),
                    'temperature_c',     COALESCE(s.temperature_c, 12.0),
                    'precipitation_mm',  COALESCE(s.precipitation_mm, 600.0)
                  )
           FROM runs r
           JOIN communities c ON r.community_id = c.community_id
           JOIN samples s ON r.sample_id = s.sample_id
           WHERE r.t025_function_score >= ?
             AND c.phylum_profile IS NOT NULL
             AND r.t2_pass IS NULL
           ORDER BY r.t025_function_score DESC
           LIMIT ?""",
        (min_bnf, n_max)
    ).fetchall()
    conn.close()
    return [(r[0], r[1], r[2], 90) for r in rows]


def _write_results(db_path: str, results: list[dict]) -> int:
    """Write dFBA T2 results back to the DB. Returns number written."""
    conn = _db_connect(db_path, timeout=60)
    conn.execute("PRAGMA synchronous=OFF")  # write path
    n = 0
    for r in results:
        if r.get("error"):
            continue
        try:
            conn.execute(
                """UPDATE runs
                   SET t2_pass=?, t2_stability_score=?, t1_target_flux=?,
                       t2_walltime_s=?, t2_best_intervention=?, tier_reached=2
                   WHERE community_id=? AND t2_pass IS NULL""",
                (
                    1 if r["t2_pass"] else 0,
                    r["stability_score"],
                    r["target_flux"],
                    r["walltime_s"],
                    json.dumps(r["perturbation_responses"]),
                    r["community_id"],
                )
            )
            n += 1
        except Exception as exc:
            logger.debug("Write failed for cid=%s: %s", r.get("community_id"), exc)
    conn.commit()
    conn.execute("PRAGMA synchronous=NORMAL")  # restore after commit (can't change inside tx)
    conn.close()
    return n


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    db_path:      Path          = typer.Option(Path("/data/pipeline/db/soil_microbiome.db"), "--db"),
    min_bnf:      float         = typer.Option(0.60,  "--min-bnf"),
    n_communities:int           = typer.Option(10_000, "--n-communities", "-n"),
    workers:      int           = typer.Option(36,    "--workers", "-w"),
    batch_size:   int           = typer.Option(100,   "--batch-size"),
    sim_days:     int           = typer.Option(90,    "--sim-days"),
    log_path:     Optional[Path] = typer.Option(Path("/var/log/pipeline/dfba_batch.log"), "--log"),
):
    """Run batch dFBA ODE simulations on top-scoring BNF communities."""
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if log_path:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(str(log_path)))
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        handlers=handlers, force=True
    )

    logger.info("=== dFBA batch starting: min_bnf=%.2f, n=%d, workers=%d ===",
                min_bnf, n_communities, workers)

    logger.info("Loading communities from DB …")
    communities = _fetch_communities(str(db_path), min_bnf, n_communities)
    logger.info("Found %d communities to simulate", len(communities))

    if not communities:
        logger.warning("No communities found with t025_function_score >= %.2f and t2_pass IS NULL", min_bnf)
        logger.info("Hint: run synthetic_bootstrap.py first to generate communities")
        raise typer.Exit(0)

    # Chunk into batches
    def _chunks(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i : i + n]

    batches = list(_chunks(communities, batch_size))
    logger.info("Submitting %d batches × %d communities to %d workers",
                len(batches), batch_size, workers)

    t_start = time.time()
    n_written = 0
    n_passed = 0

    with ProcessPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_worker_batch, batch): i for i, batch in enumerate(batches)}
        for fut in as_completed(futures):
            batch_idx = futures[fut]
            try:
                batch_results = fut.result()
                n = _write_results(str(db_path), batch_results)
                n_written += n
                n_passed += sum(1 for r in batch_results if r.get("t2_pass") and not r.get("error"))
                elapsed = time.time() - t_start
                rate = n_written / elapsed if elapsed > 0 else 0
                logger.info(
                    "Batch %4d/%d done — %6d written, %5d T2-passed (%.1f/s, %.1f min elapsed)",
                    batch_idx + 1, len(batches), n_written, n_passed, rate, elapsed / 60
                )
            except Exception as exc:
                logger.error("Batch %d failed: %s", batch_idx, exc)

    elapsed = time.time() - t_start
    logger.info("=== dFBA batch complete: %d written, %d T2-passed in %.1f min ===",
                n_written, n_passed, elapsed / 60)


if __name__ == "__main__":
    app()
