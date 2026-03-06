"""
compute/agent_based_sim.py — T2 optional agent-based community simulation.

Wraps iDynoMiCS 2 (individual-based microbial community simulator) for
spatially explicit modeling of community dynamics. Use when spatial
structure matters: biofilm formation, soil aggregate colonisation.

This is the optional T2 engine — dfba_runner.py is the default.
iDynoMiCS 2 is a Java-based tool; this module manages subprocess
invocation and XML protocol generation / output parsing.

Install iDynoMiCS: https://github.com/kreft/iDynoMiCS-2
  java -jar iDynoMiCS.jar protocol.xml

Usage:
  from compute.agent_based_sim import run_idynomics
  result = run_idynomics(community_model, metadata, simulation_days=30)
"""

from __future__ import annotations

import logging
import shutil
import subprocess
import tempfile
import time
import xml.etree.ElementTree as ET
from pathlib import Path

logger = logging.getLogger(__name__)

_JAR_CANDIDATES = ["iDynoMiCS.jar", "iDynomics.jar", "idynomics.jar"]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _find_jar(jar_override: str | None = None) -> Path | None:
    """Locate iDynoMiCS.jar: explicit override → PATH search → cwd."""
    if jar_override:
        p = Path(jar_override)
        if p.exists():
            return p
    for name in _JAR_CANDIDATES:
        found = shutil.which(name)
        if found:
            return Path(found)
        if Path(name).exists():
            return Path(name)
    return None


def _write_protocol_xml(
    community_model,
    metadata: dict,
    simulation_days: int,
    out_dir: Path,
) -> Path:
    """
    Write a minimal iDynoMiCS 2 protocol XML from the community model.

    Generates a simplified soil-aggregate geometry (100 µm cube).
    Species are derived from the community model's member IDs.
    """
    root = ET.Element("idynomics")

    # Simulator block
    sim = ET.SubElement(root, "simulator")
    ET.SubElement(sim, "param", attrib={"name": "restartPreviousRun"}).text = "false"
    ET.SubElement(sim, "param", attrib={"name": "randomSeed"}).text = "4242"
    ET.SubElement(sim, "param", attrib={"name": "outputPeriod", "unit": "day"}).text = "1"
    timer = ET.SubElement(sim, "timeStep")
    ET.SubElement(timer, "param", attrib={"name": "adaptive"}).text = "false"
    ET.SubElement(timer, "param", attrib={"name": "timeStepIni", "unit": "hour"}).text = "0.1"
    ET.SubElement(timer, "param", attrib={"name": "endOfSimulation", "unit": "day"}).text = str(simulation_days)

    # World / bulk soil compartment
    world = ET.SubElement(root, "world")
    bulk  = ET.SubElement(world, "bulk", attrib={"name": "soil_bulk"})
    soil_ph = metadata.get("soil_ph", 6.5)
    for sol, conc in [("o2", "2.5e-4"), ("nh4", "1e-4"), ("no3", "5e-5"), ("glucose", "1e-3")]:
        s = ET.SubElement(bulk, "solute", attrib={"name": sol})
        ET.SubElement(s, "param", attrib={"name": "concentration"}).text = conc

    # Soil aggregate compartment (100 µm cube)
    comp = ET.SubElement(world, "computationDomain", attrib={"name": "soil_aggregate"})
    ET.SubElement(comp, "param", attrib={"name": "domainDimension", "unit": "um"}).text = "100"
    ET.SubElement(comp, "param", attrib={"name": "geometry"}).text = "cube"

    # Derive member species from community model
    member_ids: list[str] = []
    if community_model is not None:
        members = getattr(community_model, "_member_models", None)
        if members:
            member_ids = [m.id for m in members]
        elif hasattr(community_model, "id"):
            member_ids = [community_model.id]
    if not member_ids:
        member_ids = ["organism_0"]

    for org_id in member_ids:
        sp = ET.SubElement(root, "species", attrib={"name": org_id, "class": "Bacterium"})
        ET.SubElement(sp, "param", attrib={"name": "initialPopulation"}).text = "10"
        ET.SubElement(sp, "param", attrib={"name": "growthRate"}).text = "0.5"

    proto_path = out_dir / "protocol.xml"
    tree = ET.ElementTree(root)
    ET.indent(tree, space="  ")
    tree.write(str(proto_path), encoding="unicode", xml_declaration=True)
    return proto_path


def _parse_idynomics_output(output_dir: Path) -> dict:
    """
    Parse iDynoMiCS agent_State/ XML logs.

    Stability score = (final agent count) / (initial agent count), capped at 1.
    Spatial community profile = {species: final count} dict.
    """
    state_dir = output_dir / "agent_State"
    if not state_dir.exists():
        return {"stability_score": 0.0, "spatial_community_profile": {}}

    timestep_files = sorted(state_dir.glob("*.xml"))
    if not timestep_files:
        return {"stability_score": 0.0, "spatial_community_profile": {}}

    def _count_agents(xml_path: Path) -> tuple[dict, int]:
        try:
            root = ET.parse(xml_path).getroot()
            counts: dict[str, int] = {}
            for agent in root.findall(".//agent"):
                sp = agent.get("species", "unknown")
                counts[sp] = counts.get(sp, 0) + 1
            return counts, sum(counts.values())
        except ET.ParseError:
            return {}, 0

    _, initial_total = _count_agents(timestep_files[0])
    final_counts, final_total = _count_agents(timestep_files[-1])

    stability = min(1.0, final_total / max(1, initial_total))
    return {
        "stability_score":           round(stability, 4),
        "spatial_community_profile": final_counts,
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_idynomics(
    community_model,
    metadata: dict,
    simulation_days: int = 30,
    idynomics_jar: str | None = None,
    java_exe: str = "java",
    java_args: list[str] | None = None,
) -> dict:
    """
    Run iDynoMiCS 2 agent-based simulation (optional T2 engine).

    Falls back gracefully (stability_score=0.0, engine="fallback") when
    Java or iDynoMiCS.jar is unavailable, so the pipeline never hard-fails.

    Parameters
    ----------
    community_model : cobra.Model | None
        Merged community metabolic model (species extracted from _member_models).
    metadata : dict
        Sample metadata (soil_ph, temperature_c, etc.) — used in protocol XML.
    simulation_days : int
        Duration of the simulation in days.
    idynomics_jar : str | None
        Explicit path to iDynoMiCS.jar. If None, searches PATH and cwd.
    java_exe : str
        Java executable. Override to a full path if needed.
    java_args : list[str] | None
        Extra JVM flags, e.g. ["-Xmx4g"]. Default: ["-Xmx2g"].

    Returns
    -------
    dict with keys:
        stability_score           float       0–1
        spatial_community_profile dict        {species_id: agent_count}
        walltime_s                float
        engine                    str         "idynomics" | "fallback"
        note                      str | None  reason string when fallback used
    """
    t0 = time.perf_counter()

    def _fallback(note: str) -> dict:
        return {
            "stability_score": 0.0,
            "spatial_community_profile": {},
            "walltime_s": time.perf_counter() - t0,
            "engine": "fallback",
            "note": note,
        }

    # Check Java
    if not shutil.which(java_exe):
        logger.warning(
            "Java not found ('%s') — iDynoMiCS unavailable. Install JDK 11+.", java_exe
        )
        return _fallback(f"Java executable '{java_exe}' not found. Install JDK 11+.")

    # Locate JAR
    jar_path = _find_jar(idynomics_jar)
    if jar_path is None:
        logger.warning(
            "iDynoMiCS.jar not found. "
            "Download from https://github.com/kreft/iDynoMiCS-2 and place in PATH."
        )
        return _fallback(
            "iDynoMiCS.jar not found. See https://github.com/kreft/iDynoMiCS-2"
        )

    # Build protocol + run
    with tempfile.TemporaryDirectory(prefix="idynomics_") as tmp_str:
        tmp = Path(tmp_str)
        proto_path = _write_protocol_xml(community_model, metadata, simulation_days, tmp)

        jvm_args = java_args or ["-Xmx2g"]
        cmd = [java_exe] + jvm_args + ["-jar", str(jar_path), str(proto_path)]

        logger.info(
            "Running iDynoMiCS: %s  simulation_days=%d  species=%d",
            jar_path.name,
            simulation_days,
            len(getattr(community_model, "_member_models", []) or [community_model]),
        )

        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=simulation_days * 60 * 3,  # 3 min per simulated day
                cwd=str(tmp),
            )
        except subprocess.TimeoutExpired:
            logger.error("iDynoMiCS timed out after %d simulated days", simulation_days)
            return _fallback("Simulation timed out.")

        if proc.returncode != 0:
            logger.error(
                "iDynoMiCS failed (rc=%d):\n%s", proc.returncode, proc.stderr[:500]
            )
            return _fallback(f"iDynoMiCS exited with code {proc.returncode}.")

        parsed = _parse_idynomics_output(tmp)
        return {
            **parsed,
            "walltime_s": time.perf_counter() - t0,
            "engine": "idynomics",
            "note": None,
        }
