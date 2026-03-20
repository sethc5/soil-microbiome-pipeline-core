from __future__ import annotations

import io
import json
import sys
from contextlib import redirect_stdout
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sim_model.simulate import main, run_simulation, run_simulation_for_scenario


def test_run_simulation_with_overrides_changes_flux():
    baseline = run_simulation_for_scenario("easy_win")
    shifted = run_simulation(
        scenario_name="easy_win",
        community_overrides={"diazotrophs": 0.18, "decomposers": None, "competitors": None, "stress_tolerant_taxa": None},
        environment_overrides={"soil_ph": 4.2, "organic_matter_pct": None, "moisture": None, "temperature_c": None},
        intervention_overrides={"inoculation_strength": None, "amendment_strength": None, "management_shift": None},
        note_override="custom test run",
    )

    assert shifted["target_flux"] < baseline["target_flux"]
    assert shifted["note"] == "custom test run"
    assert "diazotrophs" in shifted["applied_overrides"]
    assert "soil_ph" in shifted["applied_overrides"]


def test_main_json_with_cli_overrides():
    buffer = io.StringIO()
    with redirect_stdout(buffer):
        code = main(
            [
                "--scenario",
                "easy_win",
                "--diazotrophs",
                "0.10",
                "--soil-ph",
                "4.5",
                "--management-shift",
                "-0.5",
                "--json",
            ]
        )

    assert code == 0
    output = json.loads(buffer.getvalue())
    assert output["scenario"] == "easy_win"
    assert output["inputs"]["community"]["diazotrophs"] == 0.10
    assert output["inputs"]["environment"]["soil_ph"] == 4.5
    assert output["inputs"]["intervention"]["management_shift"] == -0.5
    assert sorted(output["applied_overrides"]) == ["diazotrophs", "management_shift", "soil_ph"]


def test_no_overrides_reports_empty_list():
    output = run_simulation_for_scenario("acidic_stress")
    assert output["applied_overrides"] == []
