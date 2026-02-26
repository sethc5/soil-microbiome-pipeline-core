"""
compute/amendment_effect_model.py — T2 biochar/compost/amendment effect modeling.

Translates physical amendment properties to soil parameter changes
that are used to adjust environmental constraints in T1/T2 FBA:

  Biochar:
    - pH increase:  +0.5 to +1.5 pH units (feedstock and temperature dependent)
    - Moisture:     +5-15% water holding capacity
    - Bulk density: -0.1 to -0.3 g/cm³
    - CEC:          +2-5 cmol/kg

  Compost:
    - Organic matter: +0.5-2% per t/ha
    - Available N:    +10-30 ppm per t/ha
    - pH:             small buffering effect

Use conservative estimates — amendment effects are heterogeneous (see README gotchas).

Usage:
  from compute.amendment_effect_model import compute_amendment_effect
  new_metadata = compute_amendment_effect(metadata, amendment_type="biochar", rate_t_ha=2)
"""

from __future__ import annotations
import logging

logger = logging.getLogger(__name__)

AMENDMENT_DEFAULTS = {
    "biochar": {
        "ph_delta_low": 0.5,
        "ph_delta_high": 1.5,
        "ph_delta_conservative": 0.6,
        "moisture_delta_pct": 10.0,
        "bulk_density_delta": -0.15,
        "cec_delta": 3.0,
    },
    "compost": {
        "organic_matter_delta_per_t_ha": 0.8,
        "available_n_delta_ppm_per_t_ha": 15.0,
        "ph_buffer_delta": 0.1,
    },
}


def compute_amendment_effect(
    metadata: dict,
    amendment_type: str,
    rate_t_ha: float,
    use_conservative: bool = True,
) -> dict:
    """
    Return updated metadata dict with amendment effects applied.

    Uses conservative estimates by default — see README gotchas.
    """
    raise NotImplementedError
