from __future__ import annotations
from typing import Any, Dict, List, Optional
from core.base import AbstractIntent

class BNFIntent(AbstractIntent):
    """
    Biological Nitrogen Fixation (BNF) implementation.
    """

    @property
    def target_id(self) -> str:
        return "nitrogen_fixation"

    # Mo-nitrogenase stoichiometry (Burgess & Lowe 1996)
    NITROGENASE_STOICH = {
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

    # Whitelist for inorganic exchanges that should remain open in minimal medium
    INORGANIC_EXCHANGES = {
        "EX_h2o_e", "EX_h_e", "EX_co2_e", "EX_hco3_e", "EX_o2_e", 
        "EX_pi_e", "EX_ppi_e", "EX_so4_e", "EX_h2s_e", "EX_fe2_e", 
        "EX_fe3_e", "EX_mg2_e", "EX_k_e", "EX_na1_e", "EX_ca2_e", 
        "EX_zn2_e", "EX_mn2_e", "EX_cu2_e", "EX_mobd_e", "EX_cobalt2_e", 
        "EX_cl_e", "EX_sel_e", "EX_ni2_e", "EX_n2_e"
    }

    def get_t0_filters(self) -> Dict[str, Any]:
        return {
            "required_functional_genes": ["nifH"],
            "ph_range": [5.5, 7.5]
        }

    def get_t1_constraints(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Logic extracted from compute/community_fba.py _apply_bnf_minimal_medium.
        """
        return {
            "medium_type": "N-limited-minimal",
            "carbon_uptake_bound": -10.0,
            "cofactor_uptake_bound": -0.001,
            "inorganic_whitelist": list(self.INORGANIC_EXCHANGES),
            "preferred_carbon_sources": [
                "EX_glc__D_e", "EX_sucr_e", "EX_fru_e", "EX_ac_e"
            ]
        }

    def get_t1_target_reactions(self, model: Any) -> List[Any]:
        """
        Identify Mo-nitrogenase reactions in the model.
        """
        return [
            r for r in model.reactions 
            if r.id == "NITROGENASE_MO" or r.id.startswith("NITROGENASE_MO__org")
        ]

    def get_t2_perturbations(self, metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
        return [
            {"type": "drought", "day": 15, "severity": 0.5},
            {"type": "fertilizer_pulse", "day": 20, "severity": 0.4}
        ]

    def score_intervention(self, intervention: Dict[str, Any], results: Dict[str, Any]) -> float:
        # Simplified scoring logic: effect on stability * confidence
        return intervention.get("predicted_effect", 0.0) * intervention.get("confidence", 1.0)
