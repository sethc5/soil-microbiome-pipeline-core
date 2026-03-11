from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from pathlib import Path

class AbstractIntent(ABC):
    """
    Interface for application-specific logic in the soil microbiome pipeline.
    An 'Intent' defines the biological parameters for a specific screening target.
    """

    @property
    @abstractmethod
    def target_id(self) -> str:
        """Unique identifier for this intent (e.g., 'bnf', 'carbon_seq')."""
        pass

    @abstractmethod
    def get_t0_filters(self) -> Dict[str, Any]:
        """Return application-specific T0 filtering parameters."""
        pass

    @abstractmethod
    def get_t1_constraints(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Return constraints for T1 FBA.
        Includes shared metabolite pools, medium definitions, and reaction bounds.
        """
        pass

    @abstractmethod
    def get_t1_target_reactions(self, model: Any) -> List[Any]:
        """Identify reactions representing the target functional outcome in a COBRA model."""
        pass

    @abstractmethod
    def get_t2_perturbations(self, metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Return a schedule of perturbations for dFBA simulation."""
        pass

    @abstractmethod
    def score_intervention(self, intervention: Dict[str, Any], results: Dict[str, Any]) -> float:
        """Assign a functional improvement score to a simulated intervention."""
        pass
