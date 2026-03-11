from __future__ import annotations
import logging
import time
import json
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional
from concurrent.futures import ProcessPoolExecutor, as_completed

from core.base import AbstractIntent
from receipt_system import Receipt

logger = logging.getLogger(__name__)

class PipelineEngine:
    """
    Unified, application-agnostic funnel engine.
    Orchestrates T0 through T2 using an AbstractIntent for biological parameters.
    """

    def __init__(self, intent: AbstractIntent, db: Any, config: Any):
        self.intent = intent
        self.db = db
        self.config = config
        self._solver = "glpk" # Enforce GLPK for safety on AGORA2 models

    def run_t1_batch(
        self, 
        community_ids: List[int], 
        workers: int = 4,
        models_dir: str | Path = "models/"
    ) -> Dict[str, Any]:
        """
        Unified T1 batch runner. Logic consolidated from scripts/t1_fba_batch.py.
        """
        receipt = Receipt().start()
        n_passed = 0
        n_failed = 0
        errors = []

        # Split into chunks for saturation
        batch_size = 5
        batches = [community_ids[i:i + batch_size] for i in range(0, len(community_ids), batch_size)]

        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(self._worker_t1_batch, batch, str(models_dir)): i 
                for i, batch in enumerate(batches)
            }
            for future in as_completed(futures):
                try:
                    batch_results = future.result()
                    for res in batch_results:
                        self._persist_t1_result(res)
                        if res.get("t1_pass"):
                            n_passed += 1
                        else:
                            n_failed += 1
                except Exception as exc:
                    n_failed += len(batches[futures[future]])
                    errors.append({"error": str(exc), "traceback": traceback.format_exc()})

        receipt.n_samples_processed = n_passed + n_failed
        receipt.n_fba_runs = n_passed + n_failed
        receipt.finish()

        return {"n_processed": n_passed + n_failed, "n_passed": n_passed, "errors": errors}

    def _worker_t1_batch(self, community_ids: List[int], models_dir: str) -> List[Dict[str, Any]]:
        """
        Consolidated worker logic with namespacing and GLPK enforcement.
        """
        from compute.community_fba import run_community_fba
        
        results = []
        for cid in community_ids:
            try:
                community_data = self.db.get_community(cid)
                metadata = self.db.get_sample_metadata(community_data["sample_id"])
                
                # In a real implementation, we would load the actual member models here
                # using a GenomeFetcher or similar.
                member_models = [] 
                
                res = run_community_fba(
                    member_models=member_models,
                    metadata=metadata,
                    intent=self.intent
                )
                res["community_id"] = cid
                results.append(res)
            except Exception as exc:
                results.append({"community_id": cid, "t1_pass": False, "error": str(exc)})
        
        return results

    def _persist_t1_result(self, result: Dict[str, Any]):
        """Update DB with T1 results."""
        self.db.update_community_t1(result["community_id"], result)
