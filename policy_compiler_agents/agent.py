# policy_compiler_agents/agent.py
"""
Policy Compiler - Main Orchestrator

This is the main entry point for the multi-agent policy compilation pipeline.
It orchestrates the sequential execution of:
1. Ontology Designer Agent
2. Extraction Agent  
3. Critic Agent
4. Graph Builder Agent

Built using Amazon Nova models with shared session state.
"""

import asyncio
import json
from typing import Any, Dict, Optional

from .ontology_agent import run_ontology_agent, design_ontology
from .extraction_agent import run_extraction_agent, extract_policy_rules
from .critic_agent import run_critic_agent, validate_artifacts
from .builder_agent import run_builder_agent, build_graph
from .tools import save_artifact, read_policy_markdown


# Type for log callback
LogCallback = Optional[callable]


class PolicyCompilerPipeline:
    """
    Sequential pipeline orchestrator for policy compilation.
    
    Implements agent coordination with shared state using Amazon Nova.
    """
    
    def __init__(self, max_revision_attempts: int = 2, log_callback: LogCallback = None):
        """
        Initialize the pipeline.
        
        Args:
            max_revision_attempts: Maximum times to retry after critic rejection
            log_callback: Optional callback function for progress logging (receives message string)
        """
        self.max_revision_attempts = max_revision_attempts
        self.state: Dict[str, Any] = {}
        self._log = log_callback or (lambda msg: print(msg))
    
    async def run(self, clear_existing_graph: bool = True) -> Dict[str, Any]:
        """
        Execute the full policy compilation pipeline.
        
        Args:
            clear_existing_graph: Whether to clear Neo4j before building
            
        Returns:
            Pipeline execution results
        """
        self._log("="*60)
        self._log("[PIPELINE] POLICY COMPILER PIPELINE - Starting")
        self._log("="*60)
        
        results = {
            "pipeline_status": "running",
            "stages": {},
        }
        
        try:
            # Stage 1: Ontology Design
            self._log("[STAGE 1/4] Ontology Design - Analyzing policy structure...")
            self._log("Using Nova Thinking Mode for schema generation...")
            ontology_result = await run_ontology_agent(log_callback=self._log)
            results["stages"]["ontology"] = ontology_result
            
            if ontology_result["status"] != "success":
                return self._fail_pipeline(results, "Ontology design failed")
            
            node_count = len(ontology_result.get("schema", {}).get("nodes", []))
            rel_count = len(ontology_result.get("schema", {}).get("relationships", []))
            self._log(f"[ONTOLOGY] Designed schema: {node_count} node types, {rel_count} relationships")
            self.state["schema"] = ontology_result["schema"]
            
            # Stage 2: Extraction
            self._log("[STAGE 2/4] Policy Extraction - Extracting entities & relationships...")
            self._log("Processing pages in parallel batches...")
            extraction_result = await run_extraction_agent(schema=self.state["schema"], log_callback=self._log)
            results["stages"]["extraction"] = extraction_result
            
            if extraction_result["status"] != "success":
                return self._fail_pipeline(results, "Extraction failed")
            
            stmt_count = len(extraction_result.get("extraction", {}).get("cypher_statements", []))
            self._log(f"[EXTRACTION] Generated {stmt_count} Cypher statements")
            self.state["extraction"] = extraction_result["extraction"]
            
            # Stage 3: Critic Validation (with retry loop)
            self._log("[STAGE 3/4] Validation - Checking schema & extraction quality...")
            
            approved = False
            for attempt in range(self.max_revision_attempts + 1):
                self._log(f"[CRITIC] Validation attempt {attempt + 1}/{self.max_revision_attempts + 1}...")
                critic_result = await run_critic_agent(
                    schema=self.state["schema"],
                    extraction=self.state["extraction"],
                    log_callback=self._log
                )
                results["stages"][f"validation_attempt_{attempt + 1}"] = critic_result
                
                if critic_result.get("approved", False):
                    approved = True
                    self._log(f"[CRITIC] ✓ Approved on attempt {attempt + 1}")
                    break
                else:
                    if attempt < self.max_revision_attempts:
                        self._log(f"[CRITIC] Revision needed, re-running extraction...")
                        # Re-run extraction with critic feedback
                        extraction_result = await run_extraction_agent(
                            schema=self.state["schema"],
                            log_callback=self._log
                        )
                        if extraction_result["status"] == "success":
                            self.state["extraction"] = extraction_result["extraction"]
            
            if not approved:
                self._log("[CRITIC] ⚠ Proceeding despite validation issues (max attempts reached)")
            
            self.state["validation"] = critic_result.get("validation", {})
            
            # Stage 4: Graph Building
            self._log("[STAGE 4/4] Graph Construction - Building Neo4j knowledge graph...")
            self._log("Connecting to Neo4j and executing Cypher statements...")
            builder_result = await run_builder_agent(
                extraction=self.state["extraction"],
                clear_existing=clear_existing_graph,
                log_callback=self._log
            )
            results["stages"]["builder"] = builder_result
            
            if builder_result["status"] != "success":
                return self._fail_pipeline(results, "Graph building failed")
            
            # Success!
            results["pipeline_status"] = "success"
            results["final_state"] = {
                "schema_nodes": len(self.state["schema"].get("nodes", [])),
                "schema_relationships": len(self.state["schema"].get("relationships", [])),
                "cypher_statements": len(self.state["extraction"].get("cypher_statements", [])),
                "graph_nodes": builder_result.get("build_result", {}).get("verification", {}).get("total_nodes", 0),
            }
            
            self._log("="*60)
            self._log("[PIPELINE] ✓ COMPLETE - Knowledge graph built successfully!")
            self._log("="*60)
            self._log(f"Schema: {results['final_state']['schema_nodes']} node types")
            self._log(f"Cypher: {results['final_state']['cypher_statements']} statements")
            self._log(f"Graph: {results['final_state']['graph_nodes']} nodes created")
            
            # Save final results
            save_artifact("pipeline_results", results)
            
            return results
            
        except Exception as e:
            return self._fail_pipeline(results, str(e))
    
    def _fail_pipeline(self, results: Dict, error: str) -> Dict[str, Any]:
        """Mark pipeline as failed."""
        results["pipeline_status"] = "failed"
        results["error"] = error
        self._log(f"[PIPELINE] ✗ FAILED: {error}")
        save_artifact("pipeline_results", results)
        return results


async def run_pipeline(clear_existing: bool = True) -> Dict[str, Any]:
    """
    Convenience function to run the full pipeline.
    
    Args:
        clear_existing: Whether to clear existing graph
        
    Returns:
        Pipeline results
    """
    pipeline = PolicyCompilerPipeline()
    return await pipeline.run(clear_existing_graph=clear_existing)


# CLI interface
def main():
    import argparse
    import os
    
    parser = argparse.ArgumentParser(description="Policy Compiler Pipeline")
    parser.add_argument("--run-pipeline", action="store_true", help="Run the full policy compilation pipeline")
    
    args = parser.parse_args()
    
    # Check .env first
    if not os.path.exists(".env"):
        print("[ERROR] .env file not found. Please create one with AWS credentials and NEO4J credentials.")
        return

    if args.run_pipeline:
        # Run async pipeline
        asyncio.run(run_pipeline())
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
