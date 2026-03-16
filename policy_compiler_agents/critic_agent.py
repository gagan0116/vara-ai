# policy_compiler_agents/critic_agent.py
"""
Critic Agent - Validates schema and extraction quality before graph construction.

This agent performs quality checks on the proposed schema and extracted Cypher
statements to ensure they are correct and complete before building the graph.

Amazon Nova Features Used:
- JSON mode via prompt enforcement
- Deep reasoning for thorough validation
"""

import json
import os
import re
import asyncio
import sys
from typing import Any, Dict, List

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from bedrock_client import (
    NOVA_PRO,
    async_generate_with_retry,
    extract_text_from_response,
    _make_user_message,
)

from .tools import load_artifact, save_artifact

# Retry settings for transient API errors
MAX_RETRIES = 3
BASE_DELAY = 5.0


# =============================================================================
# JSON SCHEMA DEFINITIONS (replaces google.genai.types.Schema objects)
# =============================================================================

VALIDATION_RESPONSE_JSON_SCHEMA = {
    "type": "object",
    "properties": {
        "validation_status": {"type": "string", "description": "approved or needs_revision"},
        "schema_issues": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "issue": {"type": "string"},
                    "severity": {"type": "string", "description": "error or warning"},
                    "fix": {"type": "string"},
                },
                "required": ["issue", "severity"]
            }
        },
        "cypher_issues": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "issue": {"type": "string"},
                    "statement_index": {"type": "integer"},
                    "severity": {"type": "string"},
                    "fix": {"type": "string"},
                },
                "required": ["issue", "severity"]
            }
        },
        "coverage_issues": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "missing": {"type": "string"},
                    "recommendation": {"type": "string"},
                },
                "required": ["missing", "recommendation"]
            }
        },
        "summary": {"type": "string"},
        "confidence_score": {"type": "number", "description": "0.0-1.0"},
    },
    "required": ["validation_status", "summary", "confidence_score"]
}

CRITIC_SYSTEM_PROMPT = """You are a Quality Assurance Specialist for knowledge graph construction.
Your task is to validate schema designs and Cypher statements for correctness and completeness.

VALIDATION CRITERIA:

1. SCHEMA VALIDATION:
   - All node types have source_citation property
   - Relationships connect valid node types
   - Property types are appropriate
   - No missing essential entities

2. CYPHER VALIDATION:
   - All statements are syntactically correct
   - MERGE statements use appropriate unique identifiers
   - Relationships reference existing node patterns
   - No SQL-like errors (=, ==, wrong operators)

3. COVERAGE VALIDATION:
   - Key policy sections are represented
   - Membership tiers and overrides are captured
   - Return windows are extracted correctly
   - Restocking fees are included
   - Non-returnable items are modeled

4. SOURCE CITATION CHECK:
   - Every node has source_citation
   - Citations reference real sections

OUTPUT FORMAT:
{
  "validation_status": "approved" | "needs_revision",
  "schema_issues": [{"issue": "description", "severity": "error|warning", "fix": "suggested fix"}],
  "cypher_issues": [{"issue": "description", "statement_index": number, "severity": "error|warning", "fix": "suggested fix"}],
  "coverage_issues": [{"missing": "what's missing", "recommendation": "how to fix"}],
  "summary": "Overall assessment",
  "confidence_score": 0.0-1.0
}

Be thorough but practical. Minor warnings should not block approval.
You MUST respond with valid JSON only. No additional text."""


async def validate_artifacts(
    schema: Dict[str, Any] = None,
    extraction: Dict[str, Any] = None,
    model: str = None
) -> Dict[str, Any]:
    """
    Validate the schema and extraction artifacts using Amazon Nova.
    """
    # Load artifacts if not provided
    if schema is None:
        try:
            schema = load_artifact("proposed_schema")
        except FileNotFoundError:
            return {
                "validation_status": "needs_revision",
                "schema_issues": [{"issue": "Schema not found", "severity": "error"}],
                "cypher_issues": [],
                "coverage_issues": [],
            }
    
    if extraction is None:
        try:
            extraction = load_artifact("extracted_cypher")
        except FileNotFoundError:
            return {
                "validation_status": "needs_revision",
                "schema_issues": [],
                "cypher_issues": [{"issue": "Extraction not found", "severity": "error"}],
                "coverage_issues": [],
            }
    
    model = model or os.getenv("CRITIC_MODEL", NOVA_PRO)
    
    # Perform local validation checks first
    local_issues = perform_local_validation(schema, extraction)
    
    # If critical local issues, return early
    critical_issues = [i for i in local_issues if i.get("severity") == "error"]
    if len(critical_issues) > 3:
        return {
            "validation_status": "needs_revision",
            "schema_issues": [i for i in local_issues if "schema" in i.get("type", "")],
            "cypher_issues": [i for i in local_issues if "cypher" in i.get("type", "")],
            "coverage_issues": [],
            "summary": "Multiple critical issues found in local validation",
            "confidence_score": 0.3,
        }
    
    prompt = f"""Validate this Neo4j schema and Cypher extraction for a retail return policy knowledge graph.

SCHEMA:
{json.dumps(schema, indent=2)}

CYPHER STATEMENTS (first 50):
{json.dumps(extraction.get("cypher_statements", [])[:50], indent=2)}

EXTRACTION SUMMARY:
{json.dumps(extraction.get("extraction_summary", {}), indent=2)}

Perform comprehensive validation and provide your assessment.
Respond with valid JSON matching this schema:
{json.dumps(VALIDATION_RESPONSE_JSON_SCHEMA, indent=2)}"""

    print("[CRITIC] Using Amazon Nova for thorough validation...")
    
    messages = [_make_user_message(prompt)]
    
    response = await async_generate_with_retry(
        model_id=model,
        messages=messages,
        system_prompt=CRITIC_SYSTEM_PROMPT,
        max_tokens=4096,
        temperature=1.0,
        max_retries=MAX_RETRIES,
    )
    
    response_text = extract_text_from_response(response)
    
    try:
        validation = json.loads(response_text)
    except json.JSONDecodeError:
        json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
        if json_match:
            validation = json.loads(json_match.group())
        else:
            validation = {
                "validation_status": "needs_revision",
                "summary": "Could not parse validation response",
                "confidence_score": 0.0,
            }
    
    # Merge local issues
    if local_issues:
        validation["local_validation_issues"] = local_issues
    
    # Save artifact
    artifact_path = save_artifact("critic_report", validation)
    validation["_artifact_path"] = artifact_path
    
    return validation


def perform_local_validation(
    schema: Dict[str, Any],
    extraction: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """Perform local validation checks without LLM."""
    issues = []
    
    for node in schema.get("nodes", []):
        props = [p["name"] for p in node.get("properties", [])]
        if "source_citation" not in props:
            issues.append({
                "type": "schema",
                "issue": f"Node '{node['label']}' missing source_citation",
                "severity": "error",
            })
    
    linker_warnings = extraction.get("extraction_summary", {}).get("linker_warnings", 0)
    if linker_warnings > 10:
        issues.append({
            "type": "extraction",
            "issue": f"Too many orphaned relationships: {linker_warnings} relationships could not be connected",
            "severity": "error",
        })
    elif linker_warnings > 0:
        issues.append({
            "type": "extraction",
            "issue": f"{linker_warnings} relationships could not be connected (minor)",
            "severity": "warning",
        })
    
    cypher_statements = extraction.get("cypher_statements", [])
    for i, stmt in enumerate(cypher_statements):
        if not isinstance(stmt, str):
            issues.append({
                "type": "cypher",
                "issue": f"Statement {i} is not a string",
                "severity": "error",
                "statement_index": i,
            })
            continue
        
        if "==" in stmt:
            issues.append({
                "type": "cypher",
                "issue": f"Statement {i} uses '==' instead of '='",
                "severity": "error",
                "statement_index": i,
            })
        
        if "source_citation" not in stmt.lower() and "MERGE" in stmt.upper():
            if re.match(r'MERGE\s*\(', stmt, re.IGNORECASE):
                issues.append({
                    "type": "cypher",
                    "issue": f"Statement {i} might be missing source_citation",
                    "severity": "warning",
                    "statement_index": i,
                })
    
    return issues


async def run_critic_agent(
    schema: Dict[str, Any] = None,
    extraction: Dict[str, Any] = None,
    log_callback: callable = None
) -> Dict[str, Any]:
    """Main entry point for the Critic agent."""
    log = log_callback or (lambda msg: print(msg))
    
    log("[CRITIC] Validating schema and extraction...")
    
    try:
        validation = await validate_artifacts(schema=schema, extraction=extraction)
        
        status = validation.get("validation_status", "unknown")
        confidence = validation.get("confidence_score", 0)
        
        if status == "approved":
            log(f"[CRITIC] ✓ Validation APPROVED (confidence: {confidence:.1%})")
        else:
            issues_count = (
                len(validation.get("schema_issues", [])) +
                len(validation.get("cypher_issues", []))
            )
            log(f"[CRITIC] Validation needs revision (confidence: {confidence:.1%})")
            log(f"[CRITIC] Issues found: {issues_count}")
        
        return {
            "status": "success",
            "validation": validation,
            "approved": status == "approved",
        }
    except Exception as e:
        log(f"[CRITIC] Validation failed: {e}")
        return {
            "status": "error",
            "error": str(e),
            "approved": False,
        }


if __name__ == "__main__":
    import asyncio
    result = asyncio.run(run_critic_agent())
    print(json.dumps(result, indent=2, default=str))
