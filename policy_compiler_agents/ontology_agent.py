# policy_compiler_agents/ontology_agent.py
"""
Ontology Designer Agent - Analyzes policy markdown and proposes a graph schema.

This agent reads the parsed policy document and generates a Neo4j schema
with node labels, properties, and relationship types.

Amazon Nova Features Used:
- JSON mode via system prompt enforcement
- Deep reasoning for comprehensive schema identification
"""

import os
import json
import asyncio
import sys
from typing import Any, Dict

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from bedrock_client import (
    NOVA_PRO,
    async_generate_with_retry,
    extract_text_from_response,
    _make_user_message,
)

from .tools import read_policy_markdown, save_artifact

# Retry settings for transient API errors
MAX_RETRIES = 3
BASE_DELAY = 5.0


# JSON Schema definition (replaces google.genai.types.Schema objects)
ONTOLOGY_RESPONSE_JSON_SCHEMA = {
    "type": "object",
    "properties": {
        "nodes": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "label": {"type": "string", "description": "Node label in PascalCase"},
                    "description": {"type": "string", "description": "Description of what this node represents"},
                    "properties": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string", "description": "Property name"},
                                "type": {"type": "string", "description": "Property type: string, integer, float, boolean"},
                                "required": {"type": "boolean", "description": "Whether property is required"},
                                "description": {"type": "string", "description": "Description of the property"},
                            },
                            "required": ["name", "type"]
                        },
                        "description": "List of node properties"
                    },
                    "constraints": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Constraints like UNIQUE(name)"
                    },
                },
                "required": ["label", "description", "properties"]
            },
            "description": "List of node types"
        },
        "relationships": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "type": {"type": "string", "description": "Relationship type in UPPER_SNAKE_CASE"},
                    "from_label": {"type": "string", "description": "Source node label"},
                    "to_label": {"type": "string", "description": "Target node label"},
                    "description": {"type": "string", "description": "Description of the relationship"},
                    "cardinality": {"type": "string", "description": "Cardinality like 1:N, N:M"},
                },
                "required": ["type", "from_label", "to_label", "description"]
            },
            "description": "List of relationship types"
        },
        "design_rationale": {"type": "string", "description": "Explanation of schema design decisions"},
    },
    "required": ["nodes", "relationships", "design_rationale"]
}


# =============================================================================
# SYSTEM PROMPT
# =============================================================================

ONTOLOGY_SYSTEM_PROMPT = """You are a Neo4j Schema Designer for retail policy documents.

CRITICAL RULES:
1. Every node MUST have a 'name' property (string, required) in addition to 'source_citation'. This ensures every entity is identifiable.
2. Use PascalCase for node labels (e.g., ReturnWindow), UPPER_SNAKE_CASE for relationships (e.g., HAS_RETURN_WINDOW).
3. Model conditional logic with explicit condition nodes linked via REQUIRES or EXCLUDES relationships.
4. Include constraint types where appropriate (UNIQUE, NOT NULL).
5. The 'from_label' and 'to_label' in relationships MUST EXACTLY MATCH a 'label' defined in the 'nodes' array. No spelling variations or plurals.
6. Be EXHAUSTIVE - identify ALL node types and relationships mentioned in the policy. Missing entities is worse than having too many.

DO NOT CREATE nodes for generic concepts: Policy, Document, Company, Website, Customer, Section, Page.

DOMAIN EXAMPLE:
{
  "nodes": [
    {
      "label": "ProductCategory",
      "description": "A category of products with specific return rules",
      "properties": [
        {"name": "name", "type": "string", "required": true},
        {"name": "source_citation", "type": "string", "required": true}
      ],
      "constraints": ["UNIQUE(name)"]
    },
    {
      "label": "ReturnWindow",
      "description": "Time period allowed for returns",
      "properties": [
        {"name": "name", "type": "string", "required": true},
        {"name": "days_allowed", "type": "integer", "required": true},
        {"name": "source_citation", "type": "string", "required": true}
      ]
    }
  ],
  "relationships": [
    {
      "type": "HAS_RETURN_WINDOW",
      "from_label": "ProductCategory",
      "to_label": "ReturnWindow",
      "description": "Links a category to its applicable return window",
      "cardinality": "1:N"
    }
  ],
  "design_rationale": "Categories link to return windows; membership tiers can extend via APPLIES_TO_MEMBERSHIP relationship."
}

Think carefully through the entire document to identify:
- All product categories with different return rules
- All membership tiers and their special privileges  
- All conditions (opened, sealed, defective, etc.)
- All exceptions and special cases
- All time-based rules (return windows, extended periods)
- All fees (restocking fees, shipping fees)

OUTPUT FORMAT: Valid JSON matching the schema structure. Output JSON only, no additional text."""


# =============================================================================
# MAIN FUNCTION
# =============================================================================

async def design_ontology(
    policy_content: str = None,
    model: str = None
) -> Dict[str, Any]:
    """
    Analyze policy markdown and generate a graph schema using Amazon Nova
    for comprehensive node/relationship identification.
    
    Args:
        policy_content: Optional policy markdown. If None, reads from file.
        model: Model ID to use. Defaults to NOVA_PRO.
        
    Returns:
        Schema definition with nodes and relationships
    """
    if policy_content is None:
        policy_content = read_policy_markdown()
    
    # Use environment variable override if set
    model = model or os.getenv("ONTOLOGY_MODEL", NOVA_PRO)
    
    # Streamlined user prompt
    prompt = f"""Analyze this retail return policy document and design a comprehensive Neo4j knowledge graph schema.

POLICY DOCUMENT:
{policy_content}

Focus on capturing: product categories, return rules with time windows, membership tier overrides, 
restocking fees, non-returnable items, and special conditions (opened, defective, etc.).

IMPORTANT: Be exhaustive in identifying ALL entity types and relationships. 
Think through every section of the document carefully.

Remember: Every node type MUST include 'name' and 'source_citation' properties.

You MUST respond with a JSON object matching this schema:
{json.dumps(ONTOLOGY_RESPONSE_JSON_SCHEMA, indent=2)}"""

    print("[ONTOLOGY] Using Amazon Nova for comprehensive schema design...")
    
    messages = [_make_user_message(prompt)]
    
    response = await async_generate_with_retry(
        model_id=model,
        messages=messages,
        system_prompt=ONTOLOGY_SYSTEM_PROMPT,
        max_tokens=8192,
        temperature=1.0,
        max_retries=MAX_RETRIES,
    )
    
    response_text = extract_text_from_response(response)
    
    # Parse JSON response with robust error handling
    try:
        schema = json.loads(response_text)
    except json.JSONDecodeError as e:
        # Try extracting JSON from markdown code blocks
        import re
        json_match = re.search(r'```(?:json)?\s*\n?(.*?)\n?```', response_text, re.DOTALL)
        if json_match:
            schema = json.loads(json_match.group(1))
        else:
            json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
            if json_match:
                schema = json.loads(json_match.group())
            else:
                raise ValueError(f"Failed to parse JSON response (JSONDecodeError: {e}). Raw: {response_text[:500]}")
    
    # === VALIDATION PHASE ===
    
    # 1. Validate structure types
    if not isinstance(schema.get("nodes"), list):
        raise ValueError("Schema 'nodes' must be a list")
    if not isinstance(schema.get("relationships"), list):
        raise ValueError("Schema 'relationships' must be a list")
    
    # 2. Ensure 'name' and 'source_citation' on all nodes
    for node in schema["nodes"]:
        props = [p["name"] for p in node.get("properties", [])]
        
        if "name" not in props:
            node.setdefault("properties", []).insert(0, {
                "name": "name",
                "type": "string",
                "required": True,
                "description": "Unique identifier name for this entity"
            })
        
        if "source_citation" not in props:
            node.setdefault("properties", []).append({
                "name": "source_citation",
                "type": "string",
                "required": True,
                "description": "Reference to source section in policy document"
            })
    
    # 3. Validate relationship integrity
    node_labels = {n['label'] for n in schema['nodes']}
    for rel in schema['relationships']:
        if rel['from_label'] not in node_labels:
            raise ValueError(f"Relationship '{rel['type']}' references undefined source node '{rel['from_label']}'")
        if rel['to_label'] not in node_labels:
            raise ValueError(f"Relationship '{rel['type']}' references undefined target node '{rel['to_label']}'")
    
    # Save artifact
    artifact_path = save_artifact("proposed_schema", schema)
    schema["_artifact_path"] = artifact_path
    
    return schema


async def run_ontology_agent(log_callback: callable = None) -> Dict[str, Any]:
    """
    Main entry point for the Ontology Designer agent.
    """
    log = log_callback or (lambda msg: print(msg))
    
    log("[ONTOLOGY] Starting schema design with Amazon Nova...")
    log("[ONTOLOGY] Using deep reasoning for exhaustive extraction")
    
    try:
        schema = await design_ontology()
        
        node_count = len(schema.get("nodes", []))
        rel_count = len(schema.get("relationships", []))
        
        log(f"[ONTOLOGY] Schema designed: {node_count} node types, {rel_count} relationships")
        
        return {
            "status": "success",
            "schema": schema,
            "summary": {
                "node_types": node_count,
                "relationship_types": rel_count,
            }
        }
    except Exception as e:
        log(f"[ONTOLOGY] Design failed: {e}")
        return {
            "status": "error",
            "error": str(e)
        }


# =============================================================================
# CLI TESTING
# =============================================================================

if __name__ == "__main__":
    import asyncio
    result = asyncio.run(run_ontology_agent())
    print(json.dumps(result, indent=2, default=str))
