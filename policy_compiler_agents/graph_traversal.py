# policy_compiler_agents/graph_traversal.py
"""
Graph Traversal Module for Adjudicator Agent.

This module provides structured traversal of the Neo4j knowledge graph
to fetch all policy-related nodes connected to a ProductCategory.
"""

import sys
import os
from typing import Any, Dict, List

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from neo4j_graph_engine.db import execute_query as query_graph


# =============================================================================
# STRUCTURED TRAVERSAL QUERY
# =============================================================================

TRAVERSAL_QUERY = """
MATCH (pc:ProductCategory {name: $category})

// Hop 1: Direct connections from ProductCategory
OPTIONAL MATCH (pc)-[r1]->(hop1)

// Hop 2: Secondary connections
OPTIONAL MATCH (hop1)-[r2]->(hop2)

// Hop 3: Tertiary connections (for deep chains like Fee -> WAIVED_IF -> Condition)
OPTIONAL MATCH (hop2)-[r3]->(hop3)

RETURN 
    properties(pc) AS category,
    
    collect(DISTINCT CASE WHEN hop1 IS NOT NULL THEN {
        rel: type(r1), 
        label: labels(hop1)[0], 
        data: properties(hop1)
    } END) AS hop1_nodes,
    
    collect(DISTINCT CASE WHEN hop2 IS NOT NULL THEN {
        via_rel: type(r1),
        via_label: labels(hop1)[0],
        via_data: properties(hop1),
        rel: type(r2),
        label: labels(hop2)[0],
        data: properties(hop2)
    } END) AS hop2_nodes,
    
    collect(DISTINCT CASE WHEN hop3 IS NOT NULL THEN {
        chain: [type(r1), type(r2), type(r3)],
        via_label: labels(hop2)[0],
        label: labels(hop3)[0],
        data: properties(hop3)
    } END) AS hop3_nodes
"""


async def traverse_from_category(category: str) -> Dict[str, Any]:
    """
    Traverse the knowledge graph starting from a ProductCategory.
    
    Returns all connected nodes within 3 hops, preserving relationship context.
    
    Args:
        category: The ProductCategory name to start traversal from
        
    Returns:
        Dict with category info, hop1_nodes, hop2_nodes, hop3_nodes
    """
    print(f"   [TRAVERSAL] Starting from category: {category}")
    
    results = await query_graph(TRAVERSAL_QUERY, {"category": category})
    
    if not results:
        print(f"   [TRAVERSAL] No results for category: {category}")
        return {
            "category": {"name": category},
            "hop1_nodes": [],
            "hop2_nodes": [],
            "hop3_nodes": []
        }
    
    result = results[0]
    
    # Filter out None values from collections
    hop1 = [n for n in result.get("hop1_nodes", []) if n is not None]
    hop2 = [n for n in result.get("hop2_nodes", []) if n is not None]
    hop3 = [n for n in result.get("hop3_nodes", []) if n is not None]
    
    print(f"   [TRAVERSAL] Found {len(hop1)} hop1, {len(hop2)} hop2, {len(hop3)} hop3 nodes")
    
    return {
        "category": result.get("category", {"name": category}),
        "hop1_nodes": hop1,
        "hop2_nodes": hop2,
        "hop3_nodes": hop3
    }


def build_policy_profile(traversal_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert raw traversal result into a structured policy profile.
    
    Groups nodes by type and preserves relationship context for decision-making.
    
    Args:
        traversal_result: Output from traverse_from_category()
        
    Returns:
        Structured policy profile with windows, fees, restrictions, etc.
    """
    profile = {
        "category": traversal_result["category"].get("name", "Unknown"),
        "windows": [],          # ReturnWindow nodes
        "fees": [],             # Fee nodes with waiver info
        "restrictions": [],     # Restriction nodes with trigger info
        "required_conditions": [],  # Conditions required for return
        "tiers": [],            # MembershipTier associations
        "excluded_methods": [], # ReturnMethod exclusions
        "citations": set()      # All source citations for source text lookup
    }
    
    # Process Hop 1 - Direct connections from ProductCategory
    for node in traversal_result["hop1_nodes"]:
        rel = node.get("rel", "")
        label = node.get("label", "")
        data = node.get("data", {})
        
        # Collect citation
        if data.get("source_citation"):
            profile["citations"].add(data["source_citation"])
        
        if rel == "HAS_RETURN_WINDOW" and label == "ReturnWindow":
            profile["windows"].append({
                "name": data.get("name"),
                "days": data.get("days"),
                "citation": data.get("source_citation"),
                "tiers": []  # Will be filled from hop2
            })
        
        elif rel == "SUBJECT_TO_FEE" and label == "Fee":
            profile["fees"].append({
                "name": data.get("name"),
                "value": data.get("value"),
                "amount_type": data.get("amount_type"),
                "citation": data.get("source_citation"),
                "waivers": [],      # Conditions that waive this fee
                "exemptions": []    # Regions where fee is exempt
            })
        
        elif rel == "HAS_RESTRICTION" and label == "Restriction":
            profile["restrictions"].append({
                "name": data.get("name"),
                "citation": data.get("source_citation"),
                "triggers": []  # Conditions that trigger this restriction
            })
        
        elif rel == "REQUIRES_CONDITION" and label == "Condition":
            profile["required_conditions"].append({
                "name": data.get("name"),
                "citation": data.get("source_citation")
            })
        
        elif rel == "EXCLUDES_METHOD" and label == "ReturnMethod":
            profile["excluded_methods"].append({
                "name": data.get("name"),
                "citation": data.get("source_citation")
            })
    
    # Process Hop 2 - Fill in secondary relationships
    for node in traversal_result["hop2_nodes"]:
        via_rel = node.get("via_rel", "")
        via_data = node.get("via_data", {})
        to_rel = node.get("rel", "")
        to_label = node.get("label", "")
        to_data = node.get("data", {})
        
        # Collect citation
        if to_data.get("source_citation"):
            profile["citations"].add(to_data["source_citation"])
        
        # ReturnWindow -> MembershipTier (which tier gets this window)
        if via_rel == "HAS_RETURN_WINDOW" and to_rel == "APPLIES_TO_MEMBERSHIP":
            window_name = via_data.get("name")
            tier_name = to_data.get("name")
            for w in profile["windows"]:
                if w["name"] == window_name:
                    w["tiers"].append(tier_name)
        
        # Fee -> Condition (waiver condition)
        elif via_rel == "SUBJECT_TO_FEE" and to_rel == "WAIVED_IF":
            fee_name = via_data.get("name")
            condition_name = to_data.get("name")
            for f in profile["fees"]:
                if f["name"] == fee_name:
                    f["waivers"].append(condition_name)
        
        # Fee -> Region (exemption region)
        elif via_rel == "SUBJECT_TO_FEE" and to_rel == "EXEMPT_IN_REGION":
            fee_name = via_data.get("name")
            region_name = to_data.get("name")
            for f in profile["fees"]:
                if f["name"] == fee_name:
                    f["exemptions"].append(region_name)
        
        # Restriction -> Condition (trigger condition)
        elif via_rel == "HAS_RESTRICTION" and to_rel == "TRIGGERED_BY_CONDITION":
            restriction_name = via_data.get("name")
            condition_name = to_data.get("name")
            for r in profile["restrictions"]:
                if r["name"] == restriction_name:
                    r["triggers"].append(condition_name)
    
    # Convert citations set to list
    profile["citations"] = list(profile["citations"])
    
    return profile


async def get_all_categories() -> List[str]:
    """Fetch all ProductCategory names from Neo4j."""
    results = await query_graph("MATCH (p:ProductCategory) RETURN p.name as name ORDER BY p.name")
    return [r["name"] for r in results if r.get("name")]
