# neo4j_graph_engine/mcp_server.py
"""
MCP Server for Neo4j Knowledge Graph operations.
Provides tools for building and querying the policy knowledge graph.
"""

import os
import json
import re
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

from .db import (
    execute_query,
    execute_write,
    test_connection,
    close_driver,
    get_session,
)

load_dotenv()

mcp = FastMCP("neo4j_graph")


# ==========================================
# CONNECTION & UTILITY TOOLS
# ==========================================

@mcp.tool()
async def check_neo4j_connection() -> Dict[str, Any]:
    """
    Test the Neo4j database connection.
    
    Returns:
        Connection status and server information.
    """
    return await test_connection()


@mcp.tool()
async def get_graph_schema() -> Dict[str, Any]:
    """
    Retrieve the current graph schema (node labels, relationship types, property keys).
    
    Returns:
        Dict containing nodes, relationships, and their properties in the graph.
    """
    try:
        # Get node labels and their counts
        labels_query = """
        CALL db.labels() YIELD label
        CALL apoc.cypher.run('MATCH (n:`' + label + '`) RETURN count(n) as count', {})
        YIELD value
        RETURN label, value.count as count
        """
        
        # Fallback if APOC not available
        labels_result = await execute_query("CALL db.labels() YIELD label RETURN label")
        labels = [r["label"] for r in labels_result]
        
        # Get relationship types
        rels_result = await execute_query("CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType")
        relationships = [r["relationshipType"] for r in rels_result]
        
        # Get property keys
        props_result = await execute_query("CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey")
        properties = [r["propertyKey"] for r in props_result]
        
        # Get node counts per label
        node_counts = {}
        for label in labels:
            count_result = await execute_query(f"MATCH (n:`{label}`) RETURN count(n) as count")
            node_counts[label] = count_result[0]["count"] if count_result else 0
        
        # Get relationship counts
        rel_count_result = await execute_query("MATCH ()-[r]->() RETURN count(r) as count")
        total_relationships = rel_count_result[0]["count"] if rel_count_result else 0
        
        return {
            "status": "success",
            "node_labels": labels,
            "node_counts": node_counts,
            "relationship_types": relationships,
            "total_relationships": total_relationships,
            "property_keys": properties,
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


@mcp.tool()
async def get_graph_statistics() -> Dict[str, Any]:
    """
    Get detailed statistics about the knowledge graph.
    
    Returns:
        Node counts, relationship counts, and sample data.
    """
    try:
        stats = {}
        
        # Total nodes
        result = await execute_query("MATCH (n) RETURN count(n) as count")
        stats["total_nodes"] = result[0]["count"] if result else 0
        
        # Total relationships
        result = await execute_query("MATCH ()-[r]->() RETURN count(r) as count")
        stats["total_relationships"] = result[0]["count"] if result else 0
        
        # Nodes by label
        result = await execute_query("""
            MATCH (n)
            UNWIND labels(n) as label
            RETURN label, count(*) as count
            ORDER BY count DESC
        """)
        stats["nodes_by_label"] = {r["label"]: r["count"] for r in result}
        
        # Relationships by type
        result = await execute_query("""
            MATCH ()-[r]->()
            RETURN type(r) as type, count(*) as count
            ORDER BY count DESC
        """)
        stats["relationships_by_type"] = {r["type"]: r["count"] for r in result}
        
        # Check for source citations
        result = await execute_query("""
            MATCH (n)
            WHERE n.source_citation IS NOT NULL
            RETURN count(n) as count
        """)
        stats["nodes_with_source_citation"] = result[0]["count"] if result else 0
        
        return {"status": "success", "statistics": stats}
    except Exception as e:
        return {"status": "error", "error": str(e)}


# ==========================================
# CYPHER EXECUTION TOOLS
# ==========================================

@mcp.tool()
async def execute_cypher_query(
    query: str,
    parameters: Optional[str] = None
) -> Dict[str, Any]:
    """
    Execute a read-only Cypher query against the knowledge graph.
    
    Args:
        query: Cypher query string (should be read-only: MATCH, RETURN, etc.)
        parameters: Optional JSON string of query parameters
        
    Returns:
        Query results as a list of records.
    """
    try:
        # Parse parameters if provided
        params = json.loads(parameters) if parameters else {}
        
        # Basic safety check - warn if it looks like a write query
        write_keywords = ["CREATE", "MERGE", "DELETE", "SET", "REMOVE", "DETACH"]
        query_upper = query.upper()
        if any(kw in query_upper for kw in write_keywords):
            return {
                "status": "error",
                "error": "This tool is for read queries only. Use execute_cypher_write for write operations.",
            }
        
        results = await execute_query(query, params)
        return {
            "status": "success",
            "record_count": len(results),
            "records": results[:100],  # Limit to prevent huge responses
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


@mcp.tool()
async def execute_cypher_write(
    query: str,
    parameters: Optional[str] = None
) -> Dict[str, Any]:
    """
    Execute a write Cypher query (CREATE, MERGE, DELETE, SET, etc.).
    
    Args:
        query: Cypher write query string
        parameters: Optional JSON string of query parameters
        
    Returns:
        Summary of changes made to the graph.
    """
    try:
        params = json.loads(parameters) if parameters else {}
        summary = await execute_write(query, params)
        return {
            "status": "success",
            "summary": summary,
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


@mcp.tool()
async def execute_cypher_batch(
    queries: str,
    stop_on_error: bool = True
) -> Dict[str, Any]:
    """
    Execute multiple Cypher statements in sequence.
    Ideal for bulk graph construction from extracted policy rules.
    
    Args:
        queries: JSON array of Cypher query strings
        stop_on_error: If True, stop execution on first error
        
    Returns:
        Summary of all executed queries with individual results.
    """
    try:
        query_list = json.loads(queries)
        if not isinstance(query_list, list):
            return {"status": "error", "error": "queries must be a JSON array of strings"}
        
        results = []
        total_nodes_created = 0
        total_rels_created = 0
        
        for i, query in enumerate(query_list):
            try:
                summary = await execute_write(query)
                total_nodes_created += summary.get("nodes_created", 0)
                total_rels_created += summary.get("relationships_created", 0)
                results.append({
                    "index": i,
                    "status": "success",
                    "summary": summary,
                })
            except Exception as e:
                results.append({
                    "index": i,
                    "status": "error",
                    "error": str(e),
                    "query": query[:200],  # Include partial query for debugging
                })
                if stop_on_error:
                    break
        
        return {
            "status": "success",
            "total_queries": len(query_list),
            "executed": len(results),
            "total_nodes_created": total_nodes_created,
            "total_relationships_created": total_rels_created,
            "results": results,
        }
    except json.JSONDecodeError as e:
        return {"status": "error", "error": f"Invalid JSON: {str(e)}"}
    except Exception as e:
        return {"status": "error", "error": str(e)}


# ==========================================
# GRAPH CONSTRUCTION TOOLS
# ==========================================

@mcp.tool()
async def create_node(
    label: str,
    properties: str,
    merge: bool = True
) -> Dict[str, Any]:
    """
    Create or merge a node in the knowledge graph.
    
    Args:
        label: Node label (e.g., "ProductCategory", "ReturnRule")
        properties: JSON string of node properties (must include source_citation)
        merge: If True, use MERGE to avoid duplicates. If False, use CREATE.
        
    Returns:
        Status and node details.
    """
    try:
        props = json.loads(properties)
        
        # Warn if source_citation is missing
        if "source_citation" not in props:
            return {
                "status": "warning",
                "message": "source_citation property is missing. All nodes should have source citations for traceability.",
            }
        
        # Build property string for Cypher
        prop_parts = []
        for key, value in props.items():
            if isinstance(value, str):
                prop_parts.append(f'{key}: "{value}"')
            else:
                prop_parts.append(f'{key}: {json.dumps(value)}')
        prop_string = ", ".join(prop_parts)
        
        operation = "MERGE" if merge else "CREATE"
        query = f'{operation} (n:{label} {{{prop_string}}}) RETURN n'
        
        result = await execute_query(query)
        summary = await execute_write(f'{operation} (n:{label} {{{prop_string}}})')
        
        return {
            "status": "success",
            "operation": operation,
            "label": label,
            "properties": props,
            "summary": summary,
        }
    except json.JSONDecodeError as e:
        return {"status": "error", "error": f"Invalid JSON properties: {str(e)}"}
    except Exception as e:
        return {"status": "error", "error": str(e)}


@mcp.tool()
async def create_relationship(
    from_label: str,
    from_match: str,
    to_label: str,
    to_match: str,
    rel_type: str,
    rel_properties: Optional[str] = None
) -> Dict[str, Any]:
    """
    Create a relationship between two nodes.
    
    Args:
        from_label: Label of the source node
        from_match: JSON string of properties to match source node
        to_label: Label of the target node
        to_match: JSON string of properties to match target node
        rel_type: Relationship type (e.g., "HAS_RULE", "OVERRIDES")
        rel_properties: Optional JSON string of relationship properties
        
    Returns:
        Status and relationship details.
    """
    try:
        from_props = json.loads(from_match)
        to_props = json.loads(to_match)
        rel_props = json.loads(rel_properties) if rel_properties else {}
        
        # Build match clauses
        from_match_parts = [f'n.{k} = "{v}"' if isinstance(v, str) else f'n.{k} = {v}' 
                           for k, v in from_props.items()]
        to_match_parts = [f'm.{k} = "{v}"' if isinstance(v, str) else f'm.{k} = {v}' 
                         for k, v in to_props.items()]
        
        from_where = " AND ".join(from_match_parts)
        to_where = " AND ".join(to_match_parts)
        
        # Build relationship properties
        if rel_props:
            rel_prop_parts = [f'{k}: "{v}"' if isinstance(v, str) else f'{k}: {json.dumps(v)}'
                             for k, v in rel_props.items()]
            rel_prop_string = " {" + ", ".join(rel_prop_parts) + "}"
        else:
            rel_prop_string = ""
        
        query = f"""
        MATCH (n:{from_label}), (m:{to_label})
        WHERE {from_where} AND {to_where}
        MERGE (n)-[r:{rel_type}{rel_prop_string}]->(m)
        RETURN type(r) as relationship_type
        """
        
        summary = await execute_write(query)
        
        return {
            "status": "success",
            "from": {"label": from_label, "match": from_props},
            "to": {"label": to_label, "match": to_props},
            "relationship": rel_type,
            "summary": summary,
        }
    except json.JSONDecodeError as e:
        return {"status": "error", "error": f"Invalid JSON: {str(e)}"}
    except Exception as e:
        return {"status": "error", "error": str(e)}


# ==========================================
# SCHEMA MANAGEMENT TOOLS
# ==========================================

@mcp.tool()
async def create_schema_constraints() -> Dict[str, Any]:
    """
    Create recommended indexes and constraints for the policy knowledge graph.
    Call this after initial graph construction for better query performance.
    
    Returns:
        Status of constraint creation.
    """
    constraints = [
        # Unique constraints for key identifiers
        "CREATE CONSTRAINT policy_name IF NOT EXISTS FOR (p:Policy) REQUIRE p.company_name IS UNIQUE",
        "CREATE CONSTRAINT category_name IF NOT EXISTS FOR (c:ProductCategory) REQUIRE c.name IS UNIQUE",
        "CREATE CONSTRAINT tier_name IF NOT EXISTS FOR (t:MembershipTier) REQUIRE t.tier_name IS UNIQUE",
        
        # Indexes for frequently queried properties
        "CREATE INDEX rule_days IF NOT EXISTS FOR (r:ReturnRule) ON (r.return_days)",
        "CREATE INDEX source_citation IF NOT EXISTS FOR (n:ReturnRule) ON (n.source_citation)",
        "CREATE INDEX exception_type IF NOT EXISTS FOR (e:Exception) ON (e.type)",
    ]
    
    results = []
    for constraint in constraints:
        try:
            await execute_write(constraint)
            results.append({"query": constraint[:80], "status": "success"})
        except Exception as e:
            # Constraint might already exist
            error_msg = str(e)
            if "already exists" in error_msg.lower():
                results.append({"query": constraint[:80], "status": "already_exists"})
            else:
                results.append({"query": constraint[:80], "status": "error", "error": error_msg})
    
    return {
        "status": "success",
        "constraints_processed": len(constraints),
        "results": results,
    }


@mcp.tool()
async def clear_graph(confirm: bool = False) -> Dict[str, Any]:
    """
    Delete all nodes and relationships from the graph.
    USE WITH CAUTION - this is destructive!
    
    Args:
        confirm: Must be True to execute. Safety flag to prevent accidental deletion.
        
    Returns:
        Summary of deleted nodes and relationships.
    """
    if not confirm:
        return {
            "status": "error",
            "error": "This will delete ALL data. Set confirm=True to proceed.",
        }
    
    try:
        # Delete in batches to avoid memory issues with large graphs
        deleted_nodes = 0
        deleted_rels = 0
        
        # First delete relationships
        while True:
            result = await execute_write("""
                MATCH ()-[r]->()
                WITH r LIMIT 10000
                DELETE r
            """)
            if result["relationships_deleted"] == 0:
                break
            deleted_rels += result["relationships_deleted"]
        
        # Then delete nodes
        while True:
            result = await execute_write("""
                MATCH (n)
                WITH n LIMIT 10000
                DELETE n
            """)
            if result["nodes_deleted"] == 0:
                break
            deleted_nodes += result["nodes_deleted"]
        
        return {
            "status": "success",
            "nodes_deleted": deleted_nodes,
            "relationships_deleted": deleted_rels,
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


# ==========================================
# VALIDATION TOOLS
# ==========================================

@mcp.tool()
async def validate_graph_integrity() -> Dict[str, Any]:
    """
    Validate the knowledge graph for common issues:
    - Missing source_citation properties
    - Orphan nodes (no relationships)
    - Duplicate nodes
    
    Returns:
        Validation report with issues found.
    """
    issues = []
    
    try:
        # Check for missing source_citation
        result = await execute_query("""
            MATCH (n)
            WHERE n.source_citation IS NULL
            RETURN labels(n) as labels, count(n) as count
        """)
        if result:
            for r in result:
                if r["count"] > 0:
                    issues.append({
                        "type": "missing_source_citation",
                        "labels": r["labels"],
                        "count": r["count"],
                    })
        
        # Check for orphan nodes (except Policy which might be root)
        result = await execute_query("""
            MATCH (n)
            WHERE NOT (n)--() AND NOT n:Policy
            RETURN labels(n) as labels, count(n) as count
        """)
        if result:
            for r in result:
                if r["count"] > 0:
                    issues.append({
                        "type": "orphan_nodes",
                        "labels": r["labels"],
                        "count": r["count"],
                    })
        
        # Check for duplicate category names
        result = await execute_query("""
            MATCH (c:ProductCategory)
            WITH c.name as name, count(*) as count
            WHERE count > 1
            RETURN name, count
        """)
        if result:
            for r in result:
                issues.append({
                    "type": "duplicate_category",
                    "name": r["name"],
                    "count": r["count"],
                })
        
        return {
            "status": "valid" if not issues else "issues_found",
            "issue_count": len(issues),
            "issues": issues,
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


@mcp.tool()
async def sample_graph_data(limit: int = 5) -> Dict[str, Any]:
    """
    Get sample data from the graph for verification.
    Useful for checking if the graph was built correctly.
    
    Args:
        limit: Number of samples per node type
        
    Returns:
        Sample nodes and relationships from each type.
    """
    try:
        samples = {}
        
        # Get labels
        labels_result = await execute_query("CALL db.labels() YIELD label RETURN label")
        
        for label_record in labels_result:
            label = label_record["label"]
            # Get sample nodes
            result = await execute_query(f"""
                MATCH (n:{label})
                RETURN properties(n) as props
                LIMIT {limit}
            """)
            samples[label] = [r["props"] for r in result]
        
        # Get sample relationships
        rel_samples = await execute_query(f"""
            MATCH (a)-[r]->(b)
            RETURN labels(a) as from_labels, type(r) as rel_type, 
                   labels(b) as to_labels, properties(r) as props
            LIMIT {limit * 3}
        """)
        samples["_relationships"] = [
            {
                "from": r["from_labels"],
                "type": r["rel_type"],
                "to": r["to_labels"],
                "properties": r["props"],
            }
            for r in rel_samples
        ]
        
        return {"status": "success", "samples": samples}
    except Exception as e:
        return {"status": "error", "error": str(e)}


if __name__ == "__main__":
    mcp.run(transport="stdio")
