# policy_compiler_agents/visualize_graph.py
"""
Graph Visualization Tool - Generates interactive HTML visualization from Neo4j.

This module queries the Neo4j knowledge graph and creates a standalone HTML file
with an interactive Vis.js network visualization.
"""

import asyncio
import json
import os
from typing import Any, Dict, List

from .tools import ARTIFACTS_DIR

# Import Neo4j operations
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from neo4j_graph_engine.db import execute_query, test_connection, close_driver


# Color palette for node labels
LABEL_COLORS = {
    "ProductCategory": "#4CAF50",      # Green
    "ReturnRule": "#2196F3",           # Blue
    "Condition": "#FF9800",            # Orange
    "Fee": "#F44336",                  # Red
    "MembershipTier": "#9C27B0",       # Purple
    "Exception": "#795548",            # Brown
    "TimeWindow": "#00BCD4",           # Cyan
    "RestockingFee": "#E91E63",        # Pink
    "NonReturnableItem": "#607D8B",    # Gray
    "Default": "#9E9E9E",              # Default gray
}


async def fetch_graph_data() -> Dict[str, Any]:
    """
    Fetch all nodes and relationships from Neo4j.
    
    Returns:
        Dictionary with 'nodes' and 'edges' lists for visualization
    """
    # Fetch all nodes (using elementId instead of deprecated id)
    nodes_query = """
    MATCH (n)
    RETURN elementId(n) as id, labels(n) as labels, properties(n) as props
    """
    nodes_result = await execute_query(nodes_query)
    
    # Fetch all relationships (using elementId instead of deprecated id)
    rels_query = """
    MATCH (a)-[r]->(b)
    RETURN elementId(a) as source, elementId(b) as target, type(r) as type
    """
    rels_result = await execute_query(rels_query)
    
    # Process nodes for Vis.js format
    vis_nodes = []
    for node in nodes_result:
        label = node["labels"][0] if node["labels"] else "Unknown"
        props = node["props"]
        display_name = props.get("name", props.get("description", f"Node {node['id']}"))
        
        # Truncate long names
        if len(display_name) > 30:
            display_name = display_name[:27] + "..."
            
        # Add key properties to label for better visibility
        if "days_allowed" in props:
            display_name += f"\n({props['days_allowed']} days)"
        elif "days" in props:
            display_name += f"\n({props['days']} days)"
        elif "percentage" in props:
            display_name += f"\n({props['percentage']}%)"
        elif "amount" in props:
            display_name += f"\n(${props['amount']})"
        
        vis_nodes.append({
            "id": node["id"],
            "label": display_name,
            "title": json.dumps(props, indent=2),  # Hover tooltip
            "group": label,
            "color": LABEL_COLORS.get(label, LABEL_COLORS["Default"]),
        })
    
    # Process relationships for Vis.js format
    vis_edges = []
    for rel in rels_result:
        vis_edges.append({
            "from": rel["source"],
            "to": rel["target"],
            "label": rel["type"],
            "arrows": "to",
        })
    
    return {
        "nodes": vis_nodes,
        "edges": vis_edges,
        "stats": {
            "node_count": len(vis_nodes),
            "edge_count": len(vis_edges),
        }
    }


def generate_html(graph_data: Dict[str, Any]) -> str:
    """
    Generate standalone HTML with Vis.js visualization.
    
    Args:
        graph_data: Dictionary with nodes and edges
        
    Returns:
        Complete HTML string
    """
    nodes_json = json.dumps(graph_data["nodes"])
    edges_json = json.dumps(graph_data["edges"])
    stats = graph_data["stats"]
    
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Policy Knowledge Graph</title>
    <script src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: #1a1a2e;
            color: #eee;
        }}
        #header {{
            background: linear-gradient(135deg, #16213e 0%, #0f3460 100%);
            padding: 20px;
            text-align: center;
            border-bottom: 2px solid #e94560;
        }}
        #header h1 {{
            color: #e94560;
            margin-bottom: 10px;
        }}
        #stats {{
            display: flex;
            justify-content: center;
            gap: 30px;
            margin-top: 10px;
        }}
        .stat {{
            background: rgba(233, 69, 96, 0.2);
            padding: 8px 20px;
            border-radius: 20px;
            border: 1px solid #e94560;
        }}
        .stat-value {{
            font-size: 1.5em;
            font-weight: bold;
            color: #e94560;
        }}
        #graph-container {{
            width: 100%;
            height: calc(100vh - 120px);
        }}
        #legend {{
            position: fixed;
            top: 130px;
            right: 20px;
            background: rgba(22, 33, 62, 0.95);
            padding: 15px;
            border-radius: 10px;
            border: 1px solid #0f3460;
            max-height: 400px;
            overflow-y: auto;
        }}
        #legend h3 {{
            margin-bottom: 10px;
            color: #e94560;
        }}
        .legend-item {{
            display: flex;
            align-items: center;
            margin: 5px 0;
        }}
        .legend-color {{
            width: 16px;
            height: 16px;
            border-radius: 50%;
            margin-right: 10px;
        }}
    </style>
</head>
<body>
    <div id="header">
        <h1>Policy Knowledge Graph</h1>
        <div id="stats">
            <div class="stat">
                <span class="stat-value">{stats["node_count"]}</span> Nodes
            </div>
            <div class="stat">
                <span class="stat-value">{stats["edge_count"]}</span> Relationships
            </div>
        </div>
    </div>
    
    <div id="graph-container"></div>
    
    <div id="legend">
        <h3>Node Types</h3>
        {"".join([f'<div class="legend-item"><div class="legend-color" style="background: {color};"></div>{label}</div>' for label, color in LABEL_COLORS.items() if label != "Default"])}
    </div>
    
    <script>
        // Graph data
        var nodes = new vis.DataSet({nodes_json});
        var edges = new vis.DataSet({edges_json});
        
        // Container
        var container = document.getElementById('graph-container');
        
        // Options
        var options = {{
            nodes: {{
                shape: 'dot',
                size: 20,
                font: {{
                    size: 12,
                    color: '#ffffff'
                }},
                borderWidth: 2,
                shadow: true
            }},
            edges: {{
                width: 1,
                color: {{
                    color: '#848484',
                    highlight: '#e94560'
                }},
                font: {{
                    size: 10,
                    color: '#aaa',
                    strokeWidth: 0
                }},
                smooth: {{
                    type: 'continuous'
                }}
            }},
            physics: {{
                enabled: true,
                barnesHut: {{
                    gravitationalConstant: -8000,
                    centralGravity: 0.3,
                    springLength: 150,
                    springConstant: 0.04,
                    damping: 0.09
                }},
                stabilization: {{
                    iterations: 150
                }}
            }},
            interaction: {{
                hover: true,
                tooltipDelay: 200,
                hideEdgesOnDrag: true
            }}
        }};
        
        // Create network
        var network = new vis.Network(container, {{ nodes: nodes, edges: edges }}, options);
        
        // Stabilization complete
        network.on("stabilizationIterationsDone", function () {{
            network.setOptions({{ physics: false }});
        }});
    </script>
</body>
</html>"""
    
    return html


async def visualize_graph(output_filename: str = "graph_visualization.html") -> str:
    """
    Main entry point: fetch graph data and generate visualization.
    
    Args:
        output_filename: Name of the output HTML file
        
    Returns:
        Path to the generated HTML file
    """
    print("[VISUALIZE] Connecting to Neo4j...")
    
    # Test connection
    conn = await test_connection()
    if conn.get("status") != "connected":
        raise ConnectionError(f"Cannot connect to Neo4j: {conn.get('error')}")
    
    print("[VISUALIZE] Fetching graph data...")
    graph_data = await fetch_graph_data()
    
    print(f"[VISUALIZE] Found {graph_data['stats']['node_count']} nodes, {graph_data['stats']['edge_count']} relationships")
    
    # Generate HTML
    html_content = generate_html(graph_data)
    
    # Save to artifacts
    output_path = os.path.join(ARTIFACTS_DIR, output_filename)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_content)
    
    print(f"[VISUALIZE] Saved to: {output_path}")
    
    return output_path


async def run_visualizer() -> Dict[str, Any]:
    """
    CLI entry point for the visualizer.
    """
    try:
        output_path = await visualize_graph()
        return {
            "status": "success",
            "output_path": output_path,
        }
    except Exception as e:
        print(f"[VISUALIZE] Error: {e}")
        return {
            "status": "error",
            "error": str(e),
        }
    finally:
        await close_driver()


if __name__ == "__main__":
    result = asyncio.run(run_visualizer())
    print(json.dumps(result, indent=2))
