# policy_compiler_agents/tools.py
"""
Shared tools for the Policy Compiler multi-agent system.
Provides utilities for reading policy documents, saving artifacts,
and interacting with the Neo4j knowledge graph.
"""

import os
import json
from datetime import datetime
from typing import Any, Dict, Optional, List

from dotenv import load_dotenv

load_dotenv()

# Paths
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
POLICY_DOCS_DIR = os.path.join(PROJECT_ROOT, "policy_docs")
ARTIFACTS_DIR = os.path.join(PROJECT_ROOT, "artifacts", "knowledge_graph")

# Ensure artifacts directory exists
os.makedirs(ARTIFACTS_DIR, exist_ok=True)


def read_policy_markdown(filename: str = "combined_policy.md") -> str:
    """
    Read the parsed policy markdown file.
    
    Args:
        filename: Name of the markdown file in policy_docs/
        
    Returns:
        Content of the policy markdown file
    """
    filepath = os.path.join(POLICY_DOCS_DIR, filename)
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Policy file not found: {filepath}")
    
    with open(filepath, "r", encoding="utf-8") as f:
        return f.read()


def save_artifact(name: str, content: Any, artifact_type: str = "json") -> str:
    """
    Save an artifact to the phase1 artifacts directory.
    
    Args:
        name: Base name of the artifact (without extension)
        content: Content to save (will be JSON serialized if not a string)
        artifact_type: "json" or "md"
        
    Returns:
        Path to the saved artifact
    """
    extension = ".json" if artifact_type == "json" else ".md"
    filepath = os.path.join(ARTIFACTS_DIR, f"{name}{extension}")
    
    # Add metadata
    if artifact_type == "json" and isinstance(content, dict):
        content["_metadata"] = {
            "generated_at": datetime.now().isoformat(),
            "artifact_name": name,
        }
    
    with open(filepath, "w", encoding="utf-8") as f:
        if artifact_type == "json":
            json.dump(content, f, indent=2, ensure_ascii=False)
        else:
            f.write(str(content))
    
    return filepath


def load_artifact(name: str, artifact_type: str = "json") -> Any:
    """
    Load a previously saved artifact.
    
    Args:
        name: Base name of the artifact (without extension)
        artifact_type: "json" or "md"
        
    Returns:
        Loaded artifact content
    """
    extension = ".json" if artifact_type == "json" else ".md"
    filepath = os.path.join(ARTIFACTS_DIR, f"{name}{extension}")
    
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Artifact not found: {filepath}")
    
    with open(filepath, "r", encoding="utf-8") as f:
        if artifact_type == "json":
            return json.load(f)
        return f.read()


def extract_section_citations(markdown_content: str) -> Dict[str, str]:
    """
    Extract section headers and their line numbers from markdown.
    Used for generating source_citation values.
    
    Args:
        markdown_content: The policy markdown content
        
    Returns:
        Dict mapping section text to section number (e.g., "Return Periods" -> "Section 3")
    """
    import re
    
    sections = {}
    current_section = "Header"
    current_subsection = ""
    
    lines = markdown_content.split("\n")
    for line in lines:
        # Match headers like "## 3. Return and Exchange Periods"
        h1_match = re.match(r'^# (.+)$', line)
        h2_match = re.match(r'^## (\d+\.?\s*)(.+)$', line)
        h3_match = re.match(r'^### (\d+\.\d+\.?\s*)(.+)$', line)
        
        if h1_match:
            current_section = "Header"
            sections[h1_match.group(1).strip()] = "Header"
        elif h2_match:
            section_num = h2_match.group(1).strip().rstrip('.')
            section_title = h2_match.group(2).strip()
            current_section = f"Section {section_num}"
            sections[section_title] = current_section
        elif h3_match:
            subsection_num = h3_match.group(1).strip().rstrip('.')
            subsection_title = h3_match.group(2).strip()
            current_subsection = f"Section {subsection_num}"
            sections[subsection_title] = current_subsection
    
    return sections


def get_bedrock_client():
    """
    Get a configured Bedrock Runtime client for agent operations.
    
    Returns:
        boto3 Bedrock Runtime client
    """
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    from bedrock_client import get_bedrock_client as _get_client
    return _get_client()


# Backward compatibility alias
get_nova_client = get_bedrock_client


# Pre-load policy content for agents
def get_policy_content() -> str:
    """Get the cached policy content."""
    return read_policy_markdown()


class CitationManager:
    """
    Manages accurate source citations for extracted policy entities.
    Uses page markers and line index for precise citation generation.
    
    Citation format: filename:pageX:lineY-Z
    Example: Bestbuytnc1.pdf:page2:line45-52
    """
    
    def __init__(self):
        self.markdown = read_policy_markdown()
        self.lines = self.markdown.split("\n")
        self.index = self._load_index()
        
    def _load_index(self) -> Dict[str, Any]:
        """Load the page index file."""
        index_path = os.path.join(POLICY_DOCS_DIR, "combined_policy_index.json")
        if os.path.exists(index_path):
            with open(index_path, "r", encoding="utf-8") as f:
                return json.load(f)
        return {"pages": []}
    
    def get_page_for_line(self, line_num: int) -> Optional[Dict[str, Any]]:
        """Find which page a line belongs to."""
        for page in self.index.get("pages", []):
            if page["start_line"] <= line_num <= page["end_line"]:
                return page
        return None
    
    def find_text_citation(self, text: str) -> str:
        """
        Search for text in markdown and return precise citation.
        
        Args:
            text: Text excerpt to search for
            
        Returns:
            Citation string in format: filename:pageX:lineY-Z
        """
        if not text or len(text) < 5:
            return self._fallback_citation()
        
        # Clean the text for searching
        search_text = text.strip().lower()
        
        # Strategy 1: Exact match
        for i, line in enumerate(self.lines, start=1):
            if search_text in line.lower():
                page = self.get_page_for_line(i)
                if page:
                    return f"{page['filename']}:page{page['page']}:line{i}"
        
        # Strategy 2: First 50 characters match
        short_text = search_text[:50] if len(search_text) > 50 else search_text
        for i, line in enumerate(self.lines, start=1):
            if short_text in line.lower():
                page = self.get_page_for_line(i)
                if page:
                    return f"{page['filename']}:page{page['page']}:line{i}"
        
        # Strategy 3: Key phrase match (first few words)
        words = search_text.split()[:5]
        key_phrase = " ".join(words)
        for i, line in enumerate(self.lines, start=1):
            if key_phrase in line.lower():
                page = self.get_page_for_line(i)
                if page:
                    return f"{page['filename']}:page{page['page']}:line{i}"
        
        # Fallback: return first page citation
        return self._fallback_citation()
    
    def _fallback_citation(self) -> str:
        """Return a fallback citation when text not found."""
        if self.index.get("pages"):
            first_page = self.index["pages"][0]
            return f"{first_page['filename']}:page1"
        return "unknown:page1"
    
    def add_citations_to_entities(self, entities: list) -> list:
        """
        Add source_citation to all entities based on their text_excerpt.
        
        Args:
            entities: List of entity dicts with 'text_excerpt' field
            
        Returns:
            Entities with source_citation added
        """
        for entity in entities:
            text_excerpt = entity.get("text_excerpt", entity.get("description", ""))
            entity["source_citation"] = self.find_text_citation(text_excerpt)
        return entities
    
    def add_citations_to_cypher(self, statements: list, entities: list) -> list:
        """
        Add source_citation to Cypher statements that don't have one.
        
        Args:
            statements: List of Cypher MERGE statements
            entities: List of entities with source_citation
            
        Returns:
            Updated Cypher statements with citations
        """
        import re
        
        # Build entity name -> citation mapping
        citation_map = {}
        for entity in entities:
            name = entity.get("properties", {}).get("name", "")
            if name and "source_citation" in entity:
                citation_map[name.lower()] = entity["source_citation"]
        
        updated_statements = []
        for stmt in statements:
            # Check if statement already has source_citation
            if "source_citation" in stmt:
                updated_statements.append(stmt)
                continue
            
            # Try to find entity name in statement and add citation
            for name, citation in citation_map.items():
                if name in stmt.lower():
                    # Insert source_citation before closing }
                    if stmt.rstrip().endswith("})"):
                        stmt = stmt.rstrip()[:-2] + f', source_citation: "{citation}"' + "})"
                    elif stmt.rstrip().endswith("}"):
                        stmt = stmt.rstrip()[:-1] + f', source_citation: "{citation}"' + "}"
                    break
            
            updated_statements.append(stmt)
        
        return updated_statements



