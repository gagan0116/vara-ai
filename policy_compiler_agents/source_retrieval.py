# policy_compiler_agents/source_retrieval.py
"""
Source Text Retrieval Module for Adjudicator Agent.

This module parses citations from the knowledge graph and retrieves
the original policy text from combined_policy.md for LLM context.
"""

import os
import json
import re
from typing import Dict, List, Optional

# Path configuration
POLICY_DOCS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "policy_docs")
COMBINED_POLICY_PATH = os.path.join(POLICY_DOCS_DIR, "combined_policy.md")
POLICY_INDEX_PATH = os.path.join(POLICY_DOCS_DIR, "combined_policy_index.json")


def parse_citation(citation: str) -> Optional[Dict[str, any]]:
    """
    Parse a citation string into its components.
    
    Format: "Bestbuytnc1.pdf:page1:line33"
    
    Args:
        citation: The citation string to parse
        
    Returns:
        Dict with filename, page, line_number or None if invalid
    """
    if not citation:
        return None
    
    # Match pattern: filename:pageN:lineN
    pattern = r"^(.+\.pdf):page(\d+):line(\d+)$"
    match = re.match(pattern, citation, re.IGNORECASE)
    
    if match:
        return {
            "filename": match.group(1),
            "page": int(match.group(2)),
            "line": int(match.group(3))
        }
    
    return None


def load_policy_index() -> Dict:
    """Load the policy index JSON file."""
    try:
        with open(POLICY_INDEX_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"   [SOURCE] Error loading policy index: {e}")
        return {"pages": []}


def load_policy_markdown() -> List[str]:
    """Load the combined policy markdown as lines."""
    try:
        with open(COMBINED_POLICY_PATH, 'r', encoding='utf-8') as f:
            return f.readlines()
    except Exception as e:
        print(f"   [SOURCE] Error loading policy markdown: {e}")
        return []


def get_source_text(citations: List[str], context_lines: int = 5) -> Dict[str, str]:
    """
    Retrieve original policy text for each citation.
    
    Args:
        citations: List of citation strings to look up
        context_lines: Number of lines before/after to include
        
    Returns:
        Dict mapping citation -> extracted text
    """
    if not citations:
        return {}
    
    # Load policy file
    lines = load_policy_markdown()
    if not lines:
        print("   [SOURCE] No policy markdown loaded")
        return {}
    
    # Load index for page-to-line mapping
    index = load_policy_index()
    
    source_texts = {}
    
    for citation in citations:
        parsed = parse_citation(citation)
        if not parsed:
            print(f"   [SOURCE] Could not parse citation: {citation}")
            continue
        
        # Find the actual line number in combined_policy.md
        # The citation format references the original PDF, but we need
        # to find the corresponding line in our combined markdown
        
        # Option 1: Use page-based lookup from index
        page_info = None
        for page in index.get("pages", []):
            if page["page"] == parsed["page"] and page["filename"] == parsed["filename"]:
                page_info = page
                break
        
        if page_info:
            # Calculate absolute line number in combined markdown
            # Citation line is relative to page, add to page start_line
            relative_line = parsed["line"]
            # The citation line number might be from original PDF, 
            # so we locate within the page range
            target_line = page_info["start_line"] + (relative_line - 1)
        else:
            # Fallback: use line number directly (may be off)
            target_line = parsed["line"]
        
        # Ensure within bounds
        target_line = max(1, min(target_line, len(lines)))
        
        # Extract context window
        start = max(0, target_line - context_lines - 1)
        end = min(len(lines), target_line + context_lines)
        
        extracted_text = "".join(lines[start:end]).strip()
        
        if extracted_text:
            source_texts[citation] = extracted_text
            print(f"   [SOURCE] Extracted {end - start} lines for citation: {citation}")
        else:
            print(f"   [SOURCE] No text found for citation: {citation}")
    
    return source_texts


def format_source_texts_for_prompt(source_texts: Dict[str, str]) -> str:
    """
    Format source texts for inclusion in LLM prompt.
    
    Args:
        source_texts: Dict mapping citation -> text
        
    Returns:
        Formatted string for prompt inclusion
    """
    if not source_texts:
        return "No source text available."
    
    formatted_parts = []
    for citation, text in source_texts.items():
        # Truncate to avoid excessive length
        truncated = text[:500] + "..." if len(text) > 500 else text
        formatted_parts.append(f"[{citation}]\n{truncated}")
    
    return "\n\n---\n\n".join(formatted_parts)
