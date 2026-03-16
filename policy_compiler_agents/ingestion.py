# policy_compiler_agents/ingestion.py
"""
Ingestion Module - Handles PDF parsing and Markdown generation.

This module uses LlamaParse to convert policy PDF documents into a structured
Markdown format suitable for entity extraction.

NOTE: This code mirrors the working implementation from policy_engine/mcp_server.py
"""

import os
import glob
import json
import asyncio
from datetime import datetime
from typing import Dict, Any
from llama_parse import LlamaParse

# Apply nest_asyncio only if not running with uvloop (Docker/production)
try:
    import nest_asyncio
    loop = asyncio.get_event_loop()
    # Only apply if it's a standard event loop (not uvloop)
    if 'uvloop' not in type(loop).__module__:
        nest_asyncio.apply()
except Exception:
    pass  # Skip if already patched or incompatible loop

PARSING_INSTRUCTION = """
This is a retail return policy document. Your task is to output hierarchical Markdown.

RULES:
1. Identify and preserve section numbers (e.g., 4.1, 4.2).
2. Use `#` for main titles, `##` for section headers, `###` for subsections.
3. Convert ALL tables to clean Markdown table format.
4. Nest exceptions (e.g., "Opened items") under their parent category.
5. Preserve bullet points.
6. Highlight key terms like "refund window", "return period", "non-returnable" in **bold**.
7. EXCLUDE web page footers, navigation menus, ads, social media links, and copyright sections (e.g. "Order & Purchases", "Support & Services", "Best Buy app").
8. Do NOT generate a summary or conclusion at the end. Output only the document content.
"""


async def parse_documents(
    pdf_directory: str,
    output_file: str,
    api_key: str
) -> Dict[str, Any]:
    """
    Parses ALL PDF policy documents in a directory and combines them into a single Markdown file.
    Each page is prefixed with a marker for traceability: <!-- PAGE:filename:page:start_line:end_line -->
    Also generates a combined_policy_index.json for citation lookup.
    
    Args:
        pdf_directory: Path to folder containing PDFs.
        output_file: Path for combined output markdown file.
        api_key: LlamaCloud API Key.
        
    Returns:
        Dictionary with status and stats.
    """
    # Validate directory
    if not os.path.isdir(pdf_directory):
        return {"status": "error", "message": f"Directory not found: {pdf_directory}"}

    # Find all PDFs
    pdf_files = glob.glob(os.path.join(pdf_directory, "*.pdf"))
    if not pdf_files:
        return {"status": "error", "message": "No PDF files found."}

    index_path = output_file.replace(".md", "_index.json")

    try:
        parser = LlamaParse(
            api_key=api_key,
            result_type="markdown",
            system_prompt=PARSING_INSTRUCTION,
            verbose=True
        )
        
        combined_lines = []
        page_index = {"pages": [], "generated_at": datetime.now().isoformat()}
        current_line = 1
        
        # Header
        header_lines = [
            "# Combined Policy Documents",
            f"**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"**Source Directory**: {pdf_directory}",
            f"**Total Documents**: {len(pdf_files)}",
            "",
            "---",
            ""
        ]
        combined_lines.extend(header_lines)
        current_line += len(header_lines)
        
        results = []
        for pdf_path in sorted(pdf_files):
            filename = os.path.basename(pdf_path)
            
            # Parse the PDF - each document is a page
            documents = await parser.aload_data(pdf_path)
            
            for page_num, doc in enumerate(documents, start=1):
                page_lines = doc.text.split("\n")
                start_line = current_line
                end_line = current_line + len(page_lines) - 1
                
                # Add page marker
                page_marker = f"<!-- PAGE:{filename}:{page_num}:{start_line}:{end_line} -->"
                combined_lines.append(page_marker)
                current_line += 1
                
                # Add page content
                combined_lines.extend(page_lines)
                current_line += len(page_lines)
                
                # Add empty line between pages
                combined_lines.append("")
                current_line += 1
                
                # Record in index
                page_index["pages"].append({
                    "filename": filename,
                    "page": page_num,
                    "start_line": start_line,
                    "end_line": end_line
                })
            
            results.append(f"{filename}: {len(documents)} pages")
        
        # Write combined markdown file
        final_content = "\n".join(combined_lines)
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(final_content)
        
        # Write index file
        with open(index_path, "w", encoding="utf-8") as f:
            json.dump(page_index, f, indent=2)
        
        return {
            "status": "success",
            "files_processed": len(pdf_files),
            "total_pages": len(page_index["pages"]),
            "total_lines": current_line - 1,
            "output_file": output_file,
            "index_file": index_path,
            "details": results
        }
        
    except Exception as e:
        return {"status": "error", "message": str(e)}
