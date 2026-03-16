import os
import glob
import asyncio
from datetime import datetime
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP
from llama_parse import LlamaParse

load_dotenv()

# Apply nest_asyncio only if not running with uvloop
try:
    import nest_asyncio
    loop = asyncio.get_event_loop()
    if 'uvloop' not in type(loop).__module__:
        nest_asyncio.apply()
except Exception:
    pass

mcp = FastMCP("policy_ingestion")

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

# Default paths (can be overridden via arguments)
DEFAULT_PDF_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "policy_docs", "policy_pdfs")
DEFAULT_OUTPUT_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), "policy_docs", "combined_policy.md")


@mcp.tool()
async def parse_all_policy_documents(
    pdf_directory: str = None,
    output_file: str = None
) -> str:
    """
    Parses ALL PDF policy documents in a directory and combines them into a single Markdown file.
    Each page is prefixed with a marker for traceability: <!-- PAGE:filename:page:start_line:end_line -->
    Also generates a combined_policy_index.json for citation lookup.
    
    Args:
        pdf_directory: Path to folder containing PDFs. Defaults to policy_docs/policy_pdfs.
        output_file: Path for combined output. Defaults to policy_docs/combined_policy.md.
        
    Returns:
        Status message with parsing results.
    """
    import json
    
    # Use defaults if not provided
    pdf_dir = pdf_directory or DEFAULT_PDF_DIR
    output_path = output_file or DEFAULT_OUTPUT_FILE
    index_path = output_path.replace(".md", "_index.json")
    
    api_key = os.getenv("LLAMA_CLOUD_API_KEY")
    if not api_key:
        return "Error: LLAMA_CLOUD_API_KEY not found in environment variables."

    if not os.path.isdir(pdf_dir):
        return f"Error: Directory not found at {pdf_dir}"

    # Find all PDFs
    pdf_files = glob.glob(os.path.join(pdf_dir, "*.pdf"))
    if not pdf_files:
        return f"Error: No PDF files found in {pdf_dir}"

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
            f"**Source Directory**: {pdf_dir}",
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
            
            results.append(f"✅ {filename}: {len(documents)} pages")
        
        # Write combined markdown file
        final_content = "\n".join(combined_lines)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(final_content)
        
        # Write index file
        with open(index_path, "w", encoding="utf-8") as f:
            json.dump(page_index, f, indent=2)
        
        return (
            f"Successfully parsed {len(pdf_files)} documents.\n"
            f"Combined output: {output_path}\n"
            f"Index file: {index_path}\n"
            f"Total pages: {len(page_index['pages'])}\n"
            f"Total lines: {current_line - 1}\n\n"
            f"Details:\n" + "\n".join(results)
        )
        
    except Exception as e:
        return f"Error parsing policies: {str(e)}"


@mcp.tool()
async def parse_single_policy_document(pdf_path: str) -> str:
    """
    Parses a single policy PDF using LlamaParse and returns hierarchical Markdown.
    
    Args:
        pdf_path: Absolute path to the PDF file.
        
    Returns:
        The markdown content of the policy.
    """
    api_key = os.getenv("LLAMA_CLOUD_API_KEY")
    if not api_key:
        return "Error: LLAMA_CLOUD_API_KEY not found in environment variables."

    if not os.path.exists(pdf_path):
        return f"Error: File not found at {pdf_path}"

    try:
        parser = LlamaParse(
            api_key=api_key,
            result_type="markdown",
            system_prompt=PARSING_INSTRUCTION, 
            verbose=True
        )
        
        documents = await parser.aload_data(pdf_path)
        full_text = "\n\n".join([doc.text for doc in documents])
        
        output_path = f"{pdf_path}.md"
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(full_text)
            
        return f"Successfully parsed {pdf_path}.\nSaved to {output_path}.\n\nCONTENT SAMPLE:\n{full_text[:500]}..."
        
    except Exception as e:
        return f"Error parsing policy: {str(e)}"


if __name__ == "__main__":
    mcp.run(transport='stdio')

