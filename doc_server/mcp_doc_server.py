import os
import base64
import io
from mcp.server.fastmcp import FastMCP
from pypdf import PdfReader

# Initialize FastMCP server
mcp = FastMCP("doc_server")

@mcp.tool()
def process_invoice(base64_content: str, output_txt_path: str) -> str:
    """
    Decodes a base64 PDF, parses the text, saves the text to a file, and returns the content.

    Args:
        base64_content: The base64 encoded string of the PDF file.
        output_txt_path: The absolute path where the parsed text should be saved.

    Returns:
        The extracted text content from the PDF.
    """
    try:
        # Sanitize base64 string (remove data prefix if present)
        if "," in base64_content:
            base64_content = base64_content.split(",")[1]

        pdf_bytes = base64.b64decode(base64_content)
        
        # Parse PDF using BytesIO
        reader = PdfReader(io.BytesIO(pdf_bytes))
        text = ""
        for page in reader.pages:
            text += page.extract_text() + "\n"
        
        # Ensure the directory exists
        os.makedirs(os.path.dirname(output_txt_path), exist_ok=True)
        
        # Save parsed text to the specified path
        with open(output_txt_path, "w", encoding="utf-8") as f:
            f.write(text)
            
        return f"Successfully parsed. Saved to {output_txt_path}\n\nEXTRACTED TEXT:\n{text}"

    except Exception as e:
        return f"Error processing invoice: {str(e)}"

if __name__ == "__main__":
    mcp.run(transport='stdio')