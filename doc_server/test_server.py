import sys
import os

# Add the mcp_doc_server directory to the Python path
sys.path.append(os.path.join(os.getcwd(), "mcp_doc_server"))

# Import the tool function directly
from doc_server import parse_invoice

def main():
    # 1. Define the path to a sample PDF
    # Make sure you have a file named 'sample.pdf' in your root folder!
    pdf_path = os.path.join(os.getcwd(), "artifacts/212_invoice.pdf")
    
    print(f"Testing invoice parsing on: {pdf_path}")
    
    # 2. Check if file exists before running
    if not os.path.exists(pdf_path):
        print(f"Error: Could not find '{pdf_path}'.")
        print("Please place a PDF file named 'sample.pdf' in this folder to test.")
        return

    # 3. Run the tool
    result = parse_invoice(pdf_path)
    
    # 4. Print the output
    print("-" * 50)
    print("Result:")
    print(result)
    print("-" * 50)

if __name__ == "__main__":
    main()