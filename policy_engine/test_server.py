"""
Test script for Policy Ingestion MCP Server.
Tests the parse_all_policy_documents tool.
"""
import asyncio
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from mcp_server import parse_all_policy_documents


async def main():
    print("=" * 60)
    print("🧪 Testing parse_all_policy_documents Tool")
    print("=" * 60)
    
    # Use default paths (policy_docs/policy_pdfs -> policy_docs/combined_policy.md)
    result = await parse_all_policy_documents()
    
    print("\n📋 Result:")
    print("-" * 60)
    print(result)
    print("-" * 60)
    
    # Verify output file exists
    output_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "policy_docs",
        "combined_policy.md"
    )
    
    if os.path.exists(output_path):
        print(f"\n✅ Output file created: {output_path}")
        with open(output_path, "r", encoding="utf-8") as f:
            content = f.read()
        print(f"   Total lines: {len(content.splitlines())}")
        print(f"   Total characters: {len(content)}")
        print("\n📝 First 1000 characters:")
        print("=" * 60)
        print(content[:1000])
        print("=" * 60)
    else:
        print(f"\n❌ Output file not found: {output_path}")


if __name__ == "__main__":
    asyncio.run(main())

