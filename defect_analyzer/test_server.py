"""
Test script for Defect Analyzer MCP Server.
Tests the analyze_defect_image tool with a sample image.
"""
import asyncio
import os
import sys
import argparse

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from mcp_server import analyze_defect_image


async def main():
    parser = argparse.ArgumentParser(description="Test defect image analysis")
    parser.add_argument("--image", "-i", required=True, help="Path to the image file to analyze")
    args = parser.parse_args()
    
    print("=" * 60)
    print("🔍 Testing Defect Image Analysis")
    print("=" * 60)
    print(f"📁 Image: {args.image}")
    print("-" * 60)
    
    if not os.path.exists(args.image):
        print(f"❌ Error: File not found: {args.image}")
        return
    
    result = await analyze_defect_image(image_path=args.image)
    
    print("\n📋 Analysis Result:")
    print("-" * 60)
    print(result)
    print("-" * 60)


if __name__ == "__main__":
    asyncio.run(main())
