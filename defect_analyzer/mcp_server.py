# defect_analyzer/mcp_server.py
"""
Defect Analyzer MCP Server - Analyzes product defect images using Amazon Nova Vision.
Uses Amazon Bedrock converse API for multimodal image analysis.
"""

import os
import base64
import json
import sys
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from bedrock_client import (
    NOVA_PRO,
    generate_content,
    extract_text_from_response,
)

load_dotenv()

mcp = FastMCP("defect_analyzer")

ANALYSIS_PROMPT = """You are an expert product defect analyst for furniture and appliances.

Analyze this image and provide a ONE-LINE description of any visible defects. Look very closely at all structural components (armrests, legs, wheels, backrests).

RULES:
1. Be concise - maximum ONE sentence
2. Describe the defect type and location clearly
3. If you see a structural break, crack, or missing piece anywhere, you MUST call it out.
4. If there is absolutely no visible defect, say: "No visible defect detected"

Examples of good responses:
- "The right armrest is completely broken off near the mounting point."
- "Cracked screen with fracture lines extending from the top-left corner"
- "Deep scratch marks across the back panel of the device"

Respond with ONLY the one-line description, nothing else."""


@mcp.tool()
async def analyze_defect_image(
    image_path: str = None,
    image_base64: str = None
) -> str:
    """
    Analyzes a product defect image using Amazon Nova Vision and returns a one-line description.
    
    Args:
        image_path: Local file path to the image (e.g., "C:/images/defect.jpg")
        image_base64: Base64 encoded image string (alternative to image_path)
        
    Returns:
        JSON string with defect description and status.
        Example: {"description": "Cracked screen on the display", "status": "success"}
    """
    if not image_path and not image_base64:
        return json.dumps({
            "description": "Error: No image provided. Please provide image_path or image_base64.",
            "status": "error"
        })
    
    try:
        # Prepare the image
        if image_path:
            if not os.path.exists(image_path):
                return json.dumps({
                    "description": f"Error: File not found at {image_path}",
                    "status": "error"
                })
            
            # Read the image
            with open(image_path, "rb") as f:
                image_data = f.read()
            
            # Determine mime type
            ext = os.path.splitext(image_path)[1].lower()
            mime_types = {
                ".jpg": "image/jpeg",
                ".jpeg": "image/jpeg",
                ".png": "image/png",
                ".gif": "image/gif",
                ".webp": "image/webp"
            }
            mime_type = mime_types.get(ext, "image/jpeg")
        else:
            # Use base64 image
            image_data = base64.b64decode(image_base64)
            mime_type = "image/jpeg"
        
        # Encode to base64 for Bedrock
        image_b64 = base64.b64encode(image_data).decode("utf-8")
        
        # Build multimodal message for Bedrock converse API
        model_id = os.getenv("DEFECT_MODEL", NOVA_PRO)
        
        messages = [
            {
                "role": "user",
                "content": [
                    {
                        "image": {
                            "format": mime_type.split("/")[1],  # "jpeg", "png", etc.
                            "source": {
                                "bytes": image_data
                            }
                        }
                    },
                    {
                        "text": ANALYSIS_PROMPT
                    }
                ]
            }
        ]
        
        # Call Bedrock converse API wrapper
        response = generate_content(
            model_id=model_id,
            messages=messages,
            max_tokens=256,
            temperature=0.0
        )
        
        description = extract_text_from_response(response).strip()
        
        # Determine status
        if "human review required" in description.lower():
            status = "human_review_required"
            description = "Human review required"
        elif "error" in description.lower():
            status = "error"
        else:
            status = "success"
        print(f"Defect analysis result: {description}, status: {status}")
        return json.dumps({
            "description": description,
            "status": status
        })
        
    except Exception as e:
        return json.dumps({
            "description": "Human review required",
            "status": "human_review_required",
            "error_details": str(e)
        })


if __name__ == "__main__":
    mcp.run(transport='stdio')
