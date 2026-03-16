import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from bedrock_client import (
    NOVA_LITE,
    generate_with_retry,
    extract_text_from_response,
    _make_user_message,
)

CONFIDENCE_THRESHOLD = 0.75

CLASSIFICATION_PROMPT = """
You are an email classification system.

Classify the email into ONE category:
- RETURN
- REPLACEMENT
- REFUND
- NONE

Rules:
- RETURN: customer wants to return a product
- REPLACEMENT: damaged / defective / wrong item
- REFUND: wants money back
- NONE: unrelated

Extract user_id ONLY if explicitly mentioned.
Give a confidence score between 0.0 and 1.0.

Respond ONLY in valid JSON matching this schema exactly:
{
    "category": "string - one of RETURN, REPLACEMENT, REFUND, NONE",
    "user_id": "string or null",
    "confidence": "number - between 0.0 and 1.0"
}

Email Subject:
{subject}

Email Body:
{body}
"""

def classify_email(subject, body):
    body = body[:4000]
    prompt = CLASSIFICATION_PROMPT.format(subject=subject, body=body)
    
    system_prompt = "You are an email classification AI. Respond strictly with a JSON object."
    messages = [_make_user_message(prompt)]

    try:
        response = generate_with_retry(
            model_id=NOVA_LITE,
            messages=messages,
            system_prompt=system_prompt,
            max_tokens=512,
            temperature=1.0,  # Keeping requested temperature 1.0
            max_retries=3
        )

        text = extract_text_from_response(response)
        
        # Cleanup markdown formatting
        import re
        json_match = re.search(r'```(?:json)?\s*\n?(.*?)\n?```', text, re.DOTALL)
        if json_match:
            text = json_match.group(1)
            
        result = json.loads(text)
        
        if isinstance(result, list):
            if not result:
                result = {}
            else:
                result = result[0]
                
        return {
            "category": result.get("category", "NONE"),
            "user_id": result.get("user_id"),
            "confidence": float(result.get("confidence", 0.0))
        }
    except Exception as e:
        print(f"Error classifying email: {e}")
        return {
            "category": "NONE",
            "user_id": None,
            "confidence": 0.0
        }
