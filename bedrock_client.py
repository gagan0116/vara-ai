# bedrock_client.py
"""
Shared Amazon Nova client for VARA AI.
Provides utilities for interacting with Amazon Nova API using the openai SDK,
while maintaining backward compatibility with the Bedrock Converse API structure used by the app.
"""

import os
import json
import time
import asyncio
import base64
import random
from typing import Any, Dict, List, Optional, Union

from openai import OpenAI, APIError, RateLimitError, APITimeoutError
from dotenv import load_dotenv

load_dotenv()

# =============================================================================
# MODEL CONSTANTS
# =============================================================================

# Amazon Nova model IDs (Native API)
NOVA_PRO = os.getenv("NOVA_MODEL_PRO", "nova-2-pro-v1")
NOVA_LITE = os.getenv("NOVA_MODEL_LITE", "nova-2-lite-v1")

# Clean prefixes if they were left over from Bedrock (.env update mismatch)
if "us.amazon." in NOVA_PRO:
    NOVA_PRO = NOVA_PRO.replace("us.amazon.", "")
    if NOVA_PRO.endswith(":0"):
        NOVA_PRO = NOVA_PRO[:-2]
        
if "us.amazon." in NOVA_LITE:
    NOVA_LITE = NOVA_LITE.replace("us.amazon.", "")
    if NOVA_LITE.endswith(":0"):
        NOVA_LITE = NOVA_LITE[:-2]


# Legacy aliases for backward compatibility
MODEL_PRO = NOVA_PRO
MODEL_LITE = NOVA_LITE

# Default config
DEFAULT_MAX_TOKENS = 4096
DEFAULT_TEMPERATURE = 1.0

# Retry settings
MAX_RETRIES = 5
BASE_DELAY = 2.0
MAX_DELAY = 60.0


# =============================================================================
# CLIENT MANAGEMENT
# =============================================================================

_openai_client = None

def get_bedrock_client():
    """
    Get or create the OpenAI client configured for Amazon Nova.
    Returns:
        OpenAI API client
    """
    global _openai_client
    if _openai_client is None:
        api_key = os.getenv("AWS_BEARER_TOKEN_BEDROCK", os.getenv("NOVA_API_KEY"))
        if not api_key:
            raise ValueError("NOVA_API_KEY or AWS_BEARER_TOKEN_BEDROCK must be set for Nova Native API.")
            
        _openai_client = OpenAI(
            api_key=api_key,
            base_url="https://api.nova.amazon.com/v1",
            timeout=300.0
        )
    return _openai_client


def reset_bedrock_client():
    """Reset the client."""
    global _openai_client
    _openai_client = None


# =============================================================================
# MESSAGE FORMATTING HELPERS (Maintaining Bedrock Format compatibility)
# =============================================================================

def _make_text_content(text: str) -> Dict:
    """Create a text content block."""
    return {"text": text}


def _make_image_content(image_bytes: bytes, media_type: str = "image/jpeg") -> Dict:
    """
    Create an image content block for Bedrock Converse API.
    """
    fmt = media_type.split("/")[-1]
    if fmt == "jpg":
        fmt = "jpeg"
    
    return {
        "image": {
            "format": fmt,
            "source": {
                "bytes": image_bytes
            }
        }
    }


def _make_user_message(content: Union[str, List[Dict]]) -> Dict:
    """Create a user message."""
    if isinstance(content, str):
        content = [_make_text_content(content)]
    return {"role": "user", "content": content}


def _make_assistant_message(content: Union[str, List[Dict]]) -> Dict:
    """Create an assistant message."""
    if isinstance(content, str):
        content = [_make_text_content(content)]
    return {"role": "assistant", "content": content}


def _format_system_prompt(system_text: str) -> List[Dict]:
    """Format system prompt for Bedrock Converse API."""
    return [{"text": system_text}]


# =============================================================================
# CORE API CALLS
# =============================================================================

def generate_content(
    model_id: str,
    messages: List[Dict],
    system_prompt: Optional[str] = None,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    temperature: float = DEFAULT_TEMPERATURE,
    stop_sequences: Optional[List[str]] = None,
    tool_config: Optional[Dict] = None,
) -> Dict[str, Any]:
    """
    Call Native Amazon Nova API synchronously, adapting Bedrock format to OpenAI format.
    """
    client = get_bedrock_client()
    
    # Map Bedrock messages to OpenAI messages
    openai_messages = []
    
    if system_prompt:
        openai_messages.append({"role": "system", "content": system_prompt})
        
    for msg in messages:
        role = msg.get("role", "user")
        content_blocks = msg.get("content", [])
        
        openai_content = []
        for block in content_blocks:
            if "text" in block:
                openai_content.append({"type": "text", "text": block["text"]})
            elif "image" in block:
                fmt = block["image"].get("format", "jpeg")
                raw_bytes = block["image"]["source"]["bytes"]
                b64_data = base64.b64encode(raw_bytes).decode('utf-8')
                openai_content.append({
                    "type": "image_url",
                    "image_url": {
                        "url": f"data:image/{fmt};base64,{b64_data}"
                    }
                })
                
        openai_messages.append({"role": role, "content": openai_content})
    
    kwargs = {
        "model": model_id,
        "messages": openai_messages,
        "max_tokens": max_tokens,
        "temperature": temperature,
    }
    
    # Execute OpenAI SDK call
    response = client.chat.completions.create(**kwargs)
    
    # Map back to Bedrock Converse format
    response_text = response.choices[0].message.content or ""
    
    return {
        "output": {
            "message": {
                "content": [{"text": response_text}]
            }
        }
    }


async def async_generate_content(
    model_id: str,
    messages: List[Dict],
    system_prompt: Optional[str] = None,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    temperature: float = DEFAULT_TEMPERATURE,
    stop_sequences: Optional[List[str]] = None,
    tool_config: Optional[Dict] = None,
) -> Dict[str, Any]:
    """
    Call Bedrock Converse API asynchronously (runs sync call in executor).
    """
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        None,
        lambda: generate_content(
            model_id=model_id,
            messages=messages,
            system_prompt=system_prompt,
            max_tokens=max_tokens,
            temperature=temperature,
            stop_sequences=stop_sequences,
            tool_config=tool_config,
        )
    )


# =============================================================================
# RETRY WRAPPERS
# =============================================================================

def _is_retryable_error(error: Exception) -> bool:
    """Check if an error is retryable."""
    if isinstance(error, (RateLimitError, APITimeoutError)):
        return True
    if isinstance(error, APIError):
        return error.status_code in [429, 500, 502, 503, 504]
    return any(kw in str(error).lower() for kw in ["throttl", "429", "503", "overloaded", "unavailable", "timeout"])


def generate_with_retry(
    model_id: str,
    messages: List[Dict],
    system_prompt: Optional[str] = None,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    temperature: float = DEFAULT_TEMPERATURE,
    max_retries: int = MAX_RETRIES,
    **kwargs,
) -> Dict[str, Any]:
    """Generate content with automatic retry and exponential backoff."""
    last_error = None
    for attempt in range(max_retries):
        try:
            return generate_content(
                model_id=model_id,
                messages=messages,
                system_prompt=system_prompt,
                max_tokens=max_tokens,
                temperature=temperature,
                **kwargs,
            )
        except Exception as e:
            last_error = e
            if _is_retryable_error(e) and attempt < max_retries - 1:
                delay = min(BASE_DELAY * (2 ** attempt) + random.uniform(0, 1), MAX_DELAY)
                print(f"⚠️ Nova API error (attempt {attempt + 1}/{max_retries}): {str(e)[:80]}")
                print(f"   Retrying in {delay:.1f}s...")
                time.sleep(delay)
            else:
                raise
    raise last_error


async def async_generate_with_retry(
    model_id: str,
    messages: List[Dict],
    system_prompt: Optional[str] = None,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    temperature: float = DEFAULT_TEMPERATURE,
    max_retries: int = MAX_RETRIES,
    **kwargs,
) -> Dict[str, Any]:
    """Async version of generate_with_retry."""
    last_error = None
    for attempt in range(max_retries):
        try:
            return await async_generate_content(
                model_id=model_id,
                messages=messages,
                system_prompt=system_prompt,
                max_tokens=max_tokens,
                temperature=temperature,
                **kwargs,
            )
        except Exception as e:
            last_error = e
            if _is_retryable_error(e) and attempt < max_retries - 1:
                delay = min(BASE_DELAY * (2 ** attempt) + random.uniform(0, 1), MAX_DELAY)
                print(f"⚠️ Nova API error (attempt {attempt + 1}/{max_retries}): {str(e)[:80]}")
                print(f"   Retrying in {delay:.1f}s...")
                await asyncio.sleep(delay)
            else:
                raise
    raise last_error


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def extract_text_from_response(response: Dict) -> str:
    """Extract the text content from a mapped Bedrock response dict."""
    output = response.get("output", {})
    message = output.get("message", {})
    content = message.get("content", [])
    
    for block in content:
        if "text" in block:
            return block["text"]
    return ""


def generate_json_content(
    model_id: str,
    prompt: str,
    system_prompt: Optional[str] = None,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    temperature: float = DEFAULT_TEMPERATURE,
    max_retries: int = MAX_RETRIES,
) -> Dict[str, Any]:
    """Generate content and parse as JSON."""
    json_system = system_prompt or ""
    if "json" not in json_system.lower():
        json_system += "\n\nIMPORTANT: You MUST respond with valid JSON only. No markdown, no extra text."
    
    messages = [_make_user_message(prompt)]
    
    response = generate_with_retry(
        model_id=model_id,
        messages=messages,
        system_prompt=json_system,
        max_tokens=max_tokens,
        temperature=temperature,
        max_retries=max_retries,
    )
    
    text = extract_text_from_response(response)
    
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        import re
        json_match = re.search(r'```(?:json)?\s*\n?(.*?)\n?```', text, re.DOTALL)
        if json_match:
            return json.loads(json_match.group(1))
        json_match = re.search(r'\{.*\}', text, re.DOTALL)
        if json_match:
            return json.loads(json_match.group())
        raise ValueError(f"Failed to parse JSON from response: {text[:500]}")


async def async_generate_json_content(
    model_id: str,
    prompt: str,
    system_prompt: Optional[str] = None,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    temperature: float = DEFAULT_TEMPERATURE,
    max_retries: int = MAX_RETRIES,
) -> Dict[str, Any]:
    """Async version of generate_json_content."""
    json_system = system_prompt or ""
    if "json" not in json_system.lower():
        json_system += "\n\nIMPORTANT: You MUST respond with valid JSON only. No markdown, no extra text."
    
    messages = [_make_user_message(prompt)]
    
    response = await async_generate_with_retry(
        model_id=model_id,
        messages=messages,
        system_prompt=json_system,
        max_tokens=max_tokens,
        temperature=temperature,
        max_retries=max_retries,
    )
    text = extract_text_from_response(response)
    
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        import re
        json_match = re.search(r'```(?:json)?\s*\n?(.*?)\n?```', text, re.DOTALL)
        if json_match:
            return json.loads(json_match.group(1))
        json_match = re.search(r'\{.*\}', text, re.DOTALL)
        if json_match:
            return json.loads(json_match.group())
        raise ValueError(f"Failed to parse JSON from response: {text[:500]}")
