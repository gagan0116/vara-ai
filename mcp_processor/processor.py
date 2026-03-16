import asyncio
import sys
import os
import json
import uuid
from datetime import datetime, timezone
from contextlib import AsyncExitStack
from typing import Dict, Any, Optional
from google.cloud import storage

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from dotenv import load_dotenv
from bedrock_client import (
    NOVA_PRO,
    NOVA_LITE,
    generate_content,
    async_generate_with_retry,
    extract_text_from_response,
)

# Load environment variables
load_dotenv()


current_dir = os.path.dirname(os.path.abspath(__file__))

PROJECT_ROOT = os.getenv("PROJECT_ROOT", current_dir)
sys.path.append(PROJECT_ROOT)

try:
    from policy_compiler_agents.adjudicator_agent import Adjudicator
    from db_verification.db import db_connection
except ImportError:
    # Fallback for local testing if running from subdirectory
    sys.path.append(os.path.dirname(current_dir))
    from policy_compiler_agents.adjudicator_agent import Adjudicator
    from db_verification.db import db_connection


# Nova Client using bedrock_client
api_key = os.getenv("NOVA_API_KEY") or os.getenv("AWS_BEARER_TOKEN_BEDROCK")
if not api_key:
    print("Warning: NOVA_API_KEY / AWS API KEY not set in environment.")

def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    
    directory = os.path.dirname(destination_file_name)
    if directory:
        os.makedirs(directory, exist_ok=True)
        
    blob.download_to_filename(destination_file_name)
    print(f"Downloaded {source_blob_name} to {destination_file_name}")

import random

class MCPProcessor:
    def __init__(self):
        self.exit_stack = AsyncExitStack()
        self.sessions: Dict[str, ClientSession] = {}
        # Semaphore to limit concurrent Nova API calls (prevents 429 errors)
        self.sem = asyncio.Semaphore(5)
        
        # Configuration for MCP servers
        # We assume the server scripts are copied to the container root
        self.server_configs = {
            "doc_server": {
                "command": sys.executable,
                "args": [os.path.join(PROJECT_ROOT, "doc_server", "mcp_doc_server.py")],
                "env": os.environ.copy()  # Inherit all env vars including NOVA_API_KEY
            },
            "db_verification": {
                "command": sys.executable,
                "args": ["-m", "db_verification.db_verification_server"],
                "env": {**os.environ, "PYTHONPATH": PROJECT_ROOT}  # Inherit all env vars + set PYTHONPATH
            },
            "defect_analyzer": {
                "command": sys.executable,
                "args": [os.path.join(PROJECT_ROOT, "defect_analyzer", "mcp_server.py")],
                "env": os.environ.copy()  # Inherit all env vars including NOVA_API_KEY
            }
        }
    
    async def generate_with_retry(self, model, contents, system_prompt=None, max_retries=10, temperature=1.0):
        """
        Generate content with retry logic, rate limiting, and exponential backoff with jitter.
        """
        async with self.sem:  # Limit concurrent API calls
            response = await async_generate_with_retry(
                model_id=model,
                messages=[{"role": "user", "content": [{"text": contents}]}],
                system_prompt=system_prompt,
                max_tokens=4000,
                temperature=temperature,
            )
            return response

    def insert_refund_case(self, email_data, extracted_data, verified_record, adjudication_result=None):
        # ... logic from client.py ...
        # Copied verbatim from client.py insert_refund_case to save space in thought process
        # but I must write full code to file.
        try:
            case_id = str(uuid.uuid4())
            now = datetime.now(timezone.utc)
            
            from_email = email_data.get("user_id", "")
            received_at_str = email_data.get("received_at")
            received_at = datetime.fromisoformat(received_at_str.replace("Z", "+00:00")) if received_at_str else now
            classification = email_data.get("category", "UNKNOWN")
            confidence = email_data.get("confidence")
            email_body = email_data.get("email_body", "")
            attachments = email_data.get("attachments", [])
            
            source_message_id = email_data.get("message_id") or f"{from_email}_{received_at.strftime('%Y%m%dT%H%M%SZ')}_{case_id[:8]}"
            
            from_name = extracted_data.get("full_name")
            extracted_invoice_number = extracted_data.get("invoice_number")
            extracted_order_invoice_id = extracted_data.get("order_invoice_id")
            
            customer_id = None
            order_id = None
            if verified_record:
                data_section = verified_record.get("data", {})
                if data_section:
                    customer_info = data_section.get("customer", {})
                    order_details = data_section.get("order_details", {})
                    customer_id = customer_info.get("customer_id")
                    order_id = order_details.get("order_id")
                if not customer_id:
                    customer_id = verified_record.get("customer_id")
                if not order_id:
                    order_id = verified_record.get("order_id")
            
            if verified_record:
                verification_status = "VERIFIED"
            else:
                verification_status = "PENDING_REVIEW"
            
            verification_notes = None
            if adjudication_result:
                decision = adjudication_result.get("decision", "")
                reason = adjudication_result.get("details", {}).get("reason", "")
                verification_notes = f"Decision: {decision}. {reason}"
            
            metadata = {
                "extraction_confidence": extracted_data.get("confidence_score"),
                "return_reason_category": extracted_data.get("return_reason_category"),
                "return_reason": extracted_data.get("return_reason"),
                "item_condition": extracted_data.get("item_condition"),
            }
            if adjudication_result:
                metadata["adjudication"] = adjudication_result
            
            attachments_json = [
                {"filename": att.get("filename"), "mimeType": att.get("mimeType")}
                for att in attachments
            ] if attachments else None
            
            insert_sql = """
                INSERT INTO refund_cases (
                    case_id, case_source, source_message_id, received_at,
                    from_email, from_name, subject, body,
                    customer_id, order_id,
                    extracted_invoice_number, extracted_order_invoice_id,
                    classification, confidence,
                    verification_status, verification_notes,
                    attachments, metadata,
                    created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s
                )
                ON CONFLICT (source_message_id) DO UPDATE SET
                    customer_id = EXCLUDED.customer_id,
                    order_id = EXCLUDED.order_id,
                    verification_status = EXCLUDED.verification_status,
                    verification_notes = EXCLUDED.verification_notes,
                    metadata = EXCLUDED.metadata,
                    updated_at = EXCLUDED.updated_at
                RETURNING case_id
            """
            
            params = (
                case_id, "EMAIL", source_message_id, received_at,
                from_email, from_name, None, email_body,
                customer_id, order_id,
                extracted_invoice_number, extracted_order_invoice_id,
                classification, confidence,
                verification_status, verification_notes,
                json.dumps(attachments_json) if attachments_json else None,
                json.dumps(metadata), now, now
            )
            
            with db_connection() as conn:
                cur = conn.cursor()
                cur.execute(insert_sql, params)
                result = cur.fetchone()
                conn.commit()
                returned_case_id = result[0] if result else case_id
                print(f"✅ Refund case inserted: {returned_case_id}")
                return returned_case_id
                
        except Exception as e:
            print(f"❌ Error inserting refund case: {e}")
            import traceback
            traceback.print_exc()
            raise # Re-raise to ensure task failure implies retry

    async def connect_to_all_servers(self):
        for name, config in self.server_configs.items():
            print(f"Connecting to {name}...")
            try:
                server_params = StdioServerParameters(**config)
                stdio_transport = await self.exit_stack.enter_async_context(stdio_client(server_params))
                read, write = stdio_transport
                session = await self.exit_stack.enter_async_context(ClientSession(read, write))
                await session.initialize()
                self.sessions[name] = session
                print(f"✅ Connected to {name}")
            except Exception as e:
                print(f"❌ Error connecting to {name}: {e}")
                raise

    async def extract_order_details(self, combined_text):
        prompt = f"""You are an expert data extraction agent.
Analyze the following customer support email and its attached invoice content.
Extract all available details and output ONLY a raw JSON object conforming EXACTLY to the structure requested below. Do not wrap it in markdown. If a field is not found, use null or omit it.

REQUIRED JSON STRUCTURE:
{{
  "customer_email": "string (Sender's email address)",
  "full_name": "string (Customer full name)",
  "phone": "string (Customer phone number)",
  "invoice_number": "string (Invoice number)",
  "order_invoice_id": "string (Order/Invoice ID)",
  "order_date": "string (Order date in YYYY-MM-DD format)",
  "return_request_date": "string (Date email was received)",
  "ship_mode": "string (Shipping method)",
  "ship_city": "string (Shipping city)",
  "ship_state": "string (Shipping state)",
  "ship_country": "string (Shipping country)",
  "currency": "string (Currency code e.g. USD)",
  "discount_amount": number (Discount applied),
  "shipping_amount": number (Shipping cost),
  "total_amount": number (Order total),
  "order_items": [
    {{
      "sku": "string (Product SKU)",
      "item_name": "string (Product name)",
      "category": "string (Product category)",
      "subcategory": "string (Product subcategory)",
      "quantity": integer (Quantity ordered),
      "unit_price": number (Price per unit),
      "line_total": number (Total for this line item)
    }}
  ],
  "item_condition": "string (Must be ONE OF: NEW_UNOPENED, OPENED_LIKE_NEW, DAMAGED_DEFECTIVE, MISSING_PARTS, or UNKNOWN)",
  "return_category": "string (Must be ONE OF: RETURN, REPLACEMENT, or REFUND)",
  "return_reason_category": "string (Must be ONE OF: CHANGED_MIND, DEFECTIVE, WRONG_ITEM_SENT, ARRIVED_LATE, or OTHER)",
  "return_reason": "string (Detailed summary of return reason)",
  "confidence_score": number (Extraction confidence 0.0 to 1.0)
}}

INPUT TEXT TO EXTRACT FROM:
{combined_text}
"""

        try:
            response = await self.generate_with_retry(
                model=NOVA_PRO,
                contents=prompt,
                temperature=0.0
            )
            raw_text = extract_text_from_response(response)
            
            # Clean up markdown if model incorrectly adds it
            import re
            json_match = re.search(r'```(?:json)?\s*\n?(.*?)\n?```', raw_text, re.DOTALL)
            if json_match:
                return json_match.group(1).strip()
            
            # Additional safety net: try to find the start of the JSON object
            start_idx = raw_text.find('{{')
            end_idx = raw_text.rfind('}}')
            if start_idx != -1 and end_idx != -1:
                return raw_text[start_idx:end_idx+1]
                
            return raw_text
        except Exception as e:
            return f'{{"error": "LLM Extraction failed: {str(e)}"}}'

    async def verify_request_with_db(self, extracted_data):
        """
        Agentic verification flow loops using Nova to interpret tool outputs and decide next steps.
        """
        db_session = self.sessions.get("db_verification")
        if not db_session:
            print("❌ Error: db_verification session not available.")
            return None

        print("\n" + "="*40)
        print("DATABASE VERIFICATION (AGENT LOOP)")
        print("="*40)

        tools_response = await db_session.list_tools()
        tools_map = {t.name: t for t in tools_response.tools}
        tools_desc = []
        for t in tools_response.tools:
            tools_desc.append({
                "name": t.name,
                "description": t.description,
                "parameters": t.inputSchema
            })

        messages = [
    """
            You are an expert DB Verification Agent. Your goal is to verify a customer refund request.
            
            STRICT VERIFICATION PROCESS (Follow in order):
            
            STEP 1: IDENTITY CHECK
            - Call 'verify_from_email_matches_customer' with the customer_email.
            - IF 'matched' is False: Call llm_find_orders. If llm_find_orders returns rows, include the first row in 'verified_data'. Output "Request sent for Human Review" and terminate.
            - IF 'matched' is True: Proceed to Step 2.
            
            STEP 2: FIND ORDER (Hierarchical Search)
            - ATTEMPT 1: If 'order_invoice_id' exists in data, call 'find_order_by_order_invoice_id'.
              - If found, you are DONE. Return the order details.
            - ATTEMPT 2: If finding by ID failed or ID was missing, check if 'invoice_number' exists in data.
              - If yes, call 'find_order_by_invoice_number'.
              - If found, you are DONE.
            - ATTEMPT 3: If specific searches fail, call 'get_customer_orders_with_items' to get a list of recent orders.
              - Then immediately call 'select_order_id' passing that usage data to pick the best one.
              - If a 'selected_order_id' is returned, specific logic to confirm it? No, just accept the selection.
            - ATTEMPT 4: If all else fails, call 'llm_find_orders' to search via SQL.
            
            STEP 3: REPORT
            - If an order is found in any step, output "Verification Successful" and ensure you copy the full order JSON into 'verified_data'.
            - If completely stuck after all attempts, output "Sending for Human Review".
            
            INSTRUCTIONS:
            - Decide the NEXT SINGLE Action.
            - Output JSON ONLY: { "tool_name": "...", "arguments": { ... } }
            - If you are done or need to stop, output JSON: { "action": "terminate", "reason": "...", "verified_data": object|null }
              (If verification was successful, you MUST include the full retrieved order details in the 'verified_data' field).
            """
]
        context_str = f"EXTRACTED DATA:\n{json.dumps(extracted_data, indent=2)}\n\nAVAILABLE TOOLS:\n{json.dumps(tools_desc)}"
        messages.append(context_str)

        max_turns = 8
        fuzzy_tools_used = []  # Track if llm_find_orders or select_order_id were used
        for i in range(max_turns):
            print(f"\n--- Turn {i+1} ---")
            
            # Rate limiting sleep
            await asyncio.sleep(2)
            
            prompt_content = "\n".join(messages) + "\n\nWhat is the next step? Output valid JSON only."
            
            system_prompt_text = "You are a tool-calling DB Verification agent. Return strictly JSON data according to the schema in the instructions, and absolutely no other text."
            try:
                response = await self.generate_with_retry(
                    model=NOVA_LITE, 
                    contents=prompt_content,
                    system_prompt=system_prompt_text,
                    temperature=1.0
                )
                
                decision_text = extract_text_from_response(response)
                
                # Handle None or empty response from Nova
                if decision_text is None or decision_text.strip() == "":
                    print(f"⚠️ Empty response from LLM on turn {i+1}. Retrying...")
                    messages.append("System: Your previous response was empty. Please provide a valid JSON response.")
                    continue
                
                print(f"🤖 Agent thought: {decision_text}")
                
                try:
                    import re
                    # Cleanup markdown codeblocks if present
                    json_match = re.search(r'```(?:json)?\s*\n?(.*?)\n?```', decision_text, re.DOTALL)
                    if json_match:
                        decision_text = json_match.group(1)
                    else:
                        json_match = re.search(r'\{.*\}', decision_text, re.DOTALL)
                        if json_match:
                            decision_text = json_match.group()
                    
                    decision = json.loads(decision_text)
                except json.JSONDecodeError as json_err:
                    print(f"⚠️ Failed to parse JSON response: {json_err}")
                    messages.append(f"System: Your response was not valid JSON. Error: {json_err}. Please output valid JSON only.")
                    continue

                if "action" in decision and decision["action"] == "terminate":
                    print(f"🏁 Agent Finished: {decision.get('reason')}")
                    return {
                        "verified_data": decision.get("verified_data"),
                        "fuzzy_tools_used": fuzzy_tools_used
                    }
                
                tool_name = decision.get("tool_name")
                args = decision.get("arguments", {})
                
                if not tool_name:
                    print(f"⚠️ Invalid decision format. Stopping.")
                    break

                # Validations before calling
                if tool_name not in tools_map:
                    print(f"❌ Error: Tool {tool_name} not found.")
                    messages.append(f"System: Tool {tool_name} does not exist. Choose from available tools.")
                    continue

                # Execute Tool
                print(f"▶️ Executing: {tool_name}...")
                
                # Track fuzzy matching tools
                if tool_name in ["llm_find_orders", "select_order_id"]:
                    fuzzy_tools_used.append(tool_name)
                
                result = await db_session.call_tool(tool_name, arguments=args)
                tool_output_str = result.content[0].text
                
                # Print snippet for user
                display_output = tool_output_str[:500] + "..." if len(tool_output_str) > 500 else tool_output_str
                print(f"📄 Output: {display_output}")
                
                # Feed result back to context
                messages.append(f"Tool '{tool_name}' Result:\n{tool_output_str}")
                
            except Exception as e:
                print(f"❌ Error in Agent Loop: {e}")
                break
        
        return None

    async def verify_request_with_db_streaming(self, extracted_data):
        """
        Streaming version of verify_request_with_db that yields sub-step events.
        Yields events for each MCP tool call in the agent loop.
        
        Yields dict events: {"substep": str, "status": str, "log": str, "data": dict}
        Final event: {"substep": "FINAL", "status": "complete", "data": {...}}
        """
        db_session = self.sessions.get("db_verification")
        if not db_session:
            print("❌ Error: db_verification session not available.")
            yield {"substep": "error", "status": "error", "log": "DB session not available", "data": None}
            yield {"substep": "FINAL", "status": "complete", "data": None}
            return

        yield {"substep": "init", "status": "active", "log": "Initializing agent loop...", "data": None}
        
        print("\n" + "="*40)
        print("DATABASE VERIFICATION (AGENT LOOP - STREAMING)")
        print("="*40)

        tools_response = await db_session.list_tools()
        tools_map = {t.name: t for t in tools_response.tools}
        tools_desc = []
        for t in tools_response.tools:
            tools_desc.append({
                "name": t.name,
                "description": t.description,
                "parameters": t.inputSchema
            })

        yield {"substep": "init", "status": "complete", "log": f"Loaded {len(tools_desc)} MCP tools", "data": {"tools_count": len(tools_desc)}}

        messages = [
"""
        You are an expert DB Verification Agent. Your goal is to verify a customer refund request.
        
        STRICT VERIFICATION PROCESS (Follow in order):
        
        STEP 1: IDENTITY CHECK
        - Call 'verify_from_email_matches_customer' with the customer_email.
        - IF 'matched' is False: Call llm_find_orders. If llm_find_orders returns rows, include the first row in 'verified_data'. Output "Request sent for Human Review" and terminate.
        - IF 'matched' is True: Proceed to Step 2.
        
        STEP 2: FIND ORDER (Hierarchical Search)
        - ATTEMPT 1: If 'order_invoice_id' exists in data, call 'find_order_by_order_invoice_id'.
          - If found, you are DONE. Return the order details.
        - ATTEMPT 2: If finding by ID failed or ID was missing, check if 'invoice_number' exists in data.
          - If yes, call 'find_order_by_invoice_number'.
          - If found, you are DONE.
        - ATTEMPT 3: If specific searches fail, call 'get_customer_orders_with_items' to get a list of recent orders.
          - Then immediately call 'select_order_id' passing that usage data to pick the best one.
          - If a 'selected_order_id' is returned, specific logic to confirm it? No, just accept the selection.
        - ATTEMPT 4: If all else fails, call 'llm_find_orders' to search via SQL.
        
        STEP 3: REPORT
        - If an order is found in any step, output "Verification Successful" and ensure you copy the full order JSON into 'verified_data'.
        - If completely stuck after all attempts, output "Sending for Human Review".
        
        INSTRUCTIONS:
        - Decide the NEXT SINGLE Action.
        - Output JSON ONLY: { "tool_name": "...", "arguments": { ... } }
        - If you are done or need to stop, output JSON: { "action": "terminate", "reason": "...", "verified_data": object|null }
          (If verification was successful, you MUST include the full retrieved order details in the 'verified_data' field).
        """
]
        context_str = f"EXTRACTED DATA:\n{json.dumps(extracted_data, indent=2)}\n\nAVAILABLE TOOLS:\n{json.dumps(tools_desc)}"
        messages.append(context_str)

        max_turns = 8
        fuzzy_tools_used = []
        tool_call_count = 0
        
        for i in range(max_turns):
            print(f"\n--- Turn {i+1} ---")
            
            # Rate limiting sleep
            await asyncio.sleep(2)
            
            prompt_content = "\n".join(messages) + "\n\nWhat is the next step? Output valid JSON only."
            
            yield {"substep": "llm_think", "status": "active", "log": f"Agent thinking (turn {i+1})...", "data": None}
            
            system_prompt_text = "You are a tool-calling DB Verification agent. Return strictly JSON data according to the schema in the instructions, and absolutely no other text."
            try:
                response = await self.generate_with_retry(
                    model=NOVA_LITE, 
                    contents=prompt_content,
                    system_prompt=system_prompt_text,
                    temperature=1.0
                )
                
                decision_text = extract_text_from_response(response)
                
                # Handle None or empty response from Nova
                if decision_text is None or decision_text.strip() == "":
                    print(f"⚠️ Empty response from LLM on turn {i+1}. Retrying...")
                    messages.append("System: Your previous response was empty. Please provide a valid JSON response.")
                    continue
                
                print(f"🤖 Agent thought: {decision_text}")
                
                try:
                    import re
                    # Cleanup markdown codeblocks if present
                    json_match = re.search(r'```(?:json)?\s*\n?(.*?)\n?```', decision_text, re.DOTALL)
                    if json_match:
                        decision_text = json_match.group(1)
                    else:
                        json_match = re.search(r'\{.*\}', decision_text, re.DOTALL)
                        if json_match:
                            decision_text = json_match.group()
                            
                    decision = json.loads(decision_text)
                except json.JSONDecodeError as json_err:
                    print(f"⚠️ Failed to parse JSON response: {json_err}")
                    messages.append(f"System: Your response was not valid JSON. Error: {json_err}. Please output valid JSON only.")
                    continue

                if "action" in decision and decision["action"] == "terminate":
                    reason = decision.get('reason', 'Complete')
                    print(f"🏁 Agent Finished: {reason}")
                    yield {"substep": "complete", "status": "complete", "log": reason, "data": None}
                    yield {
                        "substep": "FINAL", 
                        "status": "complete", 
                        "data": {
                            "verified_data": decision.get("verified_data"),
                            "fuzzy_tools_used": fuzzy_tools_used
                        }
                    }
                    return
                
                tool_name = decision.get("tool_name")
                args = decision.get("arguments", {})
                
                if not tool_name:
                    print(f"⚠️ Invalid decision format. Stopping.")
                    break

                # Validations before calling
                if tool_name not in tools_map:
                    print(f"❌ Error: Tool {tool_name} not found.")
                    messages.append(f"System: Tool {tool_name} does not exist. Choose from available tools.")
                    continue

                # Yield tool call event
                tool_call_count += 1
                friendly_name = tool_name.replace("_", " ").title()
                yield {
                    "substep": f"tool_{tool_call_count}", 
                    "status": "active", 
                    "log": f"Calling {friendly_name}...", 
                    "data": {"tool": tool_name, "args": args}
                }
                
                print(f"▶️ Executing: {tool_name}...")
                
                # Track fuzzy matching tools
                if tool_name in ["llm_find_orders", "select_order_id"]:
                    fuzzy_tools_used.append(tool_name)
                
                result = await db_session.call_tool(tool_name, arguments=args)
                tool_output_str = result.content[0].text
                
                # Print snippet for user
                display_output = tool_output_str[:500] + "..." if len(tool_output_str) > 500 else tool_output_str
                print(f"📄 Output: {display_output}")
                
                # Determine success/result summary for UI
                try:
                    tool_result = json.loads(tool_output_str)
                    if tool_name == "verify_from_email_matches_customer":
                        if tool_result.get("matched"):
                            result_summary = "Email verified ✓"
                        else:
                            result_summary = "Email not found"
                    elif tool_name in ["find_order_by_order_invoice_id", "find_order_by_invoice_number"]:
                        if tool_result.get("order_id") or tool_result.get("data"):
                            result_summary = f"Order found ✓"
                        else:
                            result_summary = "Order not found"
                    elif tool_name == "get_customer_orders_with_items":
                        order_count = len(tool_result.get("orders", []))
                        result_summary = f"Found {order_count} orders"
                    elif tool_name == "select_order_id":
                        if tool_result.get("selected_order_id"):
                            result_summary = f"Selected order: {tool_result.get('selected_order_id')}"
                        else:
                            result_summary = "No match found"
                    elif tool_name == "llm_find_orders":
                        result_summary = "SQL query executed"
                    else:
                        result_summary = "Complete"
                except:
                    result_summary = "Complete"
                
                yield {
                    "substep": f"tool_{tool_call_count}", 
                    "status": "complete", 
                    "log": result_summary, 
                    "data": None
                }
                
                # Feed result back to context
                messages.append(f"Tool '{tool_name}' Result:\n{tool_output_str}")
                
            except Exception as e:
                print(f"❌ Error in Agent Loop: {e}")
                yield {"substep": "error", "status": "error", "log": f"Error: {str(e)}", "data": None}
                break
        
        # If we exit the loop without terminating
        yield {"substep": "complete", "status": "complete", "log": "Max turns reached", "data": None}
        yield {"substep": "FINAL", "status": "complete", "data": None}

    async def process_single_email(self, bucket, blob_path):
        """Processes a single email from GCS."""
        print(f"Processing: gs://{bucket}/{blob_path}")
        
        # Local path for artifacts (in container)
        # Use /tmp for temporary storage or a dedicated artifacts dir
        base_name = os.path.basename(blob_path)
        folder_name = os.path.splitext(base_name)[0]
        artifacts_dir = f"/tmp/artifacts/{folder_name}"
        os.makedirs(artifacts_dir, exist_ok=True)
        
        json_file_path = os.path.join(artifacts_dir, base_name)
        
        # Download
        download_blob(bucket, blob_path, json_file_path)
        
        if not os.path.exists(json_file_path):
            raise Exception("Failed to download JSON file")

        doc_session = self.sessions.get("doc_server")
        if not doc_session: raise Exception("doc_server not connected")

        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        category = data.get("category", "NONE")
        if category not in ["RETURN", "REPLACEMENT", "REFUND"]:
             print(f"Skipping category: {category}")
             return

        combined_text = f"""
        --- EMAIL METADATA ---
        Sender: {data.get('user_id')}
        Received: {data.get('received_at')}
        Category: {category}
        Body: {data.get('email_body','')}
        """
        
        # Process attachments
        attachments = data.get("attachments", [])
        for attachment in attachments:
            filename = attachment.get("filename", "")
            if filename.lower().endswith(".pdf"):
                file_data = attachment.get("data", "")
                if isinstance(file_data, dict): file_data = file_data.get("data", "")
                if file_data:
                    txt_path = os.path.join(artifacts_dir, f"{filename}.txt")
                    parse_result = await doc_session.call_tool(
                        "process_invoice",
                        arguments={"base64_content": file_data, "output_txt_path": txt_path}
                    )
                    combined_text += f"\n\n--- INVOICE {filename} ---\n{parse_result.content[0].text}"
            elif filename.lower().endswith((".jpg", ".png", ".jpeg", ".webp")):
                 defect_session = self.sessions.get("defect_analyzer")
                 if defect_session:
                     file_data = attachment.get("data", "")
                     if isinstance(file_data, dict): file_data = file_data.get("data", "")
                     if file_data:
                         result = await defect_session.call_tool("analyze_defect_image", arguments={"image_base64": file_data})
                         combined_text += f"\n\n--- IMAGE {filename} ---\n{result.content[0].text}"

        # Extract
        extraction_json = await self.extract_order_details(combined_text)
        try:
            extracted_data = json.loads(extraction_json)
        except:
            extracted_data = {}
        
        # Verify
        verification_result = await self.verify_request_with_db(extracted_data)
        
        # Extract verified data and fuzzy tools info from result
        verified_record = None
        fuzzy_tools_used = []
        
        if verification_result:
            verified_record = verification_result.get("verified_data")
            fuzzy_tools_used = verification_result.get("fuzzy_tools_used", [])
        
        # Adjudicate
        adjudication_result = None
        if verified_record:
            # Merge extracted intent fields into verified record
            verified_record["return_request_date"] = extracted_data.get("return_request_date")
            verified_record["return_category"] = extracted_data.get("return_category")
            verified_record["return_reason_category"] = extracted_data.get("return_reason_category")
            verified_record["return_reason"] = extracted_data.get("return_reason")
            verified_record["item_condition"] = extracted_data.get("item_condition")
            verified_record["confidence_score"] = extracted_data.get("confidence_score")
            
            # Check if fuzzy matching tools were used - requires human review
            if fuzzy_tools_used:
                print(f"\n⚠️ HUMAN REVIEW REQUIRED")
                print(f"   Order was found using: {fuzzy_tools_used}")
                print(f"   Verified order saved. Skipping automatic adjudication.")
                
                # Insert refund case with pending human review status
                self.insert_refund_case(
                    email_data=data,
                    extracted_data=extracted_data,
                    verified_record=verified_record,
                    adjudication_result=None  # No adjudication - needs human review
                )
                print("Processing Complete.")
                return
            
            # --- Adjudication (only if exact match was found) ---
            try:
                print("\n" + "="*50)
                print("RUNNING ADJUDICATOR AGENT")
                print("="*50)
                adjudicator = Adjudicator()
                adjudication_result = await adjudicator.adjudicate(verified_record)
                
                print(f"\nDECISION: {adjudication_result.get('decision', 'UNKNOWN')}")
                print(f"REASON: {adjudication_result.get('details', {}).get('reasoning', 'N/A')}")
                
            except Exception as e:
                print(f"⚠️ Adjudication failed: {e}")
                import traceback
                traceback.print_exc()
                # Adjudication result remains None
        
        else:
            print("ℹ️ No verified order data was returned. Marking as PENDING_REVIEW.")

        # Insert to DB (Always insert, with or without verified_record/adjudication_result)
        self.insert_refund_case(
            email_data=data,
            extracted_data=extracted_data,
            verified_record=verified_record,
            adjudication_result=adjudication_result
        )
        
        print("Processing Complete.")

    async def cleanup(self):
        await self.exit_stack.aclose()
    async def process_demo_scenario(self, scenario_data: dict):
        """
        Process a demo scenario from JSON and yield SSE events for real-time UI updates.
        This follows the EXACT same approach as process_single_email, just with:
        - JSON input instead of GCS blob download
        - SSE yield statements for real-time UI updates
        
        Yields dict events: {"step": str, "status": str, "log": str, "data": dict}
        """
        # Use 'data' variable name to match process_single_email
        data = scenario_data
        
        # ========== STEP 1: CLASSIFICATION ==========
        yield {"step": "classification", "status": "active", "log": "📧 Analyzing email classification...", "data": None}
        
        category = data.get("category", "NONE")
        confidence = data.get("confidence", 0)
        
        yield {"step": "classification", "status": "complete", "log": f"📧 Category: {category} (confidence: {confidence})", "data": {
            "category": category,
            "confidence": confidence
        }}

        if category not in ["RETURN", "REPLACEMENT", "REFUND"]:
            print(f"Skipping category: {category}")
            yield {"step": "decision", "status": "complete", "log": f"⏭️ Skipping - category {category} not eligible", "data": {"decision": "SKIPPED"}}
            return

        # Check doc_session (same as process_single_email)
        doc_session = self.sessions.get("doc_server")
        if not doc_session:
            yield {"step": "error", "status": "error", "log": "❌ doc_server not connected", "data": None}
            return

        # ========== STEP 2: DOCUMENT PARSING ==========
        yield {"step": "parsing", "status": "active", "log": "📄 Processing attachments...", "data": None}
        
        # Build combined_text - EXACT same format as process_single_email
        combined_text = f"""
        --- EMAIL METADATA ---
        Sender: {data.get('user_id')}
        Received: {data.get('received_at')}
        Category: {category}
        Body: {data.get('email_body','')}
        """
        
        # Process attachments - EXACT same logic as process_single_email
        attachments = data.get("attachments", [])
        pdf_count = 0
        image_count = 0
        
        for attachment in attachments:
            filename = attachment.get("filename", "")
            
            # PDF processing - matches process_single_email exactly
            if filename.lower().endswith(".pdf"):
                pdf_count += 1
                yield {"step": "parsing", "status": "active", "log": f"📄 Parsing PDF: {filename}...", "data": None}
                
                file_data = attachment.get("data", "")
                if isinstance(file_data, dict):
                    file_data = file_data.get("data", "")
                
                if file_data:
                    try:
                        txt_path = f"/tmp/{filename}.txt"
                        parse_result = await doc_session.call_tool(
                            "process_invoice",
                            arguments={"base64_content": file_data, "output_txt_path": txt_path}
                        )
                        # Same header format as process_single_email
                        combined_text += f"\n\n--- INVOICE {filename} ---\n{parse_result.content[0].text}"
                        yield {"step": "parsing", "status": "active", "log": f"✅ Parsed {filename}", "data": {"filename": filename}}
                    except Exception as e:
                        yield {"step": "parsing", "status": "active", "log": f"⚠️ Error parsing {filename}: {e}", "data": None}
            
            # Image processing - matches process_single_email exactly
            elif filename.lower().endswith((".jpg", ".png", ".jpeg", ".webp")):
                image_count += 1
                defect_session = self.sessions.get("defect_analyzer")
                
                if defect_session:
                    yield {"step": "defect", "status": "active", "log": f"🔍 Analyzing image: {filename}...", "data": None}
                    
                    file_data = attachment.get("data", "")
                    if isinstance(file_data, dict):
                        file_data = file_data.get("data", "")
                    
                    if file_data:
                        try:
                            result = await defect_session.call_tool(
                                "analyze_defect_image",
                                arguments={"image_base64": file_data}
                            )
                            # Same header format as process_single_email: "--- IMAGE filename ---"
                            combined_text += f"\n\n--- IMAGE {filename} ---\n{result.content[0].text}"
                            yield {"step": "defect", "status": "active", "log": f"✅ Analyzed {filename}", "data": {"filename": filename, "analysis": result.content[0].text}}
                        except Exception as e:
                            yield {"step": "defect", "status": "active", "log": f"⚠️ Error analyzing {filename}: {e}", "data": None}

        # Complete parsing step
        if pdf_count == 0:
            yield {"step": "parsing", "status": "complete", "log": "📄 No PDF documents to parse", "data": None}
        else:
            yield {"step": "parsing", "status": "complete", "log": f"📄 Parsed {pdf_count} PDF document(s)", "data": None}

        # Complete defect step
        if image_count == 0:
            yield {"step": "defect", "status": "complete", "log": "🔍 No defect images in this request", "data": None}
        else:
            yield {"step": "defect", "status": "complete", "log": f"🔍 Analyzed {image_count} image(s)", "data": None}

        # ========== STEP 4: EXTRACTION ==========
        # Same as process_single_email
        yield {"step": "extraction", "status": "active", "log": "🧠 Extracting order details with Amazon Nova...", "data": None}
        
        extraction_json = await self.extract_order_details(combined_text)
        try:
            extracted_data = json.loads(extraction_json)
        except:
            extracted_data = {}
        
        yield {"step": "extraction", "status": "complete", "log": f"🧠 Extracted: Order #{extracted_data.get('order_invoice_id', 'N/A')}, Customer: {extracted_data.get('customer_email', 'N/A')}", "data": extracted_data}

        # ========== STEP 5: VERIFICATION ==========
        # Use streaming version to yield sub-step events
        yield {"step": "verification", "status": "active", "log": "🔐 Starting database verification...", "data": None}
        
        # Stream verification sub-steps
        verification_result = None
        async for event in self.verify_request_with_db_streaming(extracted_data):
            if event["substep"] == "FINAL":
                # Final event contains the complete result
                verification_result = event["data"]
            else:
                # Yield sub-step events to UI
                yield {
                    "step": "verification",
                    "status": "active",
                    "substep": event["substep"],
                    "substep_status": event["status"],
                    "log": event["log"],
                    "data": event.get("data")
                }
        
        # Extract verified data and fuzzy tools info from result (same as process_single_email)
        verified_record = None
        fuzzy_tools_used = []
        
        if verification_result:
            verified_record = verification_result.get("verified_data")
            fuzzy_tools_used = verification_result.get("fuzzy_tools_used", [])


        # ========== STEP 6: ADJUDICATION ==========
        adjudication_result = None
        
        if verified_record:
            yield {"step": "verification", "status": "complete", "log": f"🔐 Verified! Order found: #{verified_record.get('order_id', 'N/A')}", "data": verified_record}
            
            # Merge extracted intent fields into verified record (same as process_single_email)
            verified_record["return_request_date"] = extracted_data.get("return_request_date")
            verified_record["return_category"] = extracted_data.get("return_category")
            verified_record["return_reason_category"] = extracted_data.get("return_reason_category")
            verified_record["return_reason"] = extracted_data.get("return_reason")
            verified_record["item_condition"] = extracted_data.get("item_condition")
            verified_record["confidence_score"] = extracted_data.get("confidence_score")
            
            # Check if fuzzy matching tools were used - requires human review (same as process_single_email)
            if fuzzy_tools_used:
                print(f"\n⚠️ HUMAN REVIEW REQUIRED")
                print(f"   Order was found using: {fuzzy_tools_used}")
                print(f"   Verified order saved. Skipping automatic adjudication.")
                
                # Include the suggested order in the SSE event so UI can display it
                yield {"step": "verification", "status": "complete", "log": f"⚠️ Fuzzy match used: {fuzzy_tools_used} - Human review required", "data": {
                    "fuzzy_tools_used": fuzzy_tools_used,
                    "suggested_order": verified_record,
                    "needs_human_review": True,
                    "confidence": "low"
                }}
                yield {"step": "adjudication", "status": "complete", "log": "⏭️ Skipping adjudication - requires human review", "data": None}
                yield {"step": "decision", "status": "complete", "log": "⚠️ DECISION: PENDING_REVIEW (fuzzy match)", "data": {
                    "decision": "PENDING_REVIEW",
                    "reasoning": f"Order was found using fuzzy matching: {fuzzy_tools_used}"
                }}
                
                # Insert refund case with pending human review status (same as process_single_email)
                self.insert_refund_case(
                    email_data=data,
                    extracted_data=extracted_data,
                    verified_record=verified_record,
                    adjudication_result=None  # No adjudication - needs human review
                )
                
                print("Processing Complete.")
                return
            
            # --- Adjudication (only if exact match was found) --- (same as process_single_email)
            try:
                yield {"step": "adjudication", "status": "active", "log": "⚖️ Starting policy adjudication...", "data": None}
                
                print("\n" + "="*50)
                print("RUNNING ADJUDICATOR AGENT (STREAMING)")
                print("="*50)
                
                adjudicator = Adjudicator()
                adjudication_result = None
                
                # Use streaming adjudicator to yield sub-step events
                async for event in adjudicator.adjudicate_streaming(verified_record):
                    if event["substep"] == "FINAL":
                        # Final event contains the complete result
                        adjudication_result = event["data"]
                    else:
                        # Yield sub-step events to UI
                        yield {
                            "step": "adjudication",
                            "status": "active",
                            "substep": event["substep"],
                            "substep_status": event["status"],
                            "log": event["log"],
                            "data": event.get("data")
                        }
                
                # Mark adjudication complete
                decision = adjudication_result.get("decision", "UNKNOWN") if adjudication_result else "UNKNOWN"
                reasoning = adjudication_result.get("details", {}).get("reasoning", "N/A") if adjudication_result else "N/A"
                
                print(f"\nDECISION: {decision}")
                print(f"REASON: {reasoning}")
                
                yield {"step": "adjudication", "status": "complete", "log": f"⚖️ Policy check complete", "data": adjudication_result}
                
                # Final decision
                yield {"step": "decision", "status": "active", "log": "✅ Generating final decision...", "data": None}
                yield {"step": "decision", "status": "complete", "log": f"✅ DECISION: {decision}", "data": {
                    "decision": decision,
                    "reasoning": reasoning,
                    "verified_record": verified_record,
                    "adjudication": adjudication_result
                }}

                # Insert to DB (same as process_single_email)
                self.insert_refund_case(
                    email_data=data,
                    extracted_data=extracted_data,
                    verified_record=verified_record,
                    adjudication_result=adjudication_result
                )
            except Exception as e:
                print(f"⚠️ Adjudication failed: {e}")
                import traceback
                traceback.print_exc()
                
                yield {"step": "adjudication", "status": "error", "log": f"❌ Adjudication failed: {e}", "data": None}
                yield {"step": "decision", "status": "complete", "log": "⚠️ DECISION: MANUAL_REVIEW (adjudication error)", "data": {"decision": "MANUAL_REVIEW"}}
        
        else:
            # No verified order data was returned (same as process_single_email)
            print("ℹ️ No verified order data was returned. Marking as PENDING_REVIEW.")
            
            yield {"step": "verification", "status": "complete", "log": "⚠️ No order found - sending to human review", "data": None}
            yield {"step": "adjudication", "status": "complete", "log": "⏭️ Skipping adjudication - no verified order", "data": None}
            yield {"step": "decision", "status": "complete", "log": "⚠️ DECISION: PENDING_REVIEW (order not found)", "data": {
                "decision": "PENDING_REVIEW",
                "reasoning": "Order not found in database"
            }}
            
            # Insert to DB with no verified record (same as process_single_email)
            self.insert_refund_case(
                email_data=data,
                extracted_data=extracted_data,
                verified_record=None,
                adjudication_result=None
            )
        
        print("Processing Complete.")

