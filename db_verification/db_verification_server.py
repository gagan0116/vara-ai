# db_verification/db_verification_server.py
from __future__ import annotations

from typing import Any, Dict, List

from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

import os
import re
import json
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from bedrock_client import (
    NOVA_LITE,
    generate_with_retry,
    extract_text_from_response,
    _make_user_message,
)

# IMPORTANT: load .env BEFORE importing db.py (db.py reads env at import time)
load_dotenv()

from .db import db_connection, rows_as_dicts  # noqa: E402

mcp = FastMCP("db_verification")


_ORDER_SELECT_COLUMNS = """
    o.order_id::text            AS order_id,
    o.invoice_number            AS invoice_number,
    o.order_invoice_id          AS order_invoice_id,
    o.customer_id::text         AS customer_id,
    o.order_date                AS order_date,
    o.order_state               AS order_state,
    o.currency                  AS currency,
    o.subtotal_amount           AS subtotal_amount,
    o.discount_amount           AS discount_amount,
    o.shipping_amount           AS shipping_amount,
    o.total_amount              AS total_amount,
    o.balance_due               AS balance_due,
    o.refunded_amount           AS refunded_amount,
    o.ship_mode                 AS ship_mode,
    o.ship_city                 AS ship_city,
    o.ship_state                AS ship_state,
    o.ship_country              AS ship_country,
    o.delivered_at              AS delivered_at,
    o.seller_type               AS seller_type,
    o.created_at                AS created_at,
    o.updated_at                AS updated_at
"""

_ORDER_ITEM_SELECT_COLUMNS = """
    oi.order_item_id::text      AS order_item_id,
    oi.order_id::text           AS order_id,
    oi.sku                      AS sku,
    oi.item_name                AS item_name,
    oi.category                 AS category,
    oi.subcategory              AS subcategory,
    oi.quantity                 AS quantity,
    oi.unit_price               AS unit_price,
    oi.line_total               AS line_total,
    oi.refunded_qty             AS refunded_qty,
    oi.returned_qty             AS returned_qty,
    oi.metadata                 AS metadata
"""

_CUSTOMER_SELECT_COLUMNS = """
    c.customer_id::text AS customer_id,
    c.customer_email    AS customer_email,
    c.full_name         AS full_name,
    c.phone             AS phone,
    c.membership_tier   AS membership_tier,
    c.created_at        AS created_at
"""

def fetch_full_order_details(conn, field_name: str, value: str) -> Dict[str, Any] | None:
    """Helper to fetch full order hierarchy by a specific field (invoice_number or order_invoice_id)."""
    
    # 1. Fetch Order + Customer
    sql_main = f"""
        SELECT
            {_ORDER_SELECT_COLUMNS},
            {_CUSTOMER_SELECT_COLUMNS}
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
        WHERE o.{field_name} = %s
        LIMIT 1;
    """
    
    cur = conn.cursor()
    try:
        cur.execute(sql_main, (value,))
        rows = rows_as_dicts(cur)
    finally:
        cur.close()
        
    if not rows:
        return None
        
    main_data = rows[0]
    order_id = main_data["order_id"]
    
    # 2. Fetch Items
    sql_items = f"""
        SELECT {_ORDER_ITEM_SELECT_COLUMNS}
        FROM order_items oi
        WHERE oi.order_id = %s
        ORDER BY oi.item_name ASC;
    """
    
    cur = conn.cursor()
    try:
        cur.execute(sql_items, (order_id,))
        items = rows_as_dicts(cur)
    finally:
        cur.close()

    customer_info = {
        "customer_id": main_data["customer_id"],
        "customer_email": main_data["customer_email"],
        "full_name": main_data["full_name"],
        "phone": main_data["phone"],
        "created_at": main_data["created_at"]
    }
    
    # Separate order keys (everything else)
    order_info = {k: v for k, v in main_data.items() if k not in customer_info}

    return {
        "order_invoice_id": order_info.get("order_invoice_id"),
        "invoice_number": order_info.get("invoice_number"),
        "customer": customer_info,
        "order_details": order_info,
        "items": items
    }



@mcp.tool()
def list_orders_by_customer_email(customer_email: str, limit: int = 20) -> Dict[str, Any]:
    """
    List historical orders for a specific customer email.

    This tool searches the database for all orders associated with the provided email address.
    It performs a case-insensitive search.

    Args:
        customer_email (str): The customer's email address to search for (e.g., "user@example.com").
        limit (int, optional): The maximum number of orders to return. Defaults to 20. 
                               Values are clamped between 1 and 100.

    Returns:
        Dict[str, Any]: A dictionary containing:
            - customer_email: The email used for the search.
            - count: The number of orders found.
            - orders: A list of order dictionaries containing order summary details.
            - error: An error message string if the operation failed.
    """
    if not customer_email or not customer_email.strip():
        return {"customer_email": customer_email, "count": 0, "orders": [], "error": "customer_email is required"}

    limit = max(1, min(int(limit), 100))
    email = customer_email.strip()

    sql = f"""
        SELECT
            {_ORDER_SELECT_COLUMNS}
        FROM customers c
        JOIN orders o
          ON o.customer_id = c.customer_id
        WHERE lower(c.customer_email) = lower(%s)
        ORDER BY
            o.order_date DESC NULLS LAST,
            o.created_at DESC
        LIMIT %s;
    """

    try:
        with db_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(sql, (email, limit))
                orders: List[Dict[str, Any]] = rows_as_dicts(cur)
            finally:
                cur.close()
        return {"customer_email": email, "count": len(orders), "orders": orders}
    except Exception as e:
        return {"customer_email": email, "count": 0, "orders": [], "error": f"{type(e).__name__}: {e}"}


@mcp.tool()
def find_order_by_invoice_number(invoice_number: str, verification_email: str) -> Dict[str, Any]:
    """
    Find a single order by invoice number (exact match).
    Returns full hierarchy: Customer, Order Details, and Order Items.
    
    Args:
        invoice_number: The invoice number to search for.
        verification_email: Optional, if provided, verifies the order belongs to this customer.
    """
    if not invoice_number or not invoice_number.strip():
        return {"invoice_number": invoice_number, "found": False, "data": None, "error": "invoice_number is required"}

    inv = invoice_number.strip()

    try:
        with db_connection() as conn:
            data = fetch_full_order_details(conn, "invoice_number", inv)
        
        if data and verification_email:
            db_email = data.get("customer", {}).get("customer_email", "").strip().lower()
            verify_email = verification_email.strip().lower()
            if db_email != verify_email:
                return {
                    "invoice_number": inv, 
                    "found": False, 
                    "data": None, 
                    "error": "Validation Failed: Email verification mismatch. Order belongs to a different customer."
                }
            
        return {"invoice_number": inv, "found": data is not None, "data": data}
    except Exception as e:
        return {"invoice_number": inv, "found": False, "data": None, "error": f"{type(e).__name__}: {e}"}


@mcp.tool()
def find_order_by_order_invoice_id(order_invoice_id: str, verification_email: str) -> Dict[str, Any]:
    """
    Find a single order by order_invoice_id (exact match).
    Returns full hierarchy: Customer, Order Details, and Order Items.
    
    Args:
        order_invoice_id: The order invoice ID to search for.
        verification_email: Optional, if provided, verifies the order belongs to this customer.
    """
    if not order_invoice_id or not order_invoice_id.strip():
        return {
            "order_invoice_id": order_invoice_id,
            "found": False,
            "data": None,
            "error": "order_invoice_id is required",
        }

    oid = order_invoice_id.strip()

    try:
        with db_connection() as conn:
            data = fetch_full_order_details(conn, "order_invoice_id", oid)
            
        if data and verification_email:
            db_email = data.get("customer", {}).get("customer_email", "").strip().lower()
            verify_email = verification_email.strip().lower()
            if db_email != verify_email:
                return {
                    "order_invoice_id": oid, 
                    "found": False, 
                    "data": None, 
                    "error": "Validation Failed: Email verification mismatch. Order belongs to a different customer."
                }
            
        return {"order_invoice_id": oid, "found": data is not None, "data": data}
    except Exception as e:
        return {"order_invoice_id": oid, "found": False, "data": None, "error": f"{type(e).__name__}: {e}"}


@mcp.tool()
def list_order_items_by_order_invoice_id(order_invoice_id: str, limit: int = 200) -> Dict[str, Any]:
    """
    List detailed line items for a specific order_invoice_id.

    This tool fetches all products/items associated with an order. It is useful when you have the 
    Order ID but need to see exactly what was purchased (SKUs, current quantities, etc.).

    Args:
        order_invoice_id (str): The order invoice ID to search for (orders.order_invoice_id).
        limit (int, optional): Safety cap on number of items returned. Default 200, max 500.

    Returns:
        Dict[str, Any]: A dictionary containing:
            - order_invoice_id: ID searched.
            - order_id: The resolved internal UUID of the order.
            - count: Number of items returned.
            - order_items: List of item dictionaries (sku, item_name, quantity, metadata, etc).
            - error: Error message if operation failed.
    """

    if not order_invoice_id or not order_invoice_id.strip():
        return {
            "order_invoice_id": order_invoice_id,
            "order_id": None,
            "count": 0,
            "order_items": [],
            "error": "order_invoice_id is required",
        }

    limit = max(1, min(int(limit), 500))
    oid = order_invoice_id.strip()

    sql = f"""
        SELECT
            o.order_id::text       AS order_id,
            o.order_invoice_id     AS order_invoice_id,
            { _ORDER_ITEM_SELECT_COLUMNS }
        FROM orders o
        JOIN order_items oi
          ON oi.order_id = o.order_id
        WHERE o.order_invoice_id = %s
        ORDER BY oi.item_name ASC, oi.sku ASC
        LIMIT %s;
    """

    try:
        with db_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(sql, (oid, limit))
                rows: List[Dict[str, Any]] = rows_as_dicts(cur)
            finally:
                cur.close()

        if not rows:
            return {"order_invoice_id": oid, "order_id": None, "count": 0, "order_items": []}

        # Extract order_id from first row, and strip it out of each item
        resolved_order_id = rows[0].get("order_id")
        order_items: List[Dict[str, Any]] = []
        for r in rows:
            item = dict(r)
            item.pop("order_invoice_id", None)
            item.pop("order_id", None)  # keep order_id at top-level
            order_items.append(item)

        return {
            "order_invoice_id": oid,
            "order_id": resolved_order_id,
            "count": len(order_items),
            "order_items": order_items,
        }
    except Exception as e:
        return {
            "order_invoice_id": oid,
            "order_id": None,
            "count": 0,
            "order_items": [],
            "error": f"{type(e).__name__}: {e}",
        }
    
# -------------------
# VERIFICATION TOOLS
# -------------------

@mcp.tool()
def verify_from_email_matches_customer(from_email: str) -> Dict[str, Any]:
    """
    Check if a sender's email address exists in the customer database.

    This is an initial verification step to see if the user contacting support is a known customer.
    It performs a case-insensitive exact match.

    Args:
        from_email (str): The sender email address from the incoming request/email.

    Returns:
        Dict[str, Any]:
            - matched (bool): True if the email exists in the `customers` table.
            - customer (dict|None): Basic customer profile (ID, name, phone) if matched.
    """
    if not from_email or not from_email.strip():
        return {"from_email": from_email, "matched": False, "customer": None, "error": "from_email is required"}

    email = from_email.strip()

    sql = f"""
        SELECT
            {_CUSTOMER_SELECT_COLUMNS}
        FROM customers c
        WHERE lower(c.customer_email) = lower(%s)
        LIMIT 1;
    """

    try:
        with db_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(sql, (email,))
                rows = rows_as_dicts(cur)
            finally:
                cur.close()

        customer = rows[0] if rows else None
        return {"from_email": email, "matched": customer is not None, "customer": customer}
    except Exception as e:
        return {"from_email": email, "matched": False, "customer": None, "error": f"{type(e).__name__}: {e}"}
    

@mcp.tool()
def get_customer_orders_with_items(
    customer_email: str,
    max_orders: int = 50,
    max_items_per_order: int = 50,
    include_item_metadata: bool = False,
) -> Dict[str, Any]:
    """
    Retrieve a customer's order history including line items for each order.

    This is a "deep fetch" tool used when we can't identify a specific order ID. 
    It returns a structured history that an LLM can analyze to find a matching purchase 
    based on context (e.g., "I bought a blue shirt last week").

    Args:
        customer_email (str): The customer's email address.
        max_orders (int, optional): Max number of recent orders to fetch (default 50).
        max_items_per_order (int, optional): Max items to include per order (default 50).
        include_item_metadata (bool, optional): Whether to include large metadata JSON blobs for items. 
                                                Defaults to False to save bandwidth.

    Returns:
        Dict[str, Any]:
            - found_customer (bool): Whether the customer exists.
            - orders (List[Dict]): List of orders, where each order contains an 'items' list.
            - orders_truncated (bool): True if more orders exist than max_orders.
    """
    if not customer_email or not customer_email.strip():
        return {
            "customer_email": customer_email,
            "found_customer": False,
            "customer": None,
            "orders_count": 0,
            "orders": [],
            "error": "customer_email is required",
        }

    email = customer_email.strip()
    max_orders = max(1, min(int(max_orders), 200))
    max_items_per_order = max(1, min(int(max_items_per_order), 500))

    customer_sql = f"""
        SELECT
            {_CUSTOMER_SELECT_COLUMNS}
        FROM customers c
        WHERE lower(c.customer_email) = lower(%s)
        LIMIT 1;
    """

    # "All orders" (no timeframe), still ordered for usefulness
    orders_sql = """
        SELECT
            o.order_id::text       AS order_id,
            o.order_invoice_id     AS order_invoice_id,
            o.invoice_number       AS invoice_number,
            o.customer_id::text    AS customer_id,
            o.order_date           AS order_date,
            o.order_state          AS order_state,
            o.currency             AS currency,
            o.subtotal_amount      AS subtotal_amount,
            o.discount_amount      AS discount_amount,
            o.shipping_amount      AS shipping_amount,
            o.total_amount         AS total_amount,
            o.balance_due          AS balance_due,
            o.refunded_amount      AS refunded_amount,
            o.ship_mode            AS ship_mode,
            o.ship_city            AS ship_city,
            o.ship_state           AS ship_state,
            o.ship_country         AS ship_country,
            o.delivered_at         AS delivered_at
        FROM orders o
        WHERE o.customer_id = %s::uuid
        ORDER BY
            o.order_date DESC NULLS LAST,
            o.created_at DESC
        LIMIT %s;
    """

    # Optionally include per-item metadata
    item_cols = """
        oi.order_item_id::text AS order_item_id,
        oi.order_id::text      AS order_id,
        oi.sku                 AS sku,
        oi.item_name           AS item_name,
        oi.category            AS category,
        oi.subcategory         AS subcategory,
        oi.quantity            AS quantity,
        oi.unit_price          AS unit_price,
        oi.line_total          AS line_total,
        oi.refunded_qty        AS refunded_qty,
        oi.returned_qty        AS returned_qty
    """
    if include_item_metadata:
        item_cols += ", oi.metadata AS metadata"

    try:
        with db_connection() as conn:
            # 1) Fetch customer
            cur = conn.cursor()
            try:
                cur.execute(customer_sql, (email,))
                customer_rows = rows_as_dicts(cur)
            finally:
                cur.close()

            if not customer_rows:
                return {
                    "customer_email": email,
                    "found_customer": False,
                    "customer": None,
                    "orders_count": 0,
                    "orders": [],
                }

            customer = customer_rows[0]
            customer_id = customer["customer_id"]

            # 2) Fetch orders (no timeframe)
            cur = conn.cursor()
            try:
                cur.execute(orders_sql, (customer_id, max_orders))
                orders = rows_as_dicts(cur)
            finally:
                cur.close()

            if not orders:
                return {
                    "customer_email": email,
                    "found_customer": True,
                    "customer": customer,
                    "orders_count": 0,
                    "orders": [],
                }

            # 3) Fetch all items for these orders in one query (IN clause)
            order_ids = [o["order_id"] for o in orders]
            placeholders = ",".join(["%s"] * len(order_ids))

            items_sql = f"""
                SELECT
                    {item_cols}
                FROM order_items oi
                WHERE oi.order_id IN ({placeholders})
                ORDER BY oi.order_id, oi.item_name ASC, oi.sku ASC;
            """

            cur = conn.cursor()
            try:
                cur.execute(items_sql, tuple(order_ids))
                items_rows = rows_as_dicts(cur)
            finally:
                cur.close()

        # 4) Group items by order_id and apply per-order cap
        items_by_order: Dict[str, List[Dict[str, Any]]] = {}
        for r in items_rows:
            oid = r["order_id"]
            items_by_order.setdefault(oid, []).append(r)

        orders_out: List[Dict[str, Any]] = []
        items_truncated_any = False

        for o in orders:
            oid = o["order_id"]
            items = items_by_order.get(oid, [])

            if len(items) > max_items_per_order:
                items_truncated_any = True
                items = items[:max_items_per_order]

            # remove redundant order_id inside each item (kept at order level)
            cleaned_items: List[Dict[str, Any]] = []
            for it in items:
                it2 = dict(it)
                it2.pop("order_id", None)
                cleaned_items.append(it2)

            orders_out.append(
                {
                    **o,
                    "items_count": len(cleaned_items),
                    "items": cleaned_items,
                }
            )

        return {
            "customer_email": email,
            "found_customer": True,
            "customer": customer,
            "orders_count": len(orders_out),
            "orders_truncated": len(orders) >= max_orders,
            "items_truncated": items_truncated_any,
            "orders": orders_out,
        }

    except Exception as e:
        return {
            "customer_email": email,
            "found_customer": False,
            "customer": None,
            "orders_count": 0,
            "orders": [],
            "error": f"{type(e).__name__}: {e}",
        }
    
@mcp.tool()
def select_order_id(
    customer_orders_payload: Dict[str, Any],
    email_info: Dict[str, Any],
    model: str = None,
) -> Dict[str, Any]:
    """
    Use an LLM (Amazon Nova) to heuristically select the best matching order from history.

    This tool takes a raw list of customer orders (from `get_customer_orders_with_items`) 
    and the extracted email intent, then uses AI to reason about which order implies the request.
    It looks at item names, price amounts, dates, and other loose hints.

    Args:
        customer_orders_payload (Dict): The JSON output from `get_customer_orders_with_items`.
        email_info (Dict): The structured info extracted from the user's email 
                           (must not contain a reliable invoice ID, otherwise you wouldn't use this tool).
        model (str, optional): The Nova model to use for reasoning. Defaults to NOVA_LITE.

    Returns:
        Dict[str, Any]:
            - selected_order_id (str|None): The ID of the best match, or None if ambiguous.
            - confidence (float): A score from 0.0 to 1.0.
            - reason (str): Explanation of why this order was selected.
            - candidates (List): Alternative potential matches if uncertainty exists.
    """
    model = model or NOVA_LITE

    orders = customer_orders_payload.get("orders") if isinstance(customer_orders_payload, dict) else None
    if not orders or not isinstance(orders, list):
        return {
            "selected_order_id": None,
            "confidence": 0.0,
            "reason": "customer_orders_payload has no 'orders' list to choose from",
            "candidates": [],
            "error": "no_orders",
        }

    prompt = f"""
You are selecting the most likely order for a customer support request.

Context:
We are using this selection step ONLY because the email did NOT contain a usable invoice number or order_invoice_id.
So you MUST NOT rely on those identifiers.

You are given two JSON objects:

1) customer_orders_payload:
- Contains customer info and a list of orders.
- Each order includes a list of items.

2) email_info:
- Extracted/structured info from the customer's email.
- It may include item names, SKUs, quantities, amounts, currency, shipping location, and date hints.
- It does NOT include a reliable invoice number or order_invoice_id.

Task:
Choose the SINGLE best matching order from customer_orders_payload using only non-identifier details such as:
- item names / SKUs / categories
- quantities
- mentioned amount vs order total (allow small tolerance)
- shipping city/state/country (if mentioned)
- order recency hints in the email (e.g., "last week", "recent order") using order_date/delivered_at as context

Important rules:
- Prefer matches with clear item/SKU overlap.
- If multiple orders seem plausible, do NOT guess. Return up to 3 candidates.
- If there is not enough evidence to choose, set selected_order_id to null.

Return ONLY valid JSON in exactly this schema (no extra text):
{{
  "selected_order_id": string | null,
  "confidence": number,   // 0 to 1
  "reason": string,
  "candidates": [
    {{"order_id": string, "reason": string}}
  ]
}}

customer_orders_payload:
{json.dumps(customer_orders_payload, default=str)}

email_info:
{json.dumps(email_info, default=str)}
""".strip()


    try:
        system_prompt = "You are an order matching assistant. Respond with valid JSON only."
        messages = [_make_user_message(prompt)]

        response = generate_with_retry(
            model_id=model,
            messages=messages,
            system_prompt=system_prompt,
            max_tokens=2048,
            temperature=1.0,
            max_retries=3,
        )

        text = extract_text_from_response(response)
        if not text:
            return {
                "selected_order_id": None,
                "confidence": 0.0,
                "reason": "LLM returned empty response",
                "candidates": [],
                "error": "empty_model_response",
            }

        try:
            result = json.loads(text)
        except json.JSONDecodeError:
            # Try extracting JSON from code blocks
            json_match = re.search(r'```(?:json)?\s*\n?(.*?)\n?```', text, re.DOTALL)
            if json_match:
                result = json.loads(json_match.group(1))
            else:
                json_match = re.search(r'\{.*\}', text, re.DOTALL)
                if json_match:
                    result = json.loads(json_match.group())
                else:
                    return {
                        "selected_order_id": None,
                        "confidence": 0.0,
                        "reason": "LLM returned non-JSON output",
                        "candidates": [],
                        "error": "non_json_model_response",
                        "raw_model_text": text[:2000],
                    }

        # Minimal validation / normalization
        if not isinstance(result, dict):
            return {
                "selected_order_id": None,
                "confidence": 0.0,
                "reason": "LLM returned JSON but not an object",
                "candidates": [],
                "error": "bad_json_shape",
                "raw_model_json": result,
            }

        result.setdefault("selected_order_id", None)
        result.setdefault("confidence", 0.0)
        result.setdefault("reason", "")
        result.setdefault("candidates", [])

        return result

    except Exception as e:
        return {
            "selected_order_id": None,
            "confidence": 0.0,
            "reason": f"{type(e).__name__}: {e}",
            "candidates": [],
            "error": "llm_call_failed",
        }
    
@mcp.tool()
def llm_find_orders(email_info: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generate and execute a custom SQL query based on natural language email info.

    This is a "last resort" tool. If standard lookups fail, this tool uses an LLM to 
    construct a safe SQL SELECT statement based on the database schema and the 
    details found in the email (e.g., "products worth around $50").

    Args:
        email_info (Dict): Structured information containing potential search hints 
                           (amounts, partial names, dates).

    Returns:
        Dict[str, Any]: The results of the executed SQL query, or an error if the query was unsafe/invalid.
    """
    from db_verification.llm_sql_runner import llm_generate_and_execute  # local import to avoid import-time issues

    return llm_generate_and_execute(email_info=email_info)

if __name__ == "__main__":
    mcp.run(transport="stdio")