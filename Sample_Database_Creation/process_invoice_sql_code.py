from __future__ import annotations

import argparse
import json
import os
import re
import uuid
from pathlib import Path
from typing import Any, Dict, List

from pypdf import PdfReader

from openai import OpenAI


# Stable namespace so generated UUIDs are consistent across runs (idempotent SQL generation)
NAMESPACE_UUID = uuid.UUID("12345678-1234-5678-1234-567812345678")


def extract_pdf_text(pdf_path: Path) -> str:
    reader = PdfReader(str(pdf_path))
    chunks: List[str] = []
    for page in reader.pages:
        t = page.extract_text() or ""
        t = re.sub(r"\n{3,}", "\n\n", t.strip())
        if t:
            chunks.append(t)
    return "\n\n".join(chunks).strip()


# ----------------------------
# Nova extraction schema (internal only)
# ----------------------------

INVOICE_EXTRACT_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "customer": {
            "type": "object",
            "properties": {
                "full_name": {"type": "string"},
                "phone": {"type": "string"},
                "created_at": {"type": "string"},  # YYYY-MM-DD
            },
            "required": ["full_name", "phone", "created_at"],
        },
        "order": {
            "type": "object",
            "properties": {
                "invoice_number": {"type": "string"},
                "order_invoice_id": {"type": "string"},  # matches your schema
                "order_date": {"type": "string"},        # YYYY-MM-DD
                "ship_mode": {"type": "string"},
                "ship_city": {"type": "string"},
                "ship_state": {"type": "string"},
                "ship_country": {"type": "string"},
                "currency": {"type": "string"},
                "subtotal_amount": {"type": "string"},
                "discount_amount": {"type": "string"},
                "shipping_amount": {"type": "string"},
                "total_amount": {"type": "string"},
            },
            "required": [
                "invoice_number",
                "order_invoice_id",
                "order_date",
                "ship_mode",
                "ship_city",
                "ship_state",
                "ship_country",
                "currency",
                "subtotal_amount",
                "discount_amount",
                "shipping_amount",
                "total_amount",
            ],
        },
        "items": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "sku": {"type": "string"},
                    "item_name": {"type": "string"},
                    "category": {"type": "string"},
                    "subcategory": {"type": "string"},
                    "quantity": {"type": "integer"},
                    "unit_price": {"type": "string"},
                    "line_total": {"type": "string"},
                },
                "required": [
                    "sku",
                    "item_name",
                    "category",
                    "subcategory",
                    "quantity",
                    "unit_price",
                    "line_total",
                ],
            },
        },
    },
    "required": ["customer", "order", "items"],
}


def nova_extract(invoice_text: str, model: str, api_key: str) -> Dict[str, Any]:
    client = OpenAI(
        api_key=api_key,
        base_url="https://api.nova.amazon.com/v1"
    )

    system_instruction = (
        "You extract invoice fields from text. Return ONLY JSON matching the provided schema.\n"
        "Rules:\n"
        "- customer_email is NOT present; do not include it.\n"
        "- Generate a realistic random phone number based on ship_country (country code + plausible format).\n"
        "- created_at must be the invoice date.\n"
        "- order_date must be the invoice date.\n"
        "- Dates: output in 'YYYY-MM-DD' format.\n"
        "- Numeric fields must be plain number strings like '1232.75' (no '$', no commas).\n"
        "- currency must be a 3-letter code (USD, etc). If invoice uses '$', use USD.\n"
        "- Do not hallucinate extra items.\n\n"
        "Your schema is:\n" + json.dumps(INVOICE_EXTRACT_SCHEMA)
    )

    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_instruction},
            {"role": "user", "content": invoice_text}
        ],
        temperature=1.0
    )
    
    content = response.choices[0].message.content

    try:
        json_match = re.search(r'```(?:json)?\s*\n?(.*?)\n?```', content, re.DOTALL)
        if json_match:
            return json.loads(json_match.group(1))
        json_match = re.search(r'\{.*\}', content, re.DOTALL)
        if json_match:
            return json.loads(json_match.group())
        return json.loads(content)
    except Exception as e:
        raise RuntimeError(f"Nova returned non-JSON output:\n{content}") from e


# ----------------------------
# SQL generation
# ----------------------------

def _sql_quote(s: str | None) -> str:
    if s is None:
        return "NULL"
    return "'" + s.replace("'", "''") + "'"


def _sql_num(x: str | int | float | None) -> str:
    if x is None:
        return "NULL"
    if isinstance(x, (int, float)):
        return str(x)
    if isinstance(x, str):
        cleaned = x.replace(",", "").strip()
        cleaned = re.sub(r"[^0-9.\-]", "", cleaned)
        if cleaned == "":
            raise ValueError(f"Could not parse numeric from: {x!r}")
        return cleaned
    raise TypeError(f"Unsupported numeric type: {type(x)}")


def stable_customer_id(full_name: str, ship_city: str, ship_state: str, ship_country: str) -> uuid.UUID:
    key = f"customer|{full_name.lower()}|{ship_city.lower()}|{ship_state.lower()}|{ship_country.lower()}"
    return uuid.uuid5(NAMESPACE_UUID, key)


def stable_order_id(invoice_number: str, order_invoice_id: str) -> uuid.UUID:
    key = f"order|{invoice_number}|{order_invoice_id}"
    return uuid.uuid5(NAMESPACE_UUID, key)


def stable_item_id(invoice_number: str, order_invoice_id: str, idx: int, sku: str) -> uuid.UUID:
    key = f"item|{invoice_number}|{order_invoice_id}|{idx}|{sku}"
    return uuid.uuid5(NAMESPACE_UUID, key)


def build_sql_for_invoice(extracted: Dict[str, Any], source_pdf_name: str) -> str:
    cust = extracted["customer"]
    order = extracted["order"]
    items = extracted["items"]

    full_name = str(cust["full_name"]).strip()
    phone = str(cust["phone"]).strip()
    customer_created_at = str(cust["created_at"]).strip()  # YYYY-MM-DD

    invoice_number = str(order["invoice_number"]).strip()
    order_invoice_id = str(order["order_invoice_id"]).strip()

    order_date = str(order["order_date"]).strip()  # YYYY-MM-DD
    ship_mode = str(order["ship_mode"]).strip()
    ship_city = str(order["ship_city"]).strip()
    ship_state = str(order["ship_state"]).strip()
    ship_country = str(order["ship_country"]).strip()
    currency = (str(order["currency"]).strip() or "USD")[:3].upper()

    subtotal = order["subtotal_amount"]
    discount = order["discount_amount"]
    shipping = order["shipping_amount"]
    total = order["total_amount"]

    # Your rules:
    customer_email = None
    balance_due = "0.00"
    refunded_amount = "0.00"
    order_state = "delivered"

    customer_id = stable_customer_id(full_name, ship_city, ship_state, ship_country)
    order_id = stable_order_id(invoice_number, order_invoice_id)

    sql_parts: List[str] = []
    sql_parts.append(f"""
-- Auto-generated inserts for: {source_pdf_name}
-- Invoice {invoice_number} | Order {order_invoice_id}
BEGIN;

INSERT INTO customers (customer_id, customer_email, full_name, phone, created_at, metadata)
VALUES (
  {_sql_quote(str(customer_id))}::uuid,
  {_sql_quote(customer_email)},
  {_sql_quote(full_name)},
  {_sql_quote(phone)},
  {_sql_quote(customer_created_at)}::timestamptz,
  NULL
)
ON CONFLICT DO NOTHING;

-- NOTE: updated_at is NOT NULL in your schema, so we omit it and let DEFAULT now() apply.
INSERT INTO orders (
  order_id, invoice_number, order_invoice_id, customer_id,
  order_date, ship_mode, ship_city, ship_state, ship_country,
  currency, subtotal_amount, discount_amount, shipping_amount, total_amount,
  balance_due, refunded_amount, order_state, delivered_at,
  metadata, created_at
)
VALUES (
  {_sql_quote(str(order_id))}::uuid,
  {_sql_quote(invoice_number)},
  {_sql_quote(order_invoice_id)},
  {_sql_quote(str(customer_id))}::uuid,

  {_sql_quote(order_date)}::timestamptz,
  {_sql_quote(ship_mode)},
  {_sql_quote(ship_city)},
  {_sql_quote(ship_state)},
  {_sql_quote(ship_country)},

  {_sql_quote(currency)},
  {_sql_num(subtotal)},
  {_sql_num(discount)},
  {_sql_num(shipping)},
  {_sql_num(total)},

  {_sql_num(balance_due)},
  {_sql_num(refunded_amount)},
  {_sql_quote(order_state)},
  NULL,

  NULL,
  {_sql_quote(order_date)}::timestamptz
)
ON CONFLICT DO NOTHING;
""".strip())

    for idx, it in enumerate(items):
        sku = str(it["sku"]).strip()
        item_name = str(it["item_name"]).strip()
        category = str(it["category"]).strip()
        subcategory = str(it["subcategory"]).strip()
        quantity = int(it["quantity"])
        unit_price = it["unit_price"]
        line_total = it["line_total"]

        item_id = stable_item_id(invoice_number, order_invoice_id, idx, sku)

        sql_parts.append(f"""
INSERT INTO order_items (
  order_item_id, order_id, sku, item_name, category, subcategory,
  quantity, unit_price, line_total,
  refunded_qty, returned_qty,
  metadata
)
VALUES (
  {_sql_quote(str(item_id))}::uuid,
  {_sql_quote(str(order_id))}::uuid,
  {_sql_quote(sku)},
  {_sql_quote(item_name)},
  {_sql_quote(category)},
  {_sql_quote(subcategory)},

  {quantity},
  {_sql_num(unit_price)},
  {_sql_num(line_total)},

  0,
  0,

  NULL
)
ON CONFLICT DO NOTHING;
""".strip())

    sql_parts.append("COMMIT;\n")
    return "\n".join(sql_parts)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("invoice_pdf", help="Path to a single invoice PDF")
    ap.add_argument("--out", default="single_invoice_inserts.sql", help="Output SQL file")
    ap.add_argument("--model", default="nova-2-lite-v1", help="Nova model name")
    args = ap.parse_args()

    api_key = os.getenv("NOVA_API_KEY")
    if not api_key:
        raise RuntimeError("Set NOVA_API_KEY in your environment.")

    pdf_path = Path(args.invoice_pdf)
    if not pdf_path.exists() or pdf_path.suffix.lower() != ".pdf":
        raise FileNotFoundError(f"PDF not found: {pdf_path.resolve()}")

    text = extract_pdf_text(pdf_path)
    if not text:
        raise RuntimeError("No text extracted (PDF may be image-only).")

    extracted = nova_extract(text, model=args.model, api_key=api_key)
    sql = build_sql_for_invoice(extracted, source_pdf_name=pdf_path.name)

    out_path = Path(args.out)
    out_path.write_text(sql, encoding="utf-8")
    print(f"✅ Wrote SQL to: {out_path.resolve()}")


if __name__ == "__main__":
    main()
