from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

import psycopg
from pypdf import PdfReader
from openai import OpenAI

from dotenv import load_dotenv
load_dotenv()
import random
import time
NOVA_API_KEY = os.getenv("NOVA_API_KEY")
# =========================
# CONFIG
# =========================

SOURCE_INVOICE_DIR = Path("C:\\Users\\Lenovo\\Downloads\\1000+ PDF_Invoice_Folder\\1000+ PDF_Invoice_Folder")
FILTERED_INVOICE_DIR = Path("invoices")

ALLOWED_CUSTOMERS = {
    "sean o'donnell",
    "theone pippenger",
    "scot coram",
    "rick wilson",
    "rick reed",
    "randy bradley",
    "tamara willingham",
    "trudy brown",
    "stuart calhoun",
    "rose o'brian",
    "tamara chand",
    "roger demir",
    "sarah bern",
    "theresa coyne",
    "richard eichhorn",
}

NAMESPACE_UUID = uuid.UUID("12345678-1234-5678-1234-567812345678")


# =========================
# PDF HANDLING
# =========================

def extract_username(filename: str) -> Optional[str]:
    """
    invoice_username_id.pdf → username
    """
    parts = filename.lower().replace(".pdf", "").split("_")
    return parts[1] if len(parts) >= 3 else None


def move_selected_invoices() -> List[Path]:
    """
    IMPORTANT CHANGE:
    - This now ONLY SELECTS invoices (does NOT move them yet).
    - Actual move happens only after successful DB insert + commit.
    """
    selected: List[Path] = []

    for pdf in SOURCE_INVOICE_DIR.glob("*.pdf"):
        username = extract_username(pdf.name)
        if username and username in ALLOWED_CUSTOMERS:
            selected.append(pdf)
            print(f"✅ Selected: {pdf.name}")

    return selected


def extract_pdf_text(pdf_path: Path) -> str:
    reader = PdfReader(str(pdf_path))
    chunks: List[str] = []

    for page in reader.pages:
        text = page.extract_text() or ""
        text = re.sub(r"\n{3,}", "\n\n", text.strip())
        if text:
            chunks.append(text)

    return "\n\n".join(chunks).strip()


# =========================
# NOVA EXTRACTION
# =========================

INVOICE_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "customer": {
            "type": "object",
            "properties": {
                "full_name": {"type": "string"},
            },
            "required": ["full_name"],
        },
        "order": {
            "type": "object",
            "properties": {
                "invoice_number": {"type": "string"},
                "order_invoice_id": {"type": "string"},
                "order_date": {"type": "string"},
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


def nova_extract(text: str, model: str, api_key: str) -> Dict[str, Any]:
    client = genai.Client(api_key=api_key)
    max_retries = 5

    for attempt in range(max_retries):
        try:
            resp = client.models.generate_content(
                model=model,
                contents=text,
                config=types.GenerateContentConfig(
                    system_instruction=(
                        "Extract invoice data. "
                        "Return ONLY valid JSON matching the schema. "
                        "Dates must be YYYY-MM-DD. "
                        "Numbers must be plain strings (no currency symbols)."
                    ),
                    temperature=1.0,
                    response_mime_type="application/json",
                    response_schema=INVOICE_SCHEMA,
                ),
            )
            return json.loads(resp.text)

        except Exception as e:
            error_str = str(e).lower()
            if "503" in error_str or "overloaded" in error_str or "429" in error_str:
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                print(f"⚠️ Model overloaded. Retrying in {wait_time:.1f}s...")
                time.sleep(wait_time)
                continue
            raise e

    raise RuntimeError("Nova API failed after max retries")




# =========================
# DATABASE
# =========================

def fetch_customer_id(conn, full_name: str) -> uuid.UUID:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT customer_id
            FROM customers
            WHERE lower(full_name) = lower(%s)
            LIMIT 1
            """,
            (full_name.strip(),),
        )
        row = cur.fetchone()

    if not row:
        raise RuntimeError(f"❌ Customer not found: {full_name}")

    return row[0]


# =========================
# SQL GENERATION
# =========================

def _q(s: str | None) -> str:
    return "NULL" if s is None else "'" + s.replace("'", "''") + "'"


def _n(v: str | int | float) -> str:
    return re.sub(r"[^0-9.\-]", "", str(v))


def stable_order_id(invoice_number: str, order_invoice_id: str) -> uuid.UUID:
    return uuid.uuid5(
        NAMESPACE_UUID,
        f"order|{invoice_number}|{order_invoice_id}",
    )


def stable_item_id(
    invoice_number: str,
    order_invoice_id: str,
    idx: int,
    sku: str,
) -> uuid.UUID:
    return uuid.uuid5(
        NAMESPACE_UUID,
        f"item|{invoice_number.strip().lower()}|{order_invoice_id.strip().lower()}|{idx}|{sku}",
    )


def build_sql(
    extracted: Dict[str, Any],
    pdf_name: str,
    customer_id: uuid.UUID,
) -> str:
    order = extracted["order"]
    items = extracted["items"]

    order_id = stable_order_id(
        order["invoice_number"],
        order["order_invoice_id"],
    )

    sql: List[str] = [f"""
-- {pdf_name}
BEGIN;

INSERT INTO orders (
  order_id, invoice_number, order_invoice_id, customer_id,
  order_date, ship_mode, ship_city, ship_state, ship_country,
  currency, subtotal_amount, discount_amount, shipping_amount,
  total_amount, balance_due, refunded_amount, order_state,
  delivered_at, metadata, created_at
)
VALUES (
  '{order_id}'::uuid,
  {_q(order["invoice_number"])},
  {_q(order["order_invoice_id"])},
  '{customer_id}'::uuid,
  {_q(order["order_date"])}::timestamptz,
  {_q(order["ship_mode"])},
  {_q(order["ship_city"])},
  {_q(order["ship_state"])},
  {_q(order["ship_country"])},
  {_q(order["currency"])},
  {_n(order["subtotal_amount"])},
  {_n(order["discount_amount"])},
  {_n(order["shipping_amount"])},
  {_n(order["total_amount"])},
  0.00,
  0.00,
  'delivered',
  NULL,
  NULL,
  {_q(order["order_date"])}::timestamptz
)
ON CONFLICT DO NOTHING;
""".strip()]

    for idx, it in enumerate(items):
        item_id = stable_item_id(
            order["invoice_number"],
            order["order_invoice_id"],
            idx,
            it["sku"],
        )

        sql.append(f"""
INSERT INTO order_items (
  order_item_id, order_id, sku, item_name,
  category, subcategory, quantity,
  unit_price, line_total,
  refunded_qty, returned_qty, metadata
)
VALUES (
  '{item_id}'::uuid,
  '{order_id}'::uuid,
  {_q(it["sku"])},
  {_q(it["item_name"])},
  {_q(it["category"])},
  {_q(it["subcategory"])},
  {int(it["quantity"])},
  {_n(it["unit_price"])},
  {_n(it["line_total"])},
  0,
  0,
  NULL
)
ON CONFLICT DO NOTHING;
""".strip())

    sql.append("COMMIT;")
    return "\n".join(sql)


# =========================
# MAIN
# =========================

def main():
    api_key = os.getenv("NOVA_API_KEY")

    if not api_key:
        raise RuntimeError("Set NOVA_API_KEY")

    conn = psycopg.connect(
    host=os.getenv("DB_HOST", "localhost"),
    port=int(os.getenv("DB_PORT", "5432")),
    dbname=os.getenv("DB_NAME", "refunds_db"),
    user=os.getenv("DB_USER", "postgres"),
    password=os.getenv("DB_PASSWORD"),
)

    selected_pdfs = move_selected_invoices()
    all_sql: List[str] = []

    # Ensure destination folder exists (needed because we now move later)
    FILTERED_INVOICE_DIR.mkdir(exist_ok=True)

    for pdf in selected_pdfs:
        print(f"Processing {pdf.name}...")
        try:
            text = extract_pdf_text(pdf)
            extracted = nova_extract(text, "nova-2-lite-v1", api_key)

            full_name = extracted["customer"]["full_name"]
            customer_id = fetch_customer_id(conn, full_name)

            sql = build_sql(extracted, pdf.name, customer_id)
            all_sql.append(sql)

            # ✅ EXECUTE SQL IMMEDIATELY
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()
            print(f"✅ DB Inserted: {pdf.name}")

            # ✅ MOVE ONLY AFTER SUCCESSFUL COMMIT
            dest = FILTERED_INVOICE_DIR / pdf.name
            shutil.move(str(pdf), dest)
            print(f"✅ Moved: {pdf.name}")

        except Exception as e:
            conn.rollback()
            print(f"❌ Failed processing {pdf.name}: {e}")

        # ✅ Sleep between requests to avoid overloading
        time.sleep(2.0)


if __name__ == "__main__":
    main()
