from __future__ import annotations

import csv
import io
import json
import re
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping


FAIRE_EXPORT_COLUMNS = (
    "Brand Name",
    "Product Title",
    "Variant / Option",
    "SKU",
    "UPC",
    "EAN",
    "Wholesale Cost",
    "Retail Price (MSRP)",
    "MOQ / Pack Size",
    "Case Multiplier",
    "Product URL",
    "Image URL",
    "Description",
    "Lead Time",
    "Ships From",
    "Scraped At",
)


def clean_text(value: object) -> str:
    text = str(value or "").strip()
    return re.sub(r"\s+", " ", text).strip()


def clean_price(value: object) -> str:
    text = str(value or "").replace(",", "").strip()
    # Check if value is in cents (integer > 100 with no decimal)
    if isinstance(value, int) or (isinstance(value, str) and value.isdigit() and len(value) >= 3):
        try:
            val_int = int(value)
        except ValueError:
            pass
    text = re.sub(r"(?i)\b(?:USD|EA|\$|EUR|GBP|CAD)\b", "", text).strip()
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return ""
    try:
        number = float(match.group())
        return f"{number:.2f}" if "." in match.group() else str(int(number))
    except (ValueError, OverflowError):
        return ""


def clean_barcode(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if re.fullmatch(r"\d+\.0", text):
        return text[:-2]
    return text


def classify_barcode(code: object) -> tuple[str, str]:
    """Classifies a barcode string into (upc, ean)."""
    clean = clean_barcode(code)
    if not clean:
        return ("", "")
    digits = re.sub(r"\D", "", clean)
    if len(digits) == 12:
        return (digits, "")
    elif len(digits) in (13, 14, 8):
        return ("", digits)
    elif len(digits) < 12 and digits:
        return (digits.zfill(12), "")
    return (clean, "")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class FaireDatabase:
    def __init__(self, path: str | Path = "faire_catalog.db") -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path, timeout=15.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode = WAL;")
        connection.execute("PRAGMA busy_timeout = 15000;")
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _initialize(self) -> None:
        with sqlite3.connect(self.path, timeout=15.0) as conn:
            conn.execute("PRAGMA journal_mode = WAL;")
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS faire_products (
                    key TEXT PRIMARY KEY,
                    product_id TEXT NOT NULL DEFAULT '',
                    variant_id TEXT NOT NULL DEFAULT '',
                    variant_name TEXT NOT NULL DEFAULT '',
                    product_url TEXT NOT NULL DEFAULT '',
                    brand_id TEXT NOT NULL DEFAULT '',
                    brand_name TEXT NOT NULL DEFAULT '',
                    product_title TEXT NOT NULL DEFAULT '',
                    description TEXT NOT NULL DEFAULT '',
                    wholesale_price TEXT NOT NULL DEFAULT '',
                    retail_price TEXT NOT NULL DEFAULT '',
                    sku TEXT NOT NULL DEFAULT '',
                    upc TEXT NOT NULL DEFAULT '',
                    ean TEXT NOT NULL DEFAULT '',
                    barcode TEXT NOT NULL DEFAULT '',
                    moq TEXT NOT NULL DEFAULT '',
                    unit_multiplier TEXT NOT NULL DEFAULT '',
                    lead_time TEXT NOT NULL DEFAULT '',
                    ships_from TEXT NOT NULL DEFAULT '',
                    image_url TEXT NOT NULL DEFAULT '',
                    variants_json TEXT NOT NULL DEFAULT '[]',
                    category TEXT NOT NULL DEFAULT '',
                    enriched INTEGER NOT NULL DEFAULT 0,
                    last_scraped_at TEXT NOT NULL DEFAULT '',
                    last_run_id TEXT NOT NULL DEFAULT ''
                );

                CREATE TABLE IF NOT EXISTS faire_brands (
                    brand_id TEXT PRIMARY KEY,
                    brand_name TEXT NOT NULL DEFAULT '',
                    brand_url TEXT NOT NULL DEFAULT '',
                    category TEXT NOT NULL DEFAULT '',
                    minimum_order TEXT NOT NULL DEFAULT '',
                    lead_time TEXT NOT NULL DEFAULT '',
                    ships_from TEXT NOT NULL DEFAULT '',
                    product_count INTEGER NOT NULL DEFAULT 0,
                    last_scraped_at TEXT NOT NULL DEFAULT ''
                );

                CREATE TABLE IF NOT EXISTS faire_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL UNIQUE,
                    mode TEXT NOT NULL DEFAULT 'brand',
                    target_url TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'running',
                    items_scraped INTEGER NOT NULL DEFAULT 0,
                    started_at TEXT NOT NULL DEFAULT '',
                    finished_at TEXT NOT NULL DEFAULT ''
                );

                CREATE INDEX IF NOT EXISTS idx_faire_products_brand ON faire_products(brand_name);
                CREATE INDEX IF NOT EXISTS idx_faire_products_barcode ON faire_products(barcode);
                CREATE INDEX IF NOT EXISTS idx_faire_products_product_id ON faire_products(product_id);
                CREATE INDEX IF NOT EXISTS idx_faire_products_scraped_at ON faire_products(last_scraped_at DESC);
                """
            )
            # Automatic schema migration for existing databases
            existing_cols = {r[1] for r in conn.execute("PRAGMA table_info(faire_products)").fetchall()}
            for col in ["variant_id", "variant_name", "upc", "ean"]:
                if col not in existing_cols:
                    try:
                        conn.execute(f"ALTER TABLE faire_products ADD COLUMN {col} TEXT NOT NULL DEFAULT ''")
                    except Exception:
                        pass
            if "enriched" not in existing_cols:
                try:
                    conn.execute("ALTER TABLE faire_products ADD COLUMN enriched INTEGER NOT NULL DEFAULT 0")
                    conn.execute(
                        "UPDATE faire_products SET enriched = 1 "
                        "WHERE upc != '' OR ean != '' OR barcode != '' OR sku != '' OR variant_name != ''"
                    )
                except Exception:
                    pass

    def upsert_product(self, product: Mapping[str, object], run_id: str = "") -> None:
        product_id = str(product.get("product_id") or "").strip()
        variant_id = str(product.get("variant_id") or "").strip()
        variant_name = clean_text(product.get("variant_name") or product.get("option_name"))
        product_url = str(product.get("product_url") or "").strip()
        sku = str(product.get("sku") or "").strip()

        # UPC & EAN parsing
        raw_barcode = clean_barcode(product.get("barcode"))
        upc = str(product.get("upc") or "").strip()
        ean = str(product.get("ean") or "").strip()
        if not upc and not ean and raw_barcode:
            upc, ean = classify_barcode(raw_barcode)
        barcode = raw_barcode or upc or ean

        # Derive a robust unique key
        key = str(product.get("key") or "").strip()
        if not key:
            if product_id:
                key = f"p:{product_id}"
                if variant_id:
                    key += f":v:{variant_id}"
                elif variant_name:
                    key += f":opt:{variant_name}"
                elif sku:
                    key += f":sku:{sku}"
            elif product_url:
                key = f"u:{product_url}"
                if variant_name or sku:
                    key += f":{variant_name or sku}"
            elif barcode:
                key = f"b:{barcode}"
            else:
                key = f"t:{clean_text(product.get('product_title'))}:{variant_name}:{sku}"

        brand_id = str(product.get("brand_id") or "").strip()
        brand_name = clean_text(product.get("brand_name") or product.get("brand"))
        title = clean_text(product.get("product_title") or product.get("title") or product.get("name"))
        desc = str(product.get("description") or "").strip()
        wholesale_price = clean_price(product.get("wholesale_price") or product.get("cost") or product.get("supply_price"))
        retail_price = clean_price(product.get("retail_price") or product.get("msrp"))
        moq = str(product.get("moq") or product.get("case_quantity") or "1").strip()
        unit_multiplier = str(product.get("unit_multiplier") or product.get("pack_size") or "").strip()
        lead_time = str(product.get("lead_time") or "").strip()
        ships_from = str(product.get("ships_from") or "").strip()
        image_url = str(product.get("image_url") or "").strip()
        category = clean_text(product.get("category"))
        variants_json = json.dumps(product.get("variants") or []) if isinstance(product.get("variants"), (list, dict)) else str(product.get("variants_json") or "[]")
        enriched = 1 if product.get("enriched") else 0
        now = utc_now()

        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO faire_products (
                    key, product_id, variant_id, variant_name, product_url, brand_id, brand_name,
                    product_title, description, wholesale_price, retail_price,
                    sku, upc, ean, barcode, moq, unit_multiplier, lead_time, ships_from,
                    image_url, variants_json, category, enriched, last_scraped_at, last_run_id
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                ON CONFLICT(key) DO UPDATE SET
                    product_id = COALESCE(NULLIF(excluded.product_id, ''), faire_products.product_id),
                    variant_id = COALESCE(NULLIF(excluded.variant_id, ''), faire_products.variant_id),
                    variant_name = COALESCE(NULLIF(excluded.variant_name, ''), faire_products.variant_name),
                    product_url = COALESCE(NULLIF(excluded.product_url, ''), faire_products.product_url),
                    brand_id = COALESCE(NULLIF(excluded.brand_id, ''), faire_products.brand_id),
                    brand_name = COALESCE(NULLIF(excluded.brand_name, ''), faire_products.brand_name),
                    product_title = CASE
                        WHEN excluded.product_title != '' AND excluded.product_title NOT LIKE 'Product p_%' AND excluded.product_title != 'Find similar' THEN excluded.product_title
                        WHEN faire_products.product_title != '' AND faire_products.product_title NOT LIKE 'Product p_%' AND faire_products.product_title != 'Find similar' THEN faire_products.product_title
                        ELSE COALESCE(NULLIF(excluded.product_title, ''), faire_products.product_title)
                    END,
                    description = COALESCE(NULLIF(excluded.description, ''), faire_products.description),
                    wholesale_price = COALESCE(NULLIF(excluded.wholesale_price, ''), faire_products.wholesale_price),
                    retail_price = COALESCE(NULLIF(excluded.retail_price, ''), faire_products.retail_price),
                    sku = COALESCE(NULLIF(excluded.sku, ''), faire_products.sku),
                    upc = COALESCE(NULLIF(excluded.upc, ''), faire_products.upc),
                    ean = COALESCE(NULLIF(excluded.ean, ''), faire_products.ean),
                    barcode = COALESCE(NULLIF(excluded.barcode, ''), faire_products.barcode),
                    moq = COALESCE(NULLIF(excluded.moq, ''), faire_products.moq),
                    unit_multiplier = COALESCE(NULLIF(excluded.unit_multiplier, ''), faire_products.unit_multiplier),
                    lead_time = COALESCE(NULLIF(excluded.lead_time, ''), faire_products.lead_time),
                    ships_from = COALESCE(NULLIF(excluded.ships_from, ''), faire_products.ships_from),
                    image_url = COALESCE(NULLIF(excluded.image_url, ''), faire_products.image_url),
                    variants_json = COALESCE(NULLIF(excluded.variants_json, '[]'), faire_products.variants_json),
                    category = COALESCE(NULLIF(excluded.category, ''), faire_products.category),
                    enriched = MAX(faire_products.enriched, excluded.enriched),
                    last_scraped_at = excluded.last_scraped_at,
                    last_run_id = excluded.last_run_id;
                """,
                (
                    key, product_id, variant_id, variant_name, product_url, brand_id, brand_name,
                    title, desc, wholesale_price, retail_price,
                    sku, upc, ean, barcode, moq, unit_multiplier, lead_time, ships_from,
                    image_url, variants_json, category, enriched, now, run_id,
                ),
            )

    def delete_parent_placeholder(self, product_id: str) -> None:
        """Removes generic parent card row if specific variants have been populated."""
        key = f"faire:{product_id}"
        with self.connection() as conn:
            conn.execute("DELETE FROM faire_products WHERE key = ? AND variant_name = ''", (key,))

    def upsert_brand(self, brand: Mapping[str, object]) -> None:
        brand_id = str(brand.get("brand_id") or "").strip()
        brand_name = clean_text(brand.get("brand_name") or brand.get("name"))
        brand_url = str(brand.get("brand_url") or "").strip()
        category = clean_text(brand.get("category"))
        minimum_order = clean_price(brand.get("minimum_order") or brand.get("min_order"))
        lead_time = str(brand.get("lead_time") or "").strip()
        ships_from = str(brand.get("ships_from") or "").strip()
        product_count = int(brand.get("product_count") or 0)
        now = utc_now()

        if not brand_id and not brand_name:
            return

        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO faire_brands (
                    brand_id, brand_name, brand_url, category, minimum_order,
                    lead_time, ships_from, product_count, last_scraped_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(brand_id) DO UPDATE SET
                    brand_name = COALESCE(NULLIF(excluded.brand_name, ''), faire_brands.brand_name),
                    brand_url = COALESCE(NULLIF(excluded.brand_url, ''), faire_brands.brand_url),
                    category = COALESCE(NULLIF(excluded.category, ''), faire_brands.category),
                    minimum_order = COALESCE(NULLIF(excluded.minimum_order, ''), faire_brands.minimum_order),
                    lead_time = COALESCE(NULLIF(excluded.lead_time, ''), faire_brands.lead_time),
                    ships_from = COALESCE(NULLIF(excluded.ships_from, ''), faire_brands.ships_from),
                    product_count = MAX(excluded.product_count, faire_brands.product_count),
                    last_scraped_at = excluded.last_scraped_at;
                """,
                (
                    brand_id or brand_name, brand_name, brand_url, category,
                    minimum_order, lead_time, ships_from, product_count, now
                ),
            )

    def stats(self) -> dict[str, Any]:
        with self.connection() as conn:
            total_products = conn.execute("SELECT COUNT(*) AS c FROM faire_products").fetchone()["c"]
            total_brands = conn.execute("SELECT COUNT(DISTINCT brand_name) AS c FROM faire_products WHERE brand_name != ''").fetchone()["c"]
            last_run = conn.execute("SELECT started_at FROM faire_runs ORDER BY id DESC LIMIT 1").fetchone()
            last_scraped = last_run["started_at"] if last_run else None

            # Get count of products with UPCs
            with_upc = conn.execute("SELECT COUNT(*) AS c FROM faire_products WHERE barcode != ''").fetchone()["c"]
            unenriched = conn.execute(
                "SELECT COUNT(DISTINCT product_id) AS c FROM faire_products "
                "WHERE product_id != '' AND ("
                "  enriched = 0 "
                "  OR product_title LIKE 'Product p_%' "
                "  OR product_title = 'Find similar' "
                "  OR product_title = '' "
                "  OR brand_name = 'N/A' "
                "  OR brand_name = ''"
                ")"
            ).fetchone()["c"]

            return {
                "faire_cached_products": total_products,
                "faire_total_brands": total_brands,
                "faire_with_upc": with_upc,
                "faire_unenriched": unenriched,
                "faire_last_scraped": last_scraped,
            }

    def get_products(
        self,
        page: int = 1,
        page_size: int = 25,
        search: str = "",
        brand: str = "",
    ) -> dict[str, Any]:
        offset = max(0, (page - 1) * page_size)
        where_clauses: list[str] = []
        params: list[Any] = []

        if search:
            where_clauses.append("(product_title LIKE ? OR variant_name LIKE ? OR sku LIKE ? OR upc LIKE ? OR ean LIKE ? OR barcode LIKE ? OR product_id LIKE ? OR description LIKE ?)")
            q = f"%{search.strip()}%"
            params.extend([q, q, q, q, q, q, q, q])

        if brand:
            where_clauses.append("brand_name = ?")
            params.append(brand.strip())

        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        with self.connection() as conn:
            count_row = conn.execute(f"SELECT COUNT(*) AS c FROM faire_products {where_sql}", params).fetchone()
            total = count_row["c"] if count_row else 0

            query = f"""
                SELECT key, product_id, variant_id, variant_name, product_url, brand_id, brand_name,
                       product_title, description, wholesale_price, retail_price,
                       sku, upc, ean, barcode, moq, unit_multiplier, lead_time, ships_from,
                       image_url, category, enriched, last_scraped_at
                FROM faire_products
                {where_sql}
                ORDER BY last_scraped_at DESC
                LIMIT ? OFFSET ?
            """
            rows = conn.execute(query, [*params, page_size, offset]).fetchall()

            return {
                "items": [dict(r) for r in rows],
                "total": total,
                "page": page,
                "page_size": page_size,
                "total_pages": max(1, (total + page_size - 1) // page_size),
            }

    def get_brands(self) -> list[str]:
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT DISTINCT brand_name FROM faire_products WHERE brand_name != '' ORDER BY brand_name ASC"
            ).fetchall()
            return [r["brand_name"] for r in rows]

    def reset_products(self) -> None:
        with self.connection() as conn:
            conn.execute("DELETE FROM faire_products;")
            conn.execute("DELETE FROM faire_brands;")

    def start_run(self, run_id: str, mode: str, target_url: str) -> None:
        with self.connection() as conn:
            conn.execute(
                "INSERT INTO faire_runs (run_id, mode, target_url, status, started_at) VALUES (?, ?, ?, 'running', ?)",
                (run_id, mode, target_url, utc_now()),
            )

    def finish_run(self, run_id: str, status: str, items_scraped: int) -> None:
        with self.connection() as conn:
            conn.execute(
                "UPDATE faire_runs SET status=?, items_scraped=?, finished_at=? WHERE run_id=?",
                (status, items_scraped, utc_now(), run_id),
            )

    def export_csv(self) -> Iterator[str]:
        output = io.StringIO()
        # Prepend UTF-8 BOM so Microsoft Excel correctly displays characters without encoding issues
        output.write("\ufeff")
        writer = csv.writer(output)
        writer.writerow(FAIRE_EXPORT_COLUMNS)
        yield output.getvalue()
        output.seek(0)
        output.truncate(0)

        with self.connection() as conn:
            cursor = conn.execute(
                """
                SELECT brand_name, product_title, variant_name, sku, upc, ean, barcode,
                       wholesale_price, retail_price, moq, unit_multiplier, product_url,
                       image_url, description, lead_time, ships_from, last_scraped_at
                FROM faire_products
                ORDER BY brand_name ASC, product_title ASC, variant_name ASC
                """
            )
            while True:
                rows = cursor.fetchmany(100)
                if not rows:
                    break
                for row in rows:
                    row_upc = str(row["upc"] or "").strip()
                    row_ean = str(row["ean"] or "").strip()
                    if not row_upc and not row_ean and row["barcode"]:
                        row_upc, row_ean = classify_barcode(row["barcode"])
                    writer.writerow([
                        row["brand_name"],
                        row["product_title"],
                        row["variant_name"],
                        row["sku"],
                        row_upc,
                        row_ean,
                        f"${row['wholesale_price']}" if row["wholesale_price"] else "",
                        f"${row['retail_price']}" if row["retail_price"] else "",
                        row["moq"],
                        row["unit_multiplier"],
                        row["product_url"],
                        row["image_url"],
                        row["description"],
                        row["lead_time"],
                        row["ships_from"],
                        row["last_scraped_at"],
                    ])
                yield output.getvalue()
                output.seek(0)
                output.truncate(0)
