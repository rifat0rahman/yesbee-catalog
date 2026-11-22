from __future__ import annotations

import csv
import io
import re
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping


EXPORT_COLUMNS = (
    "Brand",
    "Description",
    "Supply Price",
    "Barcode",
    "Lead Time",
    "MOQ",
    "Subtotal Price",
    "Image URL",
)


def clean_brand(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    # Strip Korean brand suffix if present, normalize multiple spaces
    return re.sub(r"\s+", " ", re.split(r"[\uac00-\ud7af]", text, maxsplit=1)[0]).strip()


def clean_number(value: object, *, integer: bool = False) -> str:
    text = str(value or "").replace(",", "").strip()
    text = re.sub(r"(?i)\b(?:USD|EA|\$|KRW|₩)\b", "", text).strip()
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return ""
    try:
        number = float(match.group())
        if integer:
            return str(int(number))
        # Keep 2 decimal places if there was a fraction, else keep neat number
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


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class CatalogDatabase:
    def __init__(self, path: str | Path = "yesbee_catalog.db") -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path, timeout=15.0)
        connection.row_factory = sqlite3.Row
        # Enable Write-Ahead Logging (WAL) and fast busy handlers for concurrent access
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
                CREATE TABLE IF NOT EXISTS products (
                    key TEXT PRIMARY KEY,
                    product_id TEXT NOT NULL DEFAULT '',
                    product_url TEXT NOT NULL DEFAULT '',
                    brand TEXT NOT NULL DEFAULT '',
                    description TEXT NOT NULL DEFAULT '',
                    supply_price TEXT NOT NULL DEFAULT '',
                    barcode TEXT NOT NULL DEFAULT '',
                    lead_time TEXT NOT NULL DEFAULT '',
                    moq TEXT NOT NULL DEFAULT '',
                    subtotal_price TEXT NOT NULL DEFAULT '',
                    image_url TEXT NOT NULL DEFAULT '',
                    enriched INTEGER NOT NULL DEFAULT 1,
                    last_scraped_at TEXT NOT NULL,
                    run_id TEXT NOT NULL DEFAULT ''
                );
                CREATE INDEX IF NOT EXISTS idx_products_barcode ON products(barcode);
                CREATE INDEX IF NOT EXISTS idx_products_product_id ON products(product_id);
                CREATE INDEX IF NOT EXISTS idx_products_brand ON products(brand);

                CREATE TABLE IF NOT EXISTS scrape_runs (
                    run_id TEXT PRIMARY KEY,
                    mode TEXT NOT NULL,
                    state TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    scraped_count INTEGER NOT NULL DEFAULT 0,
                    total_count INTEGER NOT NULL DEFAULT 0,
                    error TEXT
                );
                """
            )
            # Automatic schema migration for existing databases
            existing_cols = [row[1] for row in conn.execute("PRAGMA table_info(products)").fetchall()]
            if "enriched" not in existing_cols:
                conn.execute("ALTER TABLE products ADD COLUMN enriched INTEGER NOT NULL DEFAULT 0")

            run_cols = [row[1] for row in conn.execute("PRAGMA table_info(scrape_runs)").fetchall()]
            if "total_count" not in run_cols:
                conn.execute("ALTER TABLE scrape_runs ADD COLUMN total_count INTEGER NOT NULL DEFAULT 0")

            conn.commit()

    def reset_products(self) -> None:
        with self.connection() as connection:
            connection.execute("DELETE FROM products")

    @staticmethod
    def product_key(product: Mapping[str, object]) -> str:
        # Prioritize stable product_id or normalized URL over barcode so key never changes between discovery and enrich
        product_id = str(product.get("product_id") or "").strip()
        product_url = str(product.get("product_url") or "").strip()
        barcode = clean_barcode(product.get("barcode"))
        return product_id or product_url or barcode

    def has_product(self, product: Mapping[str, object], *, require_enriched: bool = False) -> bool:
        key = self.product_key(product)
        product_id = str(product.get("product_id") or "").strip()
        product_url = str(product.get("product_url") or "").strip()
        barcode = clean_barcode(product.get("barcode"))

        if not (key or product_id or product_url or barcode):
            return False

        query = "SELECT enriched FROM products WHERE key=? OR (product_id != '' AND product_id=?) OR (product_url != '' AND product_url=?) OR (barcode != '' AND barcode=?)"
        params = (key, product_id, product_url, barcode)

        with self.connection() as connection:
            row = connection.execute(query, params).fetchone()
            if not row:
                return False
            if require_enriched:
                return bool(row["enriched"])
            return True

    def upsert_product(
        self,
        product: Mapping[str, object],
        run_id: str,
        *,
        enriched: bool = False,
    ) -> None:
        barcode = clean_barcode(product.get("barcode"))
        product_id = str(product.get("product_id") or "").strip()
        product_url = str(product.get("product_url") or "").strip()
        key = self.product_key(product)

        if not key:
            raise ValueError("A product must have a product_id, product_url, or barcode")

        # Ensure product_url is normalized to https://www.yesbee.com/product/{product_id}/ if product_id is available
        if product_id and not product_url:
            product_url = f"https://www.yesbee.com/product/{product_id}/"
        elif product_url and not product_id:
            match = re.search(r"/product/([0-9]+)", product_url)
            if match:
                product_id = match.group(1)
                product_url = f"https://www.yesbee.com/product/{product_id}/"

        brand = clean_brand(product.get("brand"))
        description = str(product.get("description") or "").strip()
        supply_price = clean_number(product.get("supply_price"))
        lead_time = str(product.get("lead_time") or "").strip()
        moq = clean_number(product.get("moq"), integer=True)
        subtotal_price = clean_number(product.get("subtotal_price"))
        if not subtotal_price and supply_price and moq:
            try:
                sp = float(supply_price)
                mq = int(moq)
                if sp > 0 and mq > 0:
                    subtotal_price = f"{sp * mq:.2f}"
            except Exception:
                pass

        image_url = str(product.get("image_url") or "").strip()
        now = utc_now()

        with self.connection() as connection:
            # Check if this product already exists under key, product_id, product_url, or barcode
            existing = connection.execute(
                """
                SELECT key, brand, description, supply_price, barcode, lead_time,
                       moq, subtotal_price, image_url, enriched
                FROM products
                WHERE key=? OR (product_id != '' AND product_id=?) OR (product_url != '' AND product_url=?)
                """,
                (key, product_id, product_url),
            ).fetchone()

            if existing:
                actual_key = existing["key"]
                # Merge existing fields if new values are empty
                final_brand = brand or existing["brand"]
                final_desc = description or existing["description"]
                final_price = supply_price or existing["supply_price"]
                final_barcode = barcode or existing["barcode"]
                final_lead = lead_time or existing["lead_time"]
                final_moq = moq or existing["moq"]
                final_subtotal = subtotal_price or existing["subtotal_price"]
                if not final_subtotal and final_price and final_moq:
                    try:
                        final_subtotal = f"{float(final_price) * int(final_moq):.2f}"
                    except Exception:
                        pass
                final_img = image_url or existing["image_url"]
                final_enriched = 1 if (enriched or final_lead or existing["enriched"]) else 0

                connection.execute(
                    """
                    UPDATE products SET
                        product_id = COALESCE(NULLIF(?, ''), product_id),
                        product_url = COALESCE(NULLIF(?, ''), product_url),
                        brand = ?,
                        description = ?,
                        supply_price = ?,
                        barcode = ?,
                        lead_time = ?,
                        moq = ?,
                        subtotal_price = ?,
                        image_url = ?,
                        enriched = ?,
                        last_scraped_at = ?,
                        run_id = ?
                    WHERE key = ?
                    """,
                    (
                        product_id,
                        product_url,
                        final_brand,
                        final_desc,
                        final_price,
                        final_barcode,
                        final_lead,
                        final_moq,
                        final_subtotal,
                        final_img,
                        final_enriched,
                        now,
                        run_id,
                        actual_key,
                    ),
                )
            else:
                connection.execute(
                    """
                    INSERT INTO products (
                        key, product_id, product_url, brand, description, supply_price,
                        barcode, lead_time, moq, subtotal_price,
                        image_url, enriched, last_scraped_at, run_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        key,
                        product_id,
                        product_url,
                        brand,
                        description,
                        supply_price,
                        barcode,
                        lead_time,
                        moq,
                        subtotal_price,
                        image_url,
                        1 if enriched else 0,
                        now,
                        run_id,
                    ),
                )

    def upsert_products(
        self,
        products: list[Mapping[str, object]],
        run_id: str,
        *,
        enriched: bool = False,
    ) -> int:
        """Write a group of normalized products in one SQLite transaction."""
        if not products:
            return 0

        rows: list[tuple[Any, ...]] = []
        now = utc_now()

        for product in products:
            barcode = clean_barcode(product.get("barcode"))
            product_id = str(product.get("product_id") or "").strip()
            product_url = str(product.get("product_url") or "").strip()
            key = self.product_key(product)
            if not key:
                continue

            if product_id and not product_url:
                product_url = f"https://www.yesbee.com/product/{product_id}/"
            elif product_url and not product_id:
                match = re.search(r"/product/([0-9]+)", product_url)
                if match:
                    product_id = match.group(1)
                    product_url = f"https://www.yesbee.com/product/{product_id}/"

            brand = clean_brand(product.get("brand"))
            description = str(product.get("description") or "").strip()
            supply_price = clean_number(product.get("supply_price"))
            lead_time = str(product.get("lead_time") or "").strip()
            moq = clean_number(product.get("moq"), integer=True)
            subtotal_price = clean_number(product.get("subtotal_price"))
            if not subtotal_price and supply_price and moq:
                try:
                    sp = float(supply_price)
                    mq = int(moq)
                    if sp > 0 and mq > 0:
                        subtotal_price = f"{sp * mq:.2f}"
                except (TypeError, ValueError, OverflowError):
                    pass

            image_url = str(product.get("image_url") or "").strip()
            row_enriched = 1 if (enriched or bool(product.get("_enriched")) or lead_time) else 0
            rows.append(
                (
                    key,
                    product_id,
                    product_url,
                    brand,
                    description,
                    supply_price,
                    barcode,
                    lead_time,
                    moq,
                    subtotal_price,
                    image_url,
                    1 if row_enriched else 0,
                    now,
                    run_id,
                )
            )

        if not rows:
            return 0

        with self.connection() as connection:
            connection.executemany(
                """
                INSERT INTO products (
                    key, product_id, product_url, brand, description, supply_price,
                    barcode, lead_time, moq, subtotal_price,
                    image_url, enriched, last_scraped_at, run_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    product_id = COALESCE(NULLIF(excluded.product_id, ''), products.product_id),
                    product_url = COALESCE(NULLIF(excluded.product_url, ''), products.product_url),
                    brand = COALESCE(NULLIF(excluded.brand, ''), products.brand),
                    description = COALESCE(NULLIF(excluded.description, ''), products.description),
                    supply_price = COALESCE(NULLIF(excluded.supply_price, ''), products.supply_price),
                    barcode = COALESCE(NULLIF(excluded.barcode, ''), products.barcode),
                    lead_time = COALESCE(NULLIF(excluded.lead_time, ''), products.lead_time),
                    moq = COALESCE(NULLIF(excluded.moq, ''), products.moq),
                    subtotal_price = COALESCE(NULLIF(excluded.subtotal_price, ''), products.subtotal_price),
                    image_url = COALESCE(NULLIF(excluded.image_url, ''), products.image_url),
                    enriched = CASE
                        WHEN excluded.enriched = 1 OR products.enriched = 1 THEN 1
                        ELSE 0
                    END,
                    last_scraped_at = excluded.last_scraped_at,
                    run_id = excluded.run_id
                """,
                rows,
            )
        return len(rows)

    def start_run(self, run_id: str, mode: str) -> None:
        with self.connection() as connection:
            connection.execute(
                "INSERT INTO scrape_runs (run_id, mode, state, started_at) VALUES (?, ?, 'running', ?)",
                (run_id, mode, utc_now()),
            )

    def update_run_counts(self, run_id: str, scraped: int, total: int) -> None:
        with self.connection() as connection:
            connection.execute(
                "UPDATE scrape_runs SET scraped_count=?, total_count=? WHERE run_id=?",
                (scraped, total, run_id),
            )

    def finish_run(self, run_id: str, state: str, count: int, total: int = 0, error: str | None = None) -> None:
        with self.connection() as connection:
            connection.execute(
                """
                UPDATE scrape_runs
                SET state=?, finished_at=?, scraped_count=?, total_count=?, error=?
                WHERE run_id=?
                """,
                (state, utc_now(), count, total, error, run_id),
            )

    def stats(self) -> dict[str, object]:
        with self.connection() as connection:
            count = connection.execute("SELECT COUNT(*) FROM products").fetchone()[0]
            latest = connection.execute("SELECT MAX(last_scraped_at) FROM products").fetchone()[0]
            incomplete = connection.execute(
                "SELECT COUNT(*) FROM scrape_runs WHERE state='running'"
            ).fetchone()[0]
            brands_count = connection.execute("SELECT COUNT(DISTINCT brand) FROM products WHERE brand != ''").fetchone()[0]
        return {
            "cached_products": count,
            "total_brands": brands_count,
            "last_full_scrape": latest,
            "incomplete_run": bool(incomplete),
        }

    def get_products(
        self,
        page: int = 1,
        page_size: int = 25,
        search: str = "",
        brand: str = "",
    ) -> dict[str, Any]:
        page = max(1, page)
        page_size = max(5, min(page_size, 100))
        offset = (page - 1) * page_size

        clauses: list[str] = []
        params: list[Any] = []

        if search.strip():
            term = f"%{search.strip()}%"
            clauses.append("(brand LIKE ? OR description LIKE ? OR barcode LIKE ? OR product_id LIKE ?)")
            params.extend([term, term, term, term])

        if brand.strip():
            clauses.append("brand = ?")
            params.append(brand.strip())

        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""

        with self.connection() as connection:
            total = connection.execute(f"SELECT COUNT(*) FROM products {where}", params).fetchone()[0]
            query = f"""
                SELECT key, product_id, product_url, brand, description,
                       supply_price, barcode, lead_time, moq,
                       subtotal_price, image_url, last_scraped_at
                FROM products
                {where}
                ORDER BY last_scraped_at DESC, brand ASC
                LIMIT ? OFFSET ?
            """
            rows = connection.execute(query, (*params, page_size, offset)).fetchall()
            items = [dict(row) for row in rows]

        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": max(1, (total + page_size - 1) // page_size),
            "items": items,
        }

    def get_brands(self) -> list[str]:
        with self.connection() as connection:
            rows = connection.execute(
                "SELECT DISTINCT brand FROM products WHERE brand != '' ORDER BY brand ASC"
            ).fetchall()
            return [row[0] for row in rows]

    def get_cached_keys(self) -> set[str]:
        with self.connection() as connection:
            rows = connection.execute(
                "SELECT key, product_id, product_url, barcode FROM products"
            ).fetchall()

        keys: set[str] = set()
        for row in rows:
            for value in (row["key"], row["product_id"], row["product_url"], clean_barcode(row["barcode"])):
                if value:
                    keys.add(str(value).strip())
        return keys

    def get_enriched_keys(self) -> set[str]:
        return self.get_cached_keys()

    def get_runs(self, limit: int = 10) -> list[dict[str, Any]]:
        with self.connection() as connection:
            rows = connection.execute(
                "SELECT run_id, mode, state, started_at, finished_at, scraped_count, total_count, error FROM scrape_runs ORDER BY started_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]

    def export_csv(self) -> io.BytesIO:
        output = io.StringIO(newline="")
        writer = csv.DictWriter(output, fieldnames=EXPORT_COLUMNS, lineterminator="\n")
        writer.writeheader()
        with self.connection() as connection:
            rows = connection.execute(
                """
                SELECT brand, description, supply_price, barcode, lead_time, moq,
                       subtotal_price, image_url
                FROM products ORDER BY brand, description, barcode
                """
            )
            for row in rows:
                writer.writerow(dict(zip(EXPORT_COLUMNS, row)))
        # Include UTF-8 BOM (\ufeff) so Excel auto-detects encoding properly
        return io.BytesIO(("\ufeff" + output.getvalue()).encode("utf-8"))
