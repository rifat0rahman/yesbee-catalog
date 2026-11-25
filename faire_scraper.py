from __future__ import annotations

import asyncio
import json
import logging
import random
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Awaitable, Callable
from urllib.parse import urljoin, urlparse

from playwright.async_api import BrowserContext, Page, Response, async_playwright

from faire_database import FaireDatabase, clean_barcode, clean_price, clean_text, classify_barcode


LOGGER = logging.getLogger("faire")
ROOT = Path(__file__).resolve().parent
FAIRE_BASE_URL = "https://www.faire.com/"
FAIRE_LOGIN_URL = "https://www.faire.com/?signin=1"


@dataclass
class FaireScrapeStatus:
    state: str = "idle"  # idle, logging_in, scraping, stopping, completed, error
    current_target: str = ""
    current_brand: str = ""
    current_item: str = ""
    scraped_count: int = 0
    total_estimated: int = 0
    started_at: float | None = None
    eta_seconds: float | None = None
    speed_items_per_sec: float = 0.0
    cooldown_seconds: int = 0
    recent_logs: list[dict[str, str]] = field(default_factory=list)

    def log(self, message: str, level: str = "info") -> None:
        timestamp = datetime.now().strftime("%H:%M:%S")
        entry = {"time": timestamp, "message": message, "level": level.lower()}
        self.recent_logs.append(entry)
        del self.recent_logs[:-80]
        if level.lower() == "error":
            LOGGER.error(message)
        elif level.lower() == "warn":
            LOGGER.warning(message)
        else:
            LOGGER.info(message)

    def as_dict(self) -> dict[str, Any]:
        elapsed = 0.0
        if self.started_at is not None:
            try:
                loop = asyncio.get_running_loop()
                elapsed = max(0.0, loop.time() - self.started_at)
            except RuntimeError:
                pass
        return {
            "state": self.state,
            "current_target": self.current_target,
            "current_brand": self.current_brand,
            "current_item": self.current_item,
            "scraped_count": self.scraped_count,
            "total_estimated": self.total_estimated,
            "started_at": self.started_at,
            "elapsed_seconds": round(elapsed, 1),
            "eta_seconds": self.eta_seconds if self.state in {"scraping", "running"} else None,
            "speed": round(self.speed_items_per_sec, 1),
            "cooldown_seconds": self.cooldown_seconds if self.state in {"scraping", "running"} else 0,
            "recent_logs": self.recent_logs,
        }


@dataclass(frozen=True)
class FaireScrapeOptions:
    targets: list[str]  # Brand URLs or Category URLs
    mode: str = "brand"  # 'brand', 'category', or 'bulk'
    scrape_mode: str = "fresh"  # 'fresh', 'resume', or 'enrich_all'
    headless: bool = True
    deep_enrich: bool = False
    batch_limit: int = 0  # 0 = all unenriched, >0 = limit batch size
    proxy: str = ""
    storage_path: Path = Path("faire_storage_state.json")


class FaireScraper:
    def __init__(self, database: FaireDatabase, status: FaireScrapeStatus) -> None:
        self.database = database
        self.status = status
        self._stop_requested = False
        self._captured_keys: set[str] = set()

    def request_stop(self) -> None:
        self._stop_requested = True
        self.status.state = "stopping"
        self.status.log("Stop requested. Finishing in-flight operations gracefully...", "warn")

    async def run(self, options: FaireScrapeOptions) -> None:
        self._stop_requested = False
        self.status.state = "scraping"
        self.status.started_at = asyncio.get_running_loop().time()
        self.status.scraped_count = 0
        self.status.cooldown_seconds = 0
        run_id = f"faire_{int(datetime.now().timestamp())}"

        targets = [t.strip() for t in options.targets if t.strip()]
        if not targets:
            # Default to Glowiss example if none specified
            targets = ["https://www.faire.com/brand/b_56wn63pr"]

        target_summary = targets[0] if len(targets) == 1 else f"{len(targets)} targets"
        self.database.start_run(run_id, options.mode, target_summary)
        self.status.log(f"Starting Faire scrape for {len(targets)} target(s) in '{options.mode}' mode.")

        async with async_playwright() as pw:
            launch_args = [
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ]
            launch_kwargs: dict[str, Any] = {
                "headless": options.headless,
                "args": launch_args,
            }
            if options.proxy:
                launch_kwargs["proxy"] = {"server": options.proxy}

            browser = await pw.chromium.launch(**launch_kwargs)

            context_kwargs: dict[str, Any] = {
                "viewport": {"width": 1920, "height": 1080},
                "user_agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/131.0.0.0 Safari/537.36"
                ),
                "locale": "en-US",
                "timezone_id": "America/New_York",
            }

            if options.storage_path.exists():
                try:
                    content = options.storage_path.read_text(encoding="utf-8")
                    if content and ("cookies" in content or "origins" in content):
                        context_kwargs["storage_state"] = str(options.storage_path)
                        self.status.log("Loaded authenticated session state from faire_storage_state.json", "info")
                except Exception as e:
                    self.status.log(f"Could not load session state: {e}", "warn")
            else:
                self.status.log("No saved faire_storage_state.json found. Operating in public session mode.", "warn")

            context = await browser.new_context(**context_kwargs)
            # Hide webdriver flag to evade standard bot detection
            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                window.chrome = { runtime: {} };
            """)

            page = await context.new_page()

            # Attach Network Interception to capture API JSON payloads
            page.on("response", lambda resp: asyncio.create_task(self._handle_network_response(resp, run_id)))

            try:
                if options.scrape_mode == "enrich_all":
                    limit_msg = f"next {options.batch_limit} unenriched products" if options.batch_limit > 0 else "all unenriched products"
                    self.status.log(f"Batch Enrich Mode: Skipping brand discovery, enriching {limit_msg}...", "info")
                    await self._enrich_product_details(page, run_id, options)
                elif options.scrape_mode == "resume" and self.database.stats()["faire_cached_products"] > 0:
                    cached_count = self.database.stats()["faire_cached_products"]
                    self.status.log(
                        f"Resume Mode: Found {cached_count} existing products in catalog. "
                        "Skipping discovery and resuming enrichment directly...",
                        "info"
                    )
                    await self._enrich_product_details(page, run_id, options)
                else:
                    for idx, target_url in enumerate(targets, start=1):
                        if self._stop_requested:
                            break

                        self.status.current_target = target_url
                        self.status.log(f"Processing target [{idx}/{len(targets)}]: {target_url}")

                        if "/category/" in target_url or options.mode == "category":
                            await self._scrape_category(page, target_url, run_id, options)
                        else:
                            await self._scrape_brand(page, target_url, run_id, options)

                    # Ensure all products in catalog are enriched (especially in Resume mode)
                    if not self._stop_requested:
                        await self._enrich_product_details(page, run_id, options)

                # Persist updated session state if still valid
                try:
                    await context.storage_state(path=str(options.storage_path))
                except Exception:
                    pass

                final_status = "stopped" if self._stop_requested else "completed"
                self.database.finish_run(run_id, final_status, self.status.scraped_count)
                self.status.state = final_status
                self.status.log(
                    f"Faire scraping finished with status: {final_status}. Total items scraped: {self.status.scraped_count}.",
                    "info"
                )

            except asyncio.CancelledError:
                self.status.state = "stopped"
                self.database.finish_run(run_id, "stopped", self.status.scraped_count)
                self.status.log("Scrape was stopped by user.", "warn")
            except Exception as e:
                self.status.state = "error"
                self.database.finish_run(run_id, "error", self.status.scraped_count)
                self.status.log(f"Unexpected error during scraping: {e}", "error")
            finally:
                self.status.cooldown_seconds = 0
                await context.close()
                await browser.close()

    async def _handle_network_response(self, response: Response, run_id: str) -> None:
        """Intercepts internal JSON/GraphQL network calls from Faire."""
        try:
            url = response.url
            if not response.ok:
                return

            # Check if this is an API endpoint, Next.js data route, or GraphQL call
            content_type = response.headers.get("content-type", "")
            if "application/json" not in content_type:
                return

            if not any(k in url for k in ["/api/", "/external-api/", "/graphql", "brand-view", "products", "search", "/_next/data", "/brand"]):
                return

            data = await response.json()
            if not isinstance(data, dict):
                return

            # Parse products from various Faire API schemas
            await self._extract_products_from_json(data, run_id)

        except Exception:
            # Ignore unparseable responses or background telemetry
            pass

    async def _extract_products_from_json(self, data: dict[str, Any], run_id: str) -> None:
        """Parses brand info and product models from intercepted JSON."""
        # Check Next.js pageProps wrapper
        page_props = data.get("pageProps", {}) if isinstance(data.get("pageProps"), dict) else {}

        # 1. Look for brand details
        brand_info = data.get("brand") or data.get("brandView") or page_props.get("brand") or page_props.get("brandView") or {}
        brand_name = ""
        brand_id = ""
        if isinstance(brand_info, dict):
            brand_name = clean_text(brand_info.get("name") or brand_info.get("brandName"))
            brand_id = str(brand_info.get("token") or brand_info.get("id") or "")
            if brand_name:
                self.status.current_brand = brand_name
                self.database.upsert_brand({
                    "brand_id": brand_id,
                    "brand_name": brand_name,
                    "brand_url": f"https://www.faire.com/brand/{brand_id}" if brand_id else "",
                    "minimum_order": brand_info.get("minimumOrderAmountCents", 0) / 100 if brand_info.get("minimumOrderAmountCents") else "",
                    "lead_time": str(brand_info.get("leadTimeDays") or ""),
                    "ships_from": str(brand_info.get("shipsFrom") or ""),
                })

        # 2. Look for product lists in various common keys
        product_list: list[dict[str, Any]] = []
        for src in [data, page_props]:
            for key in ["products", "brandProducts", "results", "items", "data"]:
                val = src.get(key)
                if isinstance(val, list):
                    product_list.extend([x for x in val if isinstance(x, dict)])
                elif isinstance(val, dict):
                    for subkey in ["products", "items", "results"]:
                        subval = val.get(subkey)
                        if isinstance(subval, list):
                            product_list.extend([x for x in subval if isinstance(x, dict)])

        for item in product_list:
            if self._stop_requested:
                break
            await self._process_single_product_dict(item, brand_name, brand_id, run_id)

    async def _process_single_product_dict(
        self,
        item: dict[str, Any],
        fallback_brand: str,
        fallback_brand_id: str,
        run_id: str,
    ) -> None:
        product_id = str(item.get("token") or item.get("id") or item.get("productToken") or "").strip()
        title = clean_text(item.get("name") or item.get("title") or item.get("productName"))
        if not title and not product_id:
            return

        # Brand from item if available
        brand_data = item.get("brand") or {}
        item_brand = clean_text(brand_data.get("name") if isinstance(brand_data, dict) else "")
        brand_name = item_brand or fallback_brand or self.status.current_brand
        brand_id = (brand_data.get("token") if isinstance(brand_data, dict) else "") or fallback_brand_id

        # Price parsing: handle both dollars and cents
        wholesale = ""
        retail = ""

        # Wholesale price (cost)
        if "wholesalePriceCents" in item and item["wholesalePriceCents"] is not None:
            wholesale = f"{item['wholesalePriceCents'] / 100:.2f}"
        elif "wholesale_price_cents" in item and item["wholesale_price_cents"] is not None:
            wholesale = f"{item['wholesale_price_cents'] / 100:.2f}"
        elif "wholesalePrice" in item:
            wholesale = clean_price(item["wholesalePrice"])
        elif "cost" in item:
            wholesale = clean_price(item["cost"])

        # Retail price (MSRP)
        if "retailPriceCents" in item and item["retailPriceCents"] is not None:
            retail = f"{item['retailPriceCents'] / 100:.2f}"
        elif "retail_price_cents" in item and item["retail_price_cents"] is not None:
            retail = f"{item['retail_price_cents'] / 100:.2f}"
        elif "retailPrice" in item:
            retail = clean_price(item["retailPrice"])
        elif "msrp" in item:
            retail = clean_price(item["msrp"])

        # MOQ / Pack Size
        moq = str(item.get("minimumOrderQuantity") or item.get("caseQuantity") or item.get("unitMultiplier") or "1")
        unit_multiplier = str(item.get("unitMultiplier") or item.get("packSize") or "")

        # Barcode & Variants
        barcode = clean_barcode(item.get("gtin") or item.get("barcode") or item.get("upc") or "")
        sku = str(item.get("sku") or "").strip()
        variants = item.get("variants") or item.get("productVariants") or []

        if isinstance(variants, list) and variants:
            for v in variants:
                if isinstance(v, dict):
                    v_barcode = clean_barcode(v.get("gtin") or v.get("barcode") or v.get("upc") or "")
                    if v_barcode and not barcode:
                        barcode = v_barcode
                    v_sku = str(v.get("sku") or "").strip()
                    if v_sku and not sku:
                        sku = v_sku
                    if not wholesale and "wholesalePriceCents" in v:
                        wholesale = f"{v['wholesalePriceCents'] / 100:.2f}"
                    if not retail and "retailPriceCents" in v:
                        retail = f"{v['retailPriceCents'] / 100:.2f}"

        # Image URL
        image_url = ""
        images = item.get("images") or item.get("media") or []
        if isinstance(images, list) and images:
            first = images[0]
            if isinstance(first, dict):
                image_url = str(first.get("url") or first.get("src") or "")
            elif isinstance(first, str):
                image_url = first
        if not image_url and "imageUrl" in item:
            image_url = str(item["imageUrl"])

        # Construct Product URL
        product_url = f"https://www.faire.com/product/{product_id}" if product_id else ""

        key = f"faire:{product_id or title}:{sku or barcode}"
        if key in self._captured_keys:
            return
        self._captured_keys.add(key)

        product_record = {
            "key": key,
            "product_id": product_id,
            "product_url": product_url,
            "brand_id": brand_id,
            "brand_name": brand_name,
            "product_title": title,
            "description": str(item.get("description") or ""),
            "wholesale_price": wholesale,
            "retail_price": retail,
            "barcode": barcode,
            "sku": sku,
            "moq": moq,
            "unit_multiplier": unit_multiplier,
            "image_url": image_url,
            "variants": variants if isinstance(variants, list) else [],
        }

        self.database.upsert_product(product_record, run_id=run_id)
        self.status.scraped_count += 1
        self.status.current_item = title
        if self.status.scraped_count % 10 == 0 or self.status.scraped_count == 1:
            self.status.log(f"Scraped {self.status.scraped_count} products (Latest: {title[:40]}...)")

    async def _scrape_brand(
        self,
        page: Page,
        brand_url: str,
        run_id: str,
        options: FaireScrapeOptions,
    ) -> None:
        self.status.log(f"Navigating to brand: {brand_url}")
        try:
            await page.goto(brand_url, wait_until="domcontentloaded", timeout=45000)
            await asyncio.sleep(2.0)
        except Exception as e:
            self.status.log(f"Failed to navigate to {brand_url}: {e}", "warn")
            return

        # Check for Cloudflare Turnstile / Bot Verification Challenge
        is_cf = False
        try:
            page_title = (await page.title() or "").lower()
            body_text = (await page.inner_text("body") or "").lower()
            if (
                "just a moment..." in page_title
                or "verify you are human" in body_text
                or "checking your browser" in body_text
                or await page.query_selector("iframe[src*='cloudflare']")
                or await page.query_selector("#challenge-running")
            ):
                is_cf = True
        except Exception:
            pass

        if is_cf:
            if options.headless:
                self.status.log(
                    "Cloudflare 'Verify you are human' challenge detected. Headless mode was blocked. "
                    "Please uncheck 'Headless Mode' in the UI so the browser runs visibly and allows verification.",
                    "error"
                )
                return
            else:
                self.status.log(
                    "Cloudflare verification challenge detected on screen. Please complete the verification in the open browser window. Waiting up to 45s...",
                    "warn"
                )
                for _ in range(45):
                    await asyncio.sleep(1.0)
                    cur_title = (await page.title() or "").lower()
                    if "just a moment..." not in cur_title and not await page.query_selector("#challenge-running"):
                        self.status.log("Cloudflare verification cleared!", "info")
                        await asyncio.sleep(2.0)
                        break

        # Extract brand name from document title or specific brand selectors
        try:
            doc_title = await page.title()
            if doc_title and "just a moment" not in doc_title.lower():
                cleaned_brand = re.split(r"(?i)\s+(?:wholesale products|wholesale|on faire|- faire|\| faire)", doc_title)[0].strip()
                if cleaned_brand and cleaned_brand.lower() not in {"www.faire.com", "faire", ""}:
                    self.status.current_brand = cleaned_brand
                    self.status.log(f"Identified Brand: {cleaned_brand}")

            if not self.status.current_brand:
                h1s = await page.query_selector_all("h1")
                for h in h1s:
                    txt = (await h.text_content() or "").strip()
                    if txt and txt.lower() not in {"www.faire.com", "faire", "just a moment..."}:
                        self.status.current_brand = txt
                        self.status.log(f"Identified Brand: {txt}")
                        break
        except Exception:
            pass

        # Check for expected total product count on the page
        expected_total = 0
        try:
            expected_total = await page.evaluate("""
                () => {
                    const text = document.body.innerText || "";
                    const m = text.match(/\\b(\\d+)\\s+(?:wholesale products|products|items|results)\\b/i);
                    if (m) return parseInt(m[1], 10);
                    const tm = document.title.match(/\\((\\d+)\\)/);
                    if (tm) return parseInt(tm[1], 10);
                    return 0;
                }
            """)
            if expected_total > 0:
                self.status.total_estimated = expected_total
                self.status.log(f"Catalog size detected from page: {expected_total} products expected.")
        except Exception:
            pass

        # Perform human-like infinite scrolling and click "Show more" to load ALL product batches
        last_count = self.status.scraped_count
        no_change_count = 0
        scroll_attempts = 0
        max_scrolls = 100

        show_more_selectors = [
            "button:has-text('Show more')",
            "button:has-text('Load more')",
            "button:has-text('View more')",
            "a:has-text('Show more')",
            "a:has-text('Load more')",
            "[data-test*='show-more']",
            "[data-test*='load-more']",
            "button[class*='showMore']",
            "button[class*='loadMore']",
            "[aria-label*='Show more']",
        ]

        while not self._stop_requested and scroll_attempts < max_scrolls:
            scroll_attempts += 1
            # Step 1: Incremental scrolling down to trigger virtualized elements and IntersectionObservers
            for _ in range(3):
                await page.evaluate("window.scrollBy(0, window.innerHeight * 0.75);")
                await asyncio.sleep(0.35)

            # Step 2: Harvest DOM cards currently visible or rendered
            await self._scrape_dom_product_cards(page, run_id)

            # Step 3: Check for "Show more" button in current view
            show_more_clicked = False
            for sel in show_more_selectors:
                try:
                    btn = await page.query_selector(sel)
                    if btn and await btn.is_visible():
                        self.status.log("Found 'Show more' button. Loading next batch of products...")
                        await btn.scroll_into_view_if_needed()
                        await asyncio.sleep(0.3)
                        await btn.click()
                        show_more_clicked = True
                        no_change_count = 0
                        await asyncio.sleep(2.5)
                        await self._scrape_dom_product_cards(page, run_id)
                        break
                except Exception:
                    pass

            # Step 4: If button wasn't found in current viewport, scroll to bottom to reveal it
            if not show_more_clicked:
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
                await asyncio.sleep(0.7)
                for sel in show_more_selectors:
                    try:
                        btn = await page.query_selector(sel)
                        if btn and await btn.is_visible():
                            self.status.log("Found 'Show more' button at bottom. Loading next batch...")
                            await btn.click()
                            show_more_clicked = True
                            no_change_count = 0
                            await asyncio.sleep(2.5)
                            await self._scrape_dom_product_cards(page, run_id)
                            break
                    except Exception:
                        pass

            current_count = self.status.scraped_count
            if current_count == last_count and not show_more_clicked:
                no_change_count += 1
                threshold = 8 if (expected_total > 0 and current_count < expected_total) else 4
                if no_change_count >= threshold:
                    # Final check: scroll all the way to bottom and wait
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
                    await asyncio.sleep(1.8)
                    if self.status.scraped_count == current_count:
                        self.status.log(f"All product batches loaded for {brand_url}. Discovered {current_count} products.")
                        break
            else:
                if current_count != last_count:
                    no_change_count = 0
                last_count = current_count

        # Step 5: Multi-pass sweep across page heights (0%, 25%, 50%, 75%, 100%)
        # to ensure any virtualized or off-screen items are 100% captured
        self.status.log("Running comprehensive catalog sweep to ensure zero missed products...")
        for pct in [0.0, 0.25, 0.5, 0.75, 1.0]:
            if self._stop_requested:
                break
            await page.evaluate(f"window.scrollTo(0, document.body.scrollHeight * {pct});")
            await asyncio.sleep(0.4)
            await self._scrape_dom_product_cards(page, run_id)

        self.status.log(f"Finished brand catalog discovery for {brand_url}. Total harvested: {self.status.scraped_count} items.")

        # Automatically enrich products with actual title, dropdown variants, SKU, UPC, and EAN
        if not self._stop_requested:
            await self._enrich_product_details(page, run_id, options)

    async def _scrape_dom_product_cards(self, page: Page, run_id: str) -> None:
        """Parses visible DOM product cards to extract real titles and high-res images."""
        try:
            cards_data = await page.evaluate(
                """
                () => {
                    const results = [];
                    const seenTokens = new Set();
                    const anchors = document.querySelectorAll("a[href*='/product/']");

                    const countUniqueProductTokens = (el) => {
                        const tokens = new Set();
                        const subLinks = el.querySelectorAll("a[href*='/product/']");
                        for (const sl of subLinks) {
                            const m = (sl.getAttribute("href") || "").match(/p_[a-zA-Z0-9]+/);
                            if (m) tokens.add(m[0]);
                            if (tokens.size > 1) return tokens.size;
                        }
                        return tokens.size;
                    };

                    for (const a of anchors) {
                        const href = a.getAttribute("href") || "";
                        const match = href.match(/p_[a-zA-Z0-9]+/);
                        if (!match) continue;
                        const token = match[0];
                        if (seenTokens.has(token)) continue;
                        seenTokens.add(token);

                        // Walk up to the outermost single-product tile container (stops before hitting a multi-product grid)
                        let container = a;
                        for (let depth = 0; depth < 8; depth++) {
                            const parent = container.parentElement;
                            if (!parent || parent.tagName === "BODY" || parent.tagName === "MAIN") break;
                            if (countUniqueProductTokens(parent) > 1) break;
                            container = parent;
                        }

                        // Find image: check srcset first, then data-src, then src
                        let imgSrc = "";
                        let imgAlt = "";
                        const img = container.querySelector("img") || a.querySelector("img");
                        if (img) {
                            imgAlt = (img.getAttribute("alt") || "").trim();
                            const srcset = img.getAttribute("srcset") || img.getAttribute("data-srcset") || "";
                            if (srcset) {
                                const parts = srcset.split(",").map(p => p.trim().split(" ")[0]).filter(Boolean);
                                if (parts.length > 0) imgSrc = parts[parts.length - 1];
                            }
                            if (!imgSrc || imgSrc.startsWith("data:")) {
                                imgSrc = img.getAttribute("src") || img.getAttribute("data-src") || "";
                            }
                            if (imgSrc.startsWith("//")) imgSrc = "https:" + imgSrc;
                            if (imgSrc.startsWith("data:")) imgSrc = "";
                        }

                        // Brand name (present on category/search product cards)
                        let brandName = "";
                        const brandAnchor = container.querySelector("a[href*='/brand/'], [data-test-id*='brand' i], [data-test*='brand' i]");
                        if (brandAnchor) {
                            brandName = (brandAnchor.innerText || "").trim();
                        }

                        // Find product title: extract clean title line from full tile text
                        let title = "";
                        const uiNoiseRe = /^(?:Find similar|Quick shop|Quick view|Add to cart|Save|Favorite|Shop now|New|Bestseller|Best seller|Sale|Featured|Top Shop|Trending|Staff Pick)$/i;
                        const rawText = container.innerText || a.innerText || "";
                        const lines = rawText.split("\\n").map(s => s.trim()).filter(Boolean);
                        for (const l of lines) {
                            // Skip brand name line if it appears before product title
                            if (brandName && l.toLowerCase() === brandName.toLowerCase()) continue;
                            // Skip UI hover buttons & badges
                            if (uiNoiseRe.test(l)) continue;
                            // Skip price lines
                            if (/^\\s*[$£€]?\\s*[\\d,]+(?:\\.\\d+)?\\s*$/i.test(l)) continue;
                            // Skip MSRP / currency prefixes & promo lines
                            if (/^(?:MSRP|RRP|USD|CAD|EUR|GBP|Free shipping|Ships from|Min\\s*\\$)/i.test(l)) continue;
                            // Skip case pack / MOQ indicators
                            if (/^(?:Case|Pack|Min|MOQ)\\b/i.test(l)) continue;
                            // Skip stock / scarcity warnings
                            if (/^Only \\d+ (?:cases?|items?|units?) left/i.test(l)) continue;
                            // The first remaining line with length >= 3 is the exact product title!
                            if (l.length >= 3) {
                                title = l;
                                break;
                            }
                        }
                        if (!title && imgAlt && !imgAlt.startsWith("http") && !uiNoiseRe.test(imgAlt) && imgAlt.length > 3) {
                            title = imgAlt;
                        }
                        if (!title) {
                            const titleEl = container.querySelector("h2, h3, h4, p[class*='title'], div[class*='title'], [data-test*='title'], [data-test*='name']");
                            if (titleEl) {
                                const t = titleEl.textContent.trim();
                                if (!uiNoiseRe.test(t)) title = t;
                            }
                        }

                        // Prices
                        const prices = rawText.match(/\\$\\s*(\\d+(?:\\.\\d{2})?)/g) || [];
                        const wholesale = prices.length >= 1 ? prices[0].replace(/[^0-9.]/g, "") : "";
                        const retail = prices.length >= 2 ? prices[1].replace(/[^0-9.]/g, "") : "";

                        // MOQ
                        let moq = "1";
                        const moqMatch = rawText.match(/(?:Case of|Pack of|Min|MOQ)[:\\s]*(\\d+)/i);
                        if (moqMatch) moq = moqMatch[1];

                        results.push({
                            product_id: token,
                            product_url: "https://www.faire.com/product/" + token,
                            product_title: title || ("Product " + token),
                            brand_name: brandName,
                            wholesale_price: wholesale,
                            retail_price: retail,
                            image_url: imgSrc,
                            moq: moq
                        });
                    }
                    return results;
                }
                """
            )

            for item in cards_data:
                if self._stop_requested:
                    break
                p_id = item["product_id"]
                key = f"faire:{p_id}"
                if key in self._captured_keys:
                    # Update fields if new data has better image or clean title (indexed lookup by product_id)
                    with self.database.connection() as conn:
                        old_row = conn.execute(
                            "SELECT image_url, product_title, wholesale_price, retail_price, brand_name, moq "
                            "FROM faire_products WHERE product_id = ? LIMIT 1",
                            (p_id,),
                        ).fetchone()
                    if old_row:
                        old = dict(old_row)
                        need_update = False
                        new_img = old.get("image_url") or item["image_url"]
                        new_title = item["product_title"] if (not old.get("product_title", "").startswith("Product p_") or not item["product_title"].startswith("Product p_")) else old.get("product_title")

                        if (
                            (not old.get("image_url") and item["image_url"])
                            or ((old.get("product_title", "").startswith("Product p_") or old.get("product_title", "") == "Find similar") and not item["product_title"].startswith("Product p_") and item["product_title"] != "Find similar")
                            or (not old.get("wholesale_price") and item["wholesale_price"])
                            or (not old.get("brand_name") and item.get("brand_name"))
                        ):
                            need_update = True

                        if need_update:
                            self.database.upsert_product({
                                "key": key,
                                "product_id": p_id,
                                "product_url": item["product_url"],
                                "brand_name": item.get("brand_name") or self.status.current_brand,
                                "product_title": new_title,
                                "wholesale_price": item["wholesale_price"] or old.get("wholesale_price", ""),
                                "retail_price": item["retail_price"] or old.get("retail_price", ""),
                                "image_url": new_img,
                                "moq": item["moq"] or old.get("moq", "1")
                            }, run_id=run_id)
                    continue

                self._captured_keys.add(key)
                self.database.upsert_product({
                    "key": key,
                    "product_id": p_id,
                    "product_url": item["product_url"],
                    "brand_name": item.get("brand_name") or self.status.current_brand,
                    "product_title": item["product_title"],
                    "wholesale_price": item["wholesale_price"],
                    "retail_price": item["retail_price"],
                    "image_url": item["image_url"],
                    "moq": item["moq"]
                }, run_id=run_id)
                self.status.scraped_count += 1
                self.status.current_item = item["product_title"]
                if self.status.scraped_count % 10 == 0 or self.status.scraped_count == 1:
                    self.status.log(f"Discovered {self.status.scraped_count} products (Latest: {item['product_title'][:40]}...)")
        except Exception as e:
            LOGGER.warning(f"DOM card extraction note: {e}")

    def _parse_product_from_html(self, html: str) -> dict[str, Any] | None:
        """Parses Next.js flight data (self.__next_f), H1 titles, and __NEXT_DATA__ directly from product page HTML."""
        actual_title = ""
        description = ""
        variants_list: list[dict[str, Any]] = []
        single_sku = ""
        single_gtin = ""
        single_img = ""

        # 1. Product title: check H1 tags first (especially data-test-id="product-name")
        h1_m = re.search(r"<h1[^>]*data-test-id=[\"']product-name[\"'][^>]*>(.*?)</h1>", html, re.I | re.DOTALL)
        if not h1_m:
            h1_m = re.search(r"<h1[^>]*>(.*?)</h1>", html, re.I | re.DOTALL)
        if h1_m:
            clean_h1 = clean_text(re.sub(r"<[^>]+>", " ", h1_m.group(1)))
            if clean_h1 and not clean_h1.lower().startswith("just a moment") and clean_h1 != "Faire":
                actual_title = clean_h1

        # Fallback to <head><title> tag (ignoring SVG icon/button titles)
        if not actual_title:
            head_m = re.search(r"<head[^>]*>(.*?)</head>", html, re.I | re.DOTALL)
            if head_m:
                doc_m = re.search(r"<title[^>]*>(.*?)</title>", head_m.group(1), re.I | re.DOTALL)
                if doc_m:
                    raw_title = clean_text(doc_m.group(1))
                    cleaned_t = re.split(r"(?i)\s+(?:-|\|)\s+Faire", raw_title)[0].strip()
                    if cleaned_t and not cleaned_t.lower().startswith("just a moment") and cleaned_t != "Faire":
                        if not any(k in cleaned_t.lower() for k in ["back button", "search icon", "filter icon"]):
                            actual_title = cleaned_t

        # 2. Extract from Next.js App Router streaming flight data: self.__next_f.push
        chunks = re.findall(r"self\.__next_f\.push\((\[.*?\])\)</script>", html, re.DOTALL)
        full_text = ""
        for c in chunks:
            try:
                parsed = json.loads(c)
                if isinstance(parsed, list) and len(parsed) >= 2 and isinstance(parsed[1], str):
                    full_text += parsed[1]
            except Exception:
                pass

        if full_text:
            if not actual_title:
                pname_m = re.search(r"\"productName\":\"([^\"]+)\"", full_text)
                if pname_m:
                    actual_title = clean_text(pname_m.group(1))

            pdesc_m = re.search(r"\"productDescription\":\"(.*?)(?<!\\)\"", full_text)
            if pdesc_m:
                raw_d = pdesc_m.group(1).replace(r"\"", '"').replace(r"\n", "\n").replace(r"\r", "")
                description = clean_text(raw_d)[:2500]

            # Check for variants/options objects matching "token":"po_..." or inside "options":{...}
            opts: dict[str, dict[str, Any]] = {}
            matches = list(re.finditer(r"\"token\":\"(po_[a-zA-Z0-9]+)\"", full_text))
            for m in matches:
                token = m.group(1)
                if token in opts:
                    continue
                pos = m.start()
                obj_start = full_text.rfind("{", 0, pos)
                if obj_start == -1:
                    continue
                depth = 0
                obj_end = -1
                for i in range(obj_start, min(len(full_text), obj_start + 4000)):
                    if full_text[i] == "{":
                        depth += 1
                    elif full_text[i] == "}":
                        depth -= 1
                        if depth == 0:
                            obj_end = i + 1
                            break
                if obj_end != -1:
                    try:
                        parsed_opt = json.loads(full_text[obj_start:obj_end])
                        if isinstance(parsed_opt, dict):
                            opts[token] = parsed_opt
                    except Exception:
                        pass

            # Also scan for options dictionary directly
            for opt_m in re.finditer(r"\"options\":\s*\{", full_text):
                opt_start = opt_m.end() - 1
                depth = 0
                opt_end = -1
                for i in range(opt_start, min(len(full_text), opt_start + 30000)):
                    if full_text[i] == "{":
                        depth += 1
                    elif full_text[i] == "}":
                        depth -= 1
                        if depth == 0:
                            opt_end = i + 1
                            break
                if opt_end != -1:
                    try:
                        parsed_options = json.loads(full_text[opt_start:opt_end])
                        if isinstance(parsed_options, dict):
                            for opt_k, opt_v in parsed_options.items():
                                if isinstance(opt_v, dict) and opt_k not in opts:
                                    opts[opt_k] = opt_v
                    except Exception:
                        pass

            for t, o in opts.items():
                name_val = clean_text(o.get("name") or o.get("display_name") or o.get("option_name") or "")
                sku_val = str(o.get("brand_code") or o.get("sku") or "").strip()
                gtin_val = clean_barcode(o.get("gtin") or o.get("barcode") or o.get("upc") or "")
                w_cents = o.get("wholesale_price_cents")
                r_cents = o.get("retail_price_cents")
                w_price = f"{w_cents/100:.2f}" if w_cents is not None else clean_price(o.get("wholesale_price"))
                r_price = f"{r_cents/100:.2f}" if r_cents is not None else clean_price(o.get("retail_price"))

                img_url = ""
                imgs = o.get("images") or []
                if isinstance(imgs, list) and imgs:
                    first = imgs[0]
                    if isinstance(first, dict):
                        img_url = first.get("url") or first.get("src") or ""
                    elif isinstance(first, str):
                        img_url = first

                variants_list.append({
                    "id": t,
                    "name": name_val,
                    "sku": sku_val,
                    "gtin": gtin_val,
                    "wholesale_price": w_price,
                    "retail_price": r_price,
                    "image_url": img_url,
                    "moq": str(o.get("unit_multiplier") or o.get("min_order_quantity") or "1"),
                })

            # For single product listings, extract top-level gtin and brand_code / sku
            if not single_gtin:
                gtin_matches = re.findall(r"\"(?:gtin|barcode|upc)\":\s*\"?([0-9a-zA-Z_-]+)\"?", full_text)
                for g in gtin_matches:
                    clean_g = clean_barcode(g)
                    if clean_g and clean_g.isdigit() and len(clean_g) in (8, 12, 13, 14):
                        single_gtin = clean_g
                        break
            if not single_sku:
                sku_matches = re.findall(r"\"(?:brand_code|sku)\":\s*\"?([^\",}]+)\"?", full_text)
                for s in sku_matches:
                    s_clean = str(s).strip()
                    if s_clean and s_clean.lower() not in {"null", "undefined", "false", "true", ""}:
                        single_sku = s_clean
                        break

        # 3. Fallback to legacy __NEXT_DATA__ if flight data was empty
        if not variants_list and not single_gtin:
            next_m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
            if next_m:
                try:
                    root = json.loads(next_m.group(1))
                    def find_product(obj: Any) -> dict[str, Any] | None:
                        if isinstance(obj, dict):
                            if any(k in obj for k in ["variants", "productVariants", "options"]) and ("id" in obj or "token" in obj or "productToken" in obj):
                                return obj
                            for v in obj.values():
                                res = find_product(v)
                                if res:
                                    return res
                        elif isinstance(obj, list):
                            for item in obj:
                                res = find_product(item)
                                if res:
                                    return res
                        return None

                    prod = find_product(root)
                    if prod:
                        if not actual_title:
                            actual_title = clean_text(prod.get("name") or prod.get("productName") or prod.get("title"))
                        description = clean_text(prod.get("description") or prod.get("descriptionHtml") or "")[:2000]
                        raw_vars = prod.get("variants") or prod.get("productVariants") or []
                        for v in raw_vars:
                            if not isinstance(v, dict):
                                continue
                            v_name = clean_text(v.get("name") or v.get("optionName"))
                            w_c = v.get("wholesalePriceCents")
                            r_c = v.get("retailPriceCents")
                            variants_list.append({
                                "id": str(v.get("id") or v.get("token") or ""),
                                "name": v_name,
                                "sku": str(v.get("sku") or v.get("brand_code") or "").strip(),
                                "gtin": clean_barcode(v.get("gtin") or v.get("barcode") or v.get("upc") or ""),
                                "wholesale_price": f"{w_c/100:.2f}" if w_c is not None else clean_price(v.get("wholesalePrice")),
                                "retail_price": f"{r_c/100:.2f}" if r_c is not None else clean_price(v.get("retailPrice")),
                                "image_url": str(v.get("image_url") or ""),
                            })
                except Exception:
                    pass

        # 4. Fallback: extract Schema.org JSON-LD (<script type="application/ld+json">)
        if not single_gtin and not variants_list:
            ld_matches = re.findall(r'<script[^>]*type=[\'"]application/ld\+json[\'"][^>]*>(.*?)</script>', html, re.DOTALL)
            for ld in ld_matches:
                try:
                    ld_data = json.loads(ld)
                    if isinstance(ld_data, dict):
                        if not actual_title and ld_data.get("name"):
                            actual_title = clean_text(ld_data.get("name"))
                        single_gtin = clean_barcode(ld_data.get("gtin12") or ld_data.get("gtin13") or ld_data.get("gtin") or "")
                        single_sku = str(ld_data.get("sku") or "").strip()
                except Exception:
                    pass

        return {
            "actual_title": actual_title,
            "description": description,
            "variants": variants_list,
            "single_sku": single_sku,
            "single_gtin": single_gtin,
            "single_img": single_img,
        }

    def _save_enriched_details(
        self,
        row: dict[str, Any],
        details: dict[str, Any],
        product_url: str,
        run_id: str,
    ) -> None:
        """Stores enriched details and splits dropdown variants into individual rows."""
        product_id = row["product_id"]
        current_title = row.get("product_title", "")
        final_title = clean_text(details.get("actual_title"))
        if not final_title or final_title.startswith("Product p_"):
            final_title = current_title if not current_title.startswith("Product p_") else f"Product {product_id}"

        variants = details.get("variants") or []
        description = str(details.get("description") or "").strip()
        single_sku = str(details.get("single_sku") or "").strip()
        single_gtin = clean_barcode(details.get("single_gtin"))
        single_img = str(details.get("single_img") or row.get("image_url") or "").strip()

        if variants and len(variants) > 1:
            # Delete generic parent placeholder row so only specific variants appear
            self.database.delete_parent_placeholder(product_id)

            for v in variants:
                v_name = clean_text(v.get("name") or "Default")
                v_sku = str(v.get("sku") or single_sku).strip()
                raw_gtin = clean_barcode(v.get("gtin") or single_gtin)
                upc, ean = classify_barcode(raw_gtin)
                v_img = str(v.get("image_url") or single_img).strip()
                v_wholesale = clean_price(v.get("wholesale_price")) or row.get("wholesale_price", "")
                v_retail = clean_price(v.get("retail_price")) or row.get("retail_price", "")

                v_key = f"faire:{product_id}:{v.get('id') or v_name or v_sku}"
                self._captured_keys.add(v_key)

                self.database.upsert_product({
                    "key": v_key,
                    "product_id": product_id,
                    "variant_id": str(v.get("id") or ""),
                    "variant_name": v_name,
                    "product_url": product_url,
                    "brand_name": self.status.current_brand or row.get("brand_name", ""),
                    "product_title": final_title,
                    "wholesale_price": v_wholesale,
                    "retail_price": v_retail,
                    "sku": v_sku,
                    "upc": upc,
                    "ean": ean,
                    "barcode": raw_gtin,
                    "moq": row.get("moq", "1"),
                    "image_url": v_img,
                    "description": description,
                    "enriched": 1,
                }, run_id=run_id)

        else:
            # Single variant product
            sku = (variants[0].get("sku") if variants else "") or single_sku
            raw_gtin = clean_barcode((variants[0].get("gtin") if variants else "") or single_gtin)
            upc, ean = classify_barcode(raw_gtin)
            v_img = (variants[0].get("image_url") if variants else "") or single_img
            v_wholesale = (clean_price(variants[0].get("wholesale_price")) if variants else "") or row.get("wholesale_price", "")
            v_retail = (clean_price(variants[0].get("retail_price")) if variants else "") or row.get("retail_price", "")

            self.database.upsert_product({
                "key": row["key"],
                "product_id": product_id,
                "variant_id": "",
                "variant_name": "",
                "product_url": product_url,
                "brand_name": self.status.current_brand or row.get("brand_name", ""),
                "product_title": final_title,
                "wholesale_price": v_wholesale,
                "retail_price": v_retail,
                "sku": sku,
                "upc": upc,
                "ean": ean,
                "barcode": raw_gtin,
                "moq": row.get("moq", "1"),
                "image_url": v_img,
                "description": description,
                "enriched": 1,
            }, run_id=run_id)

    async def _enrich_product_details(self, page: Page, run_id: str, options: FaireScrapeOptions) -> None:
        """Self-healing enrichment: retries indefinitely on 429 with 4–8 min cooldowns. Never stops itself.
        Processes product titles, dropdown variants (colors/sizes), SKUs, UPCs, and EANs."""
        context = page.context

        batch_limit = int(getattr(options, "batch_limit", 0) or 0)
        limit_sql = f" LIMIT {batch_limit}" if batch_limit > 0 else ""

        with self.database.connection() as conn:
            rows = conn.execute(
                f"""
                SELECT key, product_id, product_title, product_url, image_url,
                       wholesale_price, retail_price, moq, brand_name
                FROM faire_products
                WHERE product_id != ''
                  AND (enriched = 0
                       OR product_title LIKE 'Product p_%'
                       OR product_title = 'Find similar'
                       OR product_title = ''
                       OR brand_name = 'N/A'
                       OR brand_name = '')
                GROUP BY product_id
                ORDER BY rowid ASC
                {limit_sql}
                """
            ).fetchall()

        if not rows:
            self.status.log("All products already have titles, variants, and barcodes enriched.", "info")
            return

        total_to_enrich = len(rows)
        self.status.log(f"Starting self-healing enrichment for {total_to_enrich} products...")

        completed_count = 0
        # Shared adaptive pacing (AIMD): starts at 2.0–3.5s, slows on 429, recovers after clean streaks
        base_min_delay = 2.0
        base_max_delay = 3.5
        min_delay = base_min_delay
        max_delay = base_max_delay
        clean_streak = 0
        base_cooldown = random.uniform(240, 480)  # 4–8 min initial cooldown

        for idx, raw_row in enumerate(rows, start=1):
            if self._stop_requested:
                break

            row = dict(raw_row)
            product_id = row["product_id"]
            product_url = f"https://www.faire.com/product/{product_id}"
            current_title = row.get("product_title", "")

            # Polite inter-request pacing BETWEEN every product (not just at startup)
            if idx > 1:
                await asyncio.sleep(random.uniform(min_delay, max_delay))

            if self._stop_requested:
                break

            details = None
            consecutive_errors = 0

            for attempt in range(9999):  # effectively infinite retries
                if self._stop_requested:
                    break

                # Method 1: Direct HTTP fetch (reuses browser session cookies)
                try:
                    resp = await context.request.get(
                        product_url,
                        timeout=20000,
                        headers={
                            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                            "accept-language": "en-US,en;q=0.9",
                            "cache-control": "no-cache",
                        }
                    )

                    if resp.status == 429:
                        try:
                            retry_after_hdr = int(resp.headers.get("retry-after", 0) or 0)
                        except (ValueError, TypeError):
                            retry_after_hdr = 0
                        jitter = random.uniform(30, 90)
                        wait_secs = max(retry_after_hdr, base_cooldown) + jitter
                        mins_txt = f"{wait_secs / 60:.1f}"
                        self.status.log(
                            f"⏸ Rate limited (HTTP 429) on product {product_id}. "
                            f"Auto-cooling for ~{mins_txt} min and slowing request pace... "
                            f"(attempt {attempt + 1})",
                            "warn"
                        )

                        # Slow down future request pace and reset clean streak
                        clean_streak = 0
                        min_delay = min(min_delay + 1.5, 8.0)
                        max_delay = min(max_delay + 2.5, 12.0)
                        base_cooldown = min(base_cooldown * 1.35, 900)  # cap at 15 min

                        end_time = asyncio.get_running_loop().time() + wait_secs
                        while not self._stop_requested:
                            wait_remaining = end_time - asyncio.get_running_loop().time()
                            if wait_remaining <= 0:
                                break
                            secs_left = int(wait_remaining)
                            mins = secs_left // 60
                            secs = secs_left % 60
                            self.status.cooldown_seconds = secs_left
                            self.status.current_item = f"⏸ Rate limit cooldown: {mins}m {secs:02d}s remaining..."
                            await asyncio.sleep(min(1.0, wait_remaining))
                        self.status.cooldown_seconds = 0
                        consecutive_errors += 1
                        continue

                    if resp.status in (503, 502, 504):
                        wait_secs = 2 ** min(consecutive_errors, 6) + random.uniform(5, 20)
                        self.status.log(
                            f"⚠️ Server error {resp.status} for {product_id}. Retrying in {wait_secs:.0f}s...",
                            "warn"
                        )
                        consecutive_errors += 1
                        await asyncio.sleep(wait_secs)
                        continue

                    if resp.ok:
                        html = await resp.text()
                        details = self._parse_product_from_html(html)
                        consecutive_errors = 0
                        clean_streak += 1
                        # Gently recover pace after every 40 clean requests without a 429
                        if clean_streak >= 40 and min_delay > base_min_delay:
                            min_delay = max(base_min_delay, min_delay - 0.5)
                            max_delay = max(base_max_delay, max_delay - 0.75)
                            clean_streak = 0
                        break

                    LOGGER.debug(f"HTTP {resp.status} for product {product_id}, skipping.")
                    break

                except Exception as e:
                    consecutive_errors += 1
                    wait_secs = min(2 ** consecutive_errors, 60) + random.uniform(5, 15)
                    LOGGER.warning(f"Request error for {product_id} (attempt {attempt + 1}): {e} — retrying in {wait_secs:.0f}s")
                    await asyncio.sleep(wait_secs)
                    if consecutive_errors > 10:
                        LOGGER.warning(f"Too many consecutive errors for {product_id}, skipping.")
                        break
                    continue

            if self._stop_requested:
                break

            # Method 2: Browser page fallback if direct HTTP gave no data
            if not details or (not details.get("actual_title") and not details.get("variants")):
                try:
                    await page.goto(product_url, wait_until="domcontentloaded", timeout=20000)
                    await asyncio.sleep(0.8)
                    html = await page.content()
                    details = self._parse_product_from_html(html)
                except Exception as e:
                    LOGGER.warning(f"Fallback browser enrichment error for {product_id}: {e}")

            if details:
                self._save_enriched_details(row, details, product_url, run_id)

            completed_count += 1
            item_name = (details.get("actual_title") if details else "") or current_title
            self.status.current_item = f"Enriched [{completed_count}/{total_to_enrich}]: {item_name[:30]}"
            if completed_count % 5 == 0 or completed_count == total_to_enrich:
                self.status.log(f"Enriched {completed_count}/{total_to_enrich} products (Latest: {item_name[:35]}...)")

        self.status.cooldown_seconds = 0
        self.status.log(f"✅ Enrichment completed for {completed_count}/{total_to_enrich} products.")

    async def enrich_single_product_by_id(
        self,
        product_id: str,
        headless: bool = True,
        storage_path: Path = Path("faire_storage_state.json"),
    ) -> list[dict[str, Any]]:
        """Enriches an individual product row on demand with variants, SKUs, and UPC/EAN barcodes."""
        product_id = product_id.strip()
        if not product_id:
            return []

        # Find existing product in database
        with self.database.connection() as conn:
            existing = conn.execute(
                "SELECT * FROM faire_products WHERE product_id = ? LIMIT 1",
                (product_id,)
            ).fetchone()

        if existing:
            row = dict(existing)
        else:
            row = {
                "key": f"faire:{product_id}",
                "product_id": product_id,
                "product_url": f"https://www.faire.com/product/{product_id}",
                "product_title": f"Product {product_id}",
                "brand_name": self.status.current_brand or "",
                "wholesale_price": "",
                "retail_price": "",
                "moq": "1",
                "image_url": "",
            }

        product_url = f"https://www.faire.com/product/{product_id}"

        async with async_playwright() as pw:
            launch_args = [
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ]
            browser = await pw.chromium.launch(headless=headless, args=launch_args)

            context_kwargs: dict[str, Any] = {
                "viewport": {"width": 1360, "height": 880},
                "user_agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/131.0.0.0 Safari/537.36"
                ),
                "locale": "en-US",
                "timezone_id": "America/New_York",
            }
            if storage_path.exists():
                try:
                    content = storage_path.read_text(encoding="utf-8")
                    if content and ("cookies" in content or "origins" in content):
                        context_kwargs["storage_state"] = str(storage_path)
                except Exception:
                    pass

            context = await browser.new_context(**context_kwargs)
            try:
                resp = await context.request.get(
                    product_url,
                    timeout=15000,
                    headers={
                        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                        "accept-language": "en-US,en;q=0.9",
                    }
                )
                if resp.status == 429:
                    raise RuntimeError("Faire rate limit encountered ('Too many requests' - HTTP 429). Please wait 1 minute before retrying.")

                details = None
                if resp.ok:
                    html = await resp.text()
                    details = self._parse_product_from_html(html)

                if not details or (not details.get("actual_title") and not details.get("variants")):
                    page = await context.new_page()
                    await page.goto(product_url, wait_until="domcontentloaded", timeout=20000)
                    html = await page.content()
                    details = self._parse_product_from_html(html)
                    await page.close()

                if details:
                    self._save_enriched_details(row, details, product_url, run_id="single_enrich")
            finally:
                self.status.cooldown_seconds = 0
                await context.close()
                await browser.close()

        # Query updated products for this product_id
        res = self.database.get_products(search=product_id, page_size=100)
        return res["items"]

    async def _advance_category_page(self, page: Page, current_page: int) -> tuple[bool, int]:
        """Advances numbered pagination (e.g. [<-] [1] 2 3 4 5 ... 62 [->]) on Faire category pages."""
        try:
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
            await asyncio.sleep(1.0)

            before_url = page.url
            before_sig = await page.evaluate(
                """() => {
                    const links = Array.from(document.querySelectorAll("a[href*='/brand/'], a[href*='/product/']"));
                    return links.slice(0, 8).map(a => a.getAttribute("href") || "").join("|");
                }"""
            )

            click_info = await page.evaluate(
                """(currentPage) => {
                    const nextNumStr = String(currentPage + 1);

                    const candidates = Array.from(document.querySelectorAll(
                        "nav, [role='navigation'], [data-test*='pagination' i], [data-test-id*='pagination' i], [class*='pagination' i], [class*='Pagination']"
                    ));

                    const allContainers = [...candidates];
                    const numericEls = Array.from(document.querySelectorAll("a, button")).filter(el => {
                        const t = (el.innerText || "").trim();
                        return t === String(currentPage) || t === nextNumStr;
                    });
                    for (const el of numericEls) {
                        const parent = el.closest("nav, ul, div");
                        if (parent && !allContainers.includes(parent)) {
                            allContainers.push(parent);
                        }
                    }

                    let totalPages = 0;
                    let targetEl = null;

                    const isDisabled = (el) => {
                        if (!el) return true;
                        if (el.disabled) return true;
                        const ariaDis = (el.getAttribute("aria-disabled") || "").toLowerCase();
                        if (ariaDis === "true") return true;
                        const cls = (el.className || "").toString().toLowerCase();
                        if (cls.includes("disabled") || cls.includes("cursor-not-allowed")) return true;
                        const style = window.getComputedStyle(el);
                        if (style.pointerEvents === "none") return true;
                        return false;
                    };

                    for (const container of allContainers) {
                        const items = Array.from(container.querySelectorAll("a, button")).filter(el => el.offsetParent !== null);
                        if (items.length < 2) continue;

                        const nums = items
                            .map(el => parseInt((el.innerText || "").trim(), 10))
                            .filter(n => !isNaN(n) && n > 0 && n < 10000);

                        if (nums.length === 0) continue;
                        const maxNum = Math.max(...nums);
                        if (maxNum > totalPages) totalPages = maxNum;

                        if (currentPage >= maxNum) continue;

                        // 1. Exact next page number (e.g. "2", "3", ...)
                        const exactNext = items.find(el => (el.innerText || "").trim() === nextNumStr && !isDisabled(el));
                        if (exactNext) {
                            targetEl = exactNext;
                            break;
                        }

                        // 2. Explicit Next arrow button/link via aria-label / title / data-test
                        const ariaNext = items.find(el => {
                            const label = (
                                (el.getAttribute("aria-label") || "") + " " +
                                (el.getAttribute("title") || "") + " " +
                                (el.getAttribute("data-test") || "") + " " +
                                (el.getAttribute("data-test-id") || "")
                            ).toLowerCase();
                            return (label.includes("next") || label.includes("forward")) && !isDisabled(el);
                        });
                        if (ariaNext) {
                            targetEl = ariaNext;
                            break;
                        }

                        // 3. Last icon-only SVG button/link in pagination bar (-> arrow)
                        const lastItem = items[items.length - 1];
                        if (lastItem && lastItem.querySelector("svg") && !(lastItem.innerText || "").trim() && !isDisabled(lastItem)) {
                            targetEl = lastItem;
                            break;
                        }
                    }

                    if (!targetEl) {
                        return { clicked: false, totalPages };
                    }

                    targetEl.scrollIntoView({ behavior: "instant", block: "center" });
                    targetEl.click();
                    return { clicked: true, totalPages };
                }""",
                current_page,
            )

            total_pages = int(click_info.get("totalPages") or 0)
            if not click_info.get("clicked"):
                return (False, total_pages)

            # Wait up to 12s for URL or grid content to update (handles both client-side and full navigation)
            for _ in range(24):
                await asyncio.sleep(0.5)
                cur_url = page.url
                cur_sig = ""
                try:
                    cur_sig = await page.evaluate(
                        """() => {
                            const links = Array.from(document.querySelectorAll("a[href*='/brand/'], a[href*='/product/']"));
                            return links.slice(0, 8).map(a => a.getAttribute("href") || "").join("|");
                        }"""
                    )
                except Exception:
                    continue
                if cur_url != before_url or (cur_sig and cur_sig != before_sig):
                    await asyncio.sleep(1.5)
                    return (True, total_pages)

            return (False, total_pages)
        except Exception as e:
            LOGGER.warning(f"Category pagination advance note on page {current_page}: {e}")
            return (False, 0)

    async def _scrape_category(
        self,
        page: Page,
        category_url: str,
        run_id: str,
        options: FaireScrapeOptions,
    ) -> None:
        """Scrapes category pages across numbered pagination by discovering brand links and queueing them."""
        self.status.log(f"Exploring category: {category_url}")
        try:
            await page.goto(category_url, wait_until="domcontentloaded", timeout=45000)
            await asyncio.sleep(2.5)
        except Exception as e:
            self.status.log(f"Failed to load category {category_url}: {e}", "warn")
            return

        seen_brands: set[str] = set()
        current_page = 1
        max_pages = 200

        while not self._stop_requested and current_page <= max_pages:
            count_before = self.status.scraped_count
            for pct in [0.0, 0.35, 0.7, 1.0]:
                if self._stop_requested:
                    break
                await page.evaluate(f"window.scrollTo(0, document.body.scrollHeight * {pct});")
                await asyncio.sleep(0.6)
                await self._scrape_dom_product_cards(page, run_id)

            links = await page.query_selector_all("a[href*='/brand/']")
            page_new_brands: list[str] = []
            for link in links:
                href = await link.get_attribute("href") or ""
                if "/brand/" in href:
                    clean_brand_url = urljoin("https://www.faire.com", href.split("?")[0])
                    if clean_brand_url not in seen_brands:
                        seen_brands.add(clean_brand_url)
                        page_new_brands.append(clean_brand_url)

            # If this category page lists brand cards without direct product cards, scrape each brand immediately in real time
            if self.status.scraped_count == count_before and page_new_brands and not self._stop_requested:
                self.status.log(
                    f"Category page {current_page}: Found {len(page_new_brands)} brand(s). Scraping products in real time..."
                )
                brand_page = await page.context.new_page()
                try:
                    for b_url in page_new_brands:
                        if self._stop_requested:
                            break
                        await self._scrape_brand(brand_page, b_url, run_id, options)
                finally:
                    try:
                        await brand_page.close()
                    except Exception:
                        pass

            advanced, total_pages = await self._advance_category_page(page, current_page)
            page_label = f"{current_page}/{total_pages}" if total_pages else str(current_page)
            self.status.log(
                f"Category page {page_label}: +{len(page_new_brands)} new brand(s) "
                f"(Total discovered: {len(seen_brands)}). Products saved so far: {self.status.scraped_count}."
            )

            if not advanced:
                break
            current_page += 1


async def open_interactive_login(storage_path: Path, proxy: str = "") -> bool:
    """Launches a visible Google Chrome window to https://www.faire.com/?signin=1 so the user can log in and complete 2FA once."""
    async with async_playwright() as pw:
        launch_kwargs: dict[str, Any] = {
            "headless": False,
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        }
        if proxy:
            launch_kwargs["proxy"] = {"server": proxy}

        # Prefer real Google Chrome
        try:
            browser = await pw.chromium.launch(channel="chrome", **launch_kwargs)
        except Exception:
            browser = await pw.chromium.launch(**launch_kwargs)

        context_kwargs: dict[str, Any] = {
            "viewport": {"width": 1360, "height": 880},
            "user_agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
            "locale": "en-US",
            "timezone_id": "America/New_York",
        }
        if storage_path.exists():
            try:
                context_kwargs["storage_state"] = str(storage_path)
            except Exception:
                pass

        context = await browser.new_context(**context_kwargs)
        page = await context.new_page()

        LOGGER.info("Opening Faire at https://www.faire.com/?signin=1 ...")
        try:
            await page.goto("https://www.faire.com/?signin=1", wait_until="domcontentloaded", timeout=60000)
            await asyncio.sleep(2.0)
        except Exception as e:
            LOGGER.warning(f"Initial navigation note: {e}")

        # Inject helper overlay button so the user can click to save when ready
        inject_script = """
        (() => {
            if (document.getElementById('faire-save-helper-badge')) return;
            const badge = document.createElement('div');
            badge.id = 'faire-save-helper-badge';
            badge.style.cssText = 'position:fixed;bottom:24px;right:24px;z-index:999999999;background:#0f172a;color:#fff;padding:16px 20px;border-radius:16px;border:2px solid #10b981;box-shadow:0 12px 36px rgba(0,0,0,0.65);font-family:system-ui,-apple-system,sans-serif;max-width:320px;';
            badge.innerHTML = `
                <div style="display:flex;align-items:center;gap:8px;font-weight:700;font-size:14px;color:#34d399;margin-bottom:6px;">
                    <span>⚡ Faire Login Helper</span>
                </div>
                <div style="font-size:12px;color:#cbd5e1;margin-bottom:12px;line-height:1.4;">
                    Sign in to your Faire account in the modal. When you are finished, click below to save your session.
                </div>
                <button id="faire-save-session-now-btn" style="width:100%;background:#10b981;color:#022c22;font-weight:700;font-size:13px;padding:10px 14px;border:none;border-radius:10px;cursor:pointer;transition:all 0.2s;">
                    💾 Save Session & Close
                </button>
            `;
            document.body.appendChild(badge);
            const btn = document.getElementById('faire-save-session-now-btn');
            if (btn) {
                btn.onclick = () => {
                    btn.textContent = 'Saving session...';
                    window.__faire_session_saved = true;
                };
            }
        })();
        """

        try:
            await page.evaluate(inject_script)
        except Exception:
            pass

        # Also attempt to click the "Sign in" header button if the modal didn't open automatically
        try:
            signin_btn = await page.query_selector("button:has-text('Sign in'), a:has-text('Sign in')")
            if signin_btn and not await page.query_selector("input[type='email']"):
                await signin_btn.click()
        except Exception:
            pass

        # Wait up to 10 minutes (600s) for the user to complete login
        saved_successfully = False
        try:
            for _ in range(600):
                await asyncio.sleep(1.0)
                if page.is_closed():
                    break

                # Re-inject helper if page navigated
                try:
                    is_btn_present = await page.evaluate("Boolean(document.getElementById('faire-save-helper-badge'))")
                    if not is_btn_present:
                        await page.evaluate(inject_script)
                except Exception:
                    pass

                # Check if user clicked the save button
                clicked_save = False
                try:
                    clicked_save = await page.evaluate("Boolean(window.__faire_session_saved)")
                except Exception:
                    pass

                # Check if logged in via cookies (e.g. indigofair_token or auth cookies)
                cookies = await context.cookies()
                has_auth_cookie = any(
                    c["name"] in {"indigofair_token", "auth_token", "user_session", "session_token"}
                    for c in cookies
                )

                if clicked_save or has_auth_cookie:
                    LOGGER.info("Faire login detected! Saving storage state...")
                    await context.storage_state(path=str(storage_path))
                    saved_successfully = True
                    await asyncio.sleep(1.5)
                    await browser.close()
                    return True

        except Exception as e:
            LOGGER.warning(f"Session wait loop note: {e}")
        finally:
            try:
                await context.storage_state(path=str(storage_path))
                if not page.is_closed():
                    await browser.close()
            except Exception:
                pass

        return storage_path.exists()
