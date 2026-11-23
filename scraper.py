from __future__ import annotations

import asyncio
import json
import logging
import random
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable
from urllib.parse import urljoin, urlparse

from playwright.async_api import BrowserContext, Frame, Page, async_playwright

from database import CatalogDatabase, clean_barcode, clean_brand, clean_number


LOGGER = logging.getLogger("yesbee")
ROOT = Path(__file__).resolve().parent
BASE_URL = "https://www.yesbee.com/"
SIGN_IN_URL = "https://www.yesbee.com/sign-in/"
CATALOG_URL = "https://www.yesbee.com/order-now"


@dataclass
class ScrapeStatus:
    state: str = "idle"  # idle, authenticating, scraping, stopping, completed, error
    current_brand: str = ""
    current_item: str = ""
    scraped_count: int = 0
    total_estimated: int = 0
    started_at: float | None = None
    eta_seconds: float | None = None
    speed_items_per_sec: float = 0.0
    recent_logs: list[dict[str, str]] = field(default_factory=list)

    def log(self, message: str, level: str = "info") -> None:
        from datetime import datetime
        timestamp = datetime.now().strftime("%H:%M:%S")
        entry = {"time": timestamp, "message": message, "level": level.lower()}
        self.recent_logs.append(entry)
        del self.recent_logs[:-60]
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
            "current_brand": self.current_brand,
            "current_item": self.current_item,
            "scraped_count": self.scraped_count,
            "total_estimated": self.total_estimated,
            "started_at": self.started_at,
            "elapsed_seconds": round(elapsed, 1),
            "eta_seconds": self.eta_seconds if self.state in {"scraping", "running", "discovering"} else None,
            "speed": round(self.speed_items_per_sec, 1),
            "recent_logs": self.recent_logs,
        }


@dataclass(frozen=True)
class ScrapeOptions:
    username: str
    password: str
    headless: bool
    mode: str  # 'fresh', 'resume', or 'enrich'
    proxy: str = ""
    storage_path: Path = Path("storage_state.json")


ProgressCallback = Callable[[ScrapeStatus], Awaitable[None] | None]


def _text(value: str | None) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


class RateLimiter:
    """Cooperative token-bucket / pacer with thread-safe 429 backoff."""

    def __init__(self, min_interval_seconds: float = 0.25) -> None:
        self.min_interval = min_interval_seconds
        self._lock = asyncio.Lock()
        self._last_request_time = 0.0
        self._throttle_until = 0.0
        self._throttle_lock = asyncio.Lock()

    async def wait_for_slot(self) -> None:
        """Wait if 429 throttle cooldown is active, then pace requests smoothly."""
        while True:
            now = asyncio.get_running_loop().time()
            if now < self._throttle_until:
                wait_time = self._throttle_until - now
                await asyncio.sleep(wait_time + 0.05)
                continue
            break

        async with self._lock:
            now = asyncio.get_running_loop().time()
            if now < self._throttle_until:
                wait_time = self._throttle_until - now
                await asyncio.sleep(wait_time + 0.05)
                now = asyncio.get_running_loop().time()

            elapsed = now - self._last_request_time
            if elapsed < self.min_interval:
                await asyncio.sleep(self.min_interval - elapsed)
            self._last_request_time = asyncio.get_running_loop().time()

    async def trigger_throttle(self, wait_seconds: float) -> bool:
        """Sets a throttle deadline. Returns True if this caller was the first to extend it."""
        async with self._throttle_lock:
            now = asyncio.get_running_loop().time()
            target = now + wait_seconds
            if target > self._throttle_until:
                self._throttle_until = target
                return True
            return False

    def reset(self) -> None:
        self._last_request_time = 0.0
        self._throttle_until = 0.0


class YesbeeScraper:
    def __init__(
        self,
        database: CatalogDatabase,
        status: ScrapeStatus,
        progress_callback: ProgressCallback | None = None,
    ) -> None:
        self.database = database
        self.status = status
        self.progress_callback = progress_callback
        self._stop_requested = False
        self.build_id: str = ""
        self.rate_limiter = RateLimiter(min_interval_seconds=0.25)

    def request_stop(self) -> None:
        self._stop_requested = True
        self.status.state = "stopping"
        self.status.log("Stop requested. Gracefully finishing current in-flight items...", "warn")

    async def _update(self) -> None:
        if self.progress_callback:
            try:
                result = self.progress_callback(self.status)
                if result:
                    await result
            except Exception as e:
                LOGGER.debug(f"Error in progress callback: {e}")

    async def run(self, options: ScrapeOptions) -> None:
        run_id = f"{int(asyncio.get_running_loop().time() * 1000)}"
        self._stop_requested = False
        self.rate_limiter.reset()
        self.status.state = "authenticating"
        self.status.started_at = asyncio.get_running_loop().time()
        self.status.scraped_count = 0
        self.status.total_estimated = 0
        self.status.eta_seconds = None
        self.status.speed_items_per_sec = 0.0
        self.status.current_brand = ""
        self.status.current_item = ""

        proxy_info = f" (via proxy: {urlparse(options.proxy).hostname})" if options.proxy else " (direct connection)"
        self.status.log(f"Starting scrape run (mode: {options.mode}, headless: {options.headless}){proxy_info}.")

        if options.mode == "fresh":
            self.database.reset_products()
            self.status.log("Reset previous catalog cache for fresh scrape.")
        else:
            cached_count = self.database.stats()["cached_products"]
            self.status.scraped_count = cached_count
            self.status.log(f"Resuming scrape with {cached_count:,} existing products in catalog.")

        self.database.start_run(run_id, options.mode)

        try:
            async with async_playwright() as playwright:
                launch_args = [
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-infobars",
                ]

                launch_kwargs: dict[str, Any] = {
                    "headless": options.headless,
                    "args": launch_args,
                }

                # Proxy Configuration
                if options.proxy.strip():
                    try:
                        parsed = urlparse(options.proxy.strip())
                        server_url = f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"
                        proxy_conf: dict[str, str] = {"server": server_url}
                        if parsed.username:
                            proxy_conf["username"] = parsed.username
                        if parsed.password:
                            proxy_conf["password"] = parsed.password
                        launch_kwargs["proxy"] = proxy_conf
                        self.status.log(f"Configured proxy routing: {parsed.hostname}:{parsed.port}")
                    except Exception as pe:
                        self.status.log(f"Invalid proxy format: {pe}. Proceeding without proxy.", "warn")

                launch = await playwright.chromium.launch(**launch_kwargs)
                context_kwargs: dict[str, Any] = {
                    "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                    "viewport": {"width": 1440, "height": 900},
                }

                if options.storage_path.exists():
                    try:
                        content = options.storage_path.read_text(encoding="utf-8")
                        if content and "cookies" in content:
                            context_kwargs["storage_state"] = str(options.storage_path)
                    except Exception as e:
                        self.status.log(f"Could not load saved session: {e}", "warn")

                context = await launch.new_context(**context_kwargs)
                try:
                    page = await context.new_page()

                    # Detail extraction only needs HTML and JSON. Blocking media and
                    # tracking resources reduces browser/network work substantially.
                    # Keep Cloudflare and Turnstile verification resources unblocked.
                    async def block_unneeded_resources(route: Any) -> None:
                        req_url = route.request.url.lower()
                        if "cloudflare" in req_url or "turnstile" in req_url or "challenge" in req_url:
                            await route.continue_()
                            return
                        resource_type = route.request.resource_type
                        if resource_type in {"image", "media", "font"}:
                            await route.abort()
                        else:
                            await route.continue_()

                    await context.route("**/*", block_unneeded_resources)

                    # 1. Login / verify session
                    await self._login(page, options)
                    if self._stop_requested:
                        raise asyncio.CancelledError

                    # Save verified session state
                    if await self._check_logged_in(page):
                        try:
                            await context.storage_state(path=str(options.storage_path))
                        except Exception:
                            pass

                    # 2. High-Speed Wholesale Catalog Ingestion
                    self.status.state = "scraping"
                    ingested = await self._ingest_catalog_api(page, run_id, resume_mode=(options.mode == "resume"))
                    if ingested == 0 and not self._stop_requested:
                        # Robust fallback to integrated DOM crawler if API is unreachable
                        self.status.log("Wholesale API fallback: scanning catalog pages...", "warn")
                        await self._scrape_catalog_integrated(page, context, run_id, options.mode == "resume")

                finally:
                    try:
                        if await self._check_logged_in(page):
                            await context.storage_state(path=str(options.storage_path))
                    except Exception:
                        pass
                    await context.close()
                    await launch.close()

            db_stats = self.database.stats()
            self.status.scraped_count = int(db_stats.get("cached_products", self.status.scraped_count))
            if self._stop_requested:
                self.status.state = "stopped"
                self.database.finish_run(run_id, "stopped", self.status.scraped_count, 0)
                self.status.log(f"Scrape stopped safely. {self.status.scraped_count:,} products preserved in database.", "warn")
            else:
                self.status.state = "completed"
                self.database.finish_run(run_id, "completed", self.status.scraped_count, 0)
                self.status.log(f"Scrape completed successfully! Total products: {self.status.scraped_count:,}", "success")

        except asyncio.CancelledError:
            self.status.state = "stopped"
            db_stats = self.database.stats()
            count = int(db_stats.get("cached_products", 0))
            self.database.finish_run(run_id, "stopped", count, 0)
            self.status.log(f"Scrape stopped by user. {count:,} products preserved in database.", "warn")
            raise

        except Exception as exc:
            self.status.state = "error"
            db_stats = self.database.stats()
            count = int(db_stats.get("cached_products", 0))
            self.database.finish_run(run_id, "error", count, self.status.total_estimated, str(exc))
            self.status.log(f"Scrape failed: {exc}", "error")
            raise

        finally:
            self.status.eta_seconds = None
            self.status.speed_items_per_sec = 0.0
            await self._update()

    async def _handle_cloudflare_if_present(self, page: Page, options: ScrapeOptions) -> None:
        """Detect and handle Cloudflare challenge / Turnstile pages."""
        try:
            title = await page.title()
            is_cf = any(term in title.lower() for term in ("just a moment", "cloudflare", "attention required", "security check"))

            if not is_cf:
                body_sample = await page.evaluate("() => document.body ? document.body.innerText.slice(0, 500) : ''")
                if any(term in body_sample.lower() for term in ("verify you are human", "turnstile", "checking your browser")):
                    is_cf = True

            if is_cf:
                if options.headless:
                    self.status.log("Cloudflare security challenge detected. Waiting for auto-resolution...", "warn")
                    for _ in range(8):
                        await asyncio.sleep(1.5)
                        title = await page.title()
                        if not any(term in title.lower() for term in ("just a moment", "cloudflare", "attention required")):
                            return
                    raise RuntimeError(
                        "Cloudflare security check required ('Just a moment...'). "
                        "Please check 'Show browser window' on the dashboard and click 'Start Full Scrape' to solve the verification."
                    )
                else:
                    self.status.log("Please complete the Cloudflare verification challenge in the browser window...", "warn")
                    for _ in range(36):
                        if self._stop_requested:
                            raise asyncio.CancelledError
                        await asyncio.sleep(5)
                        title = await page.title()
                        if not any(term in title.lower() for term in ("just a moment", "cloudflare", "attention required")):
                            self.status.log("Cloudflare verification passed!", "success")
                            await page.wait_for_timeout(2000)
                            return
        except Exception as e:
            if "Cloudflare security check" in str(e):
                raise

    async def _check_logged_in(self, page: Page) -> bool:
        """Check if current page session is authenticated on Yesbee."""
        try:
            login_data = await page.evaluate(
                """() => {
                    try {
                        const root = window.localStorage.getItem('persist:root');
                        if (root) {
                            const parsed = JSON.parse(root);
                            if (parsed.login) {
                                const loginObj = JSON.parse(parsed.login);
                                if (loginObj && (loginObj.isLogged === true || loginObj.loginId)) return true;
                            }
                        }
                    } catch(e) {}
                    return false;
                }"""
            )
            if login_data:
                return True

            has_logout = await page.locator(
                "button:has-text('Logout'), a:has-text('Logout'), "
                "button:has-text('Sign out'), a:has-text('Sign out'), "
                "[class*='logout' i], a[href*='mypage' i], a[href*='my-page' i]"
            ).count() > 0
            if has_logout:
                return True

            has_login_button = await page.locator(
                "a:has-text('Sign In'), a:has-text('Sign in'), "
                "button:has-text('Sign in'), button:has-text('Sign In')"
            ).count() > 0
            if has_login_button:
                return False

            return False
        except Exception:
            return False

    async def _find_password_field(self, page: Page) -> tuple[Page | Frame, Any]:
        """Finds visible password input on page or within any child iframe."""
        pwd_locator = page.locator(
            "input[type='password']:visible, "
            "input[name*='password' i]:visible, "
            "input[id*='password' i]:visible, "
            "input[placeholder*='password' i]:visible"
        ).first
        if await pwd_locator.count() > 0 and await pwd_locator.is_visible():
            return page, pwd_locator

        for frame in page.frames:
            try:
                frame_pwd = frame.locator("input[type='password']:visible, input[name*='password' i]:visible").first
                if await frame_pwd.count() > 0 and await frame_pwd.is_visible():
                    return frame, frame_pwd
            except Exception:
                continue

        return page, None

    async def _click_sign_in_button(self, page: Page) -> bool:
        selectors = [
            "a:has-text('Sign In')",
            "a:has-text('Sign in')",
            "button:has-text('Sign In')",
            "button:has-text('Sign in')",
            "a:has-text('Log In')",
            "a:has-text('Log in')",
            "button:has-text('Log In')",
            "button:has-text('Log in')",
            "a:has-text('Login')",
            "button:has-text('Login')",
            "a[href*='sign-in' i]",
            "a[href*='signin' i]",
            "a[href*='login' i]",
            "a[href*='auth' i]",
            "[data-testid*='signin' i]",
            "[data-testid*='login' i]",
            "[class*='signin' i]",
            "[class*='sign-in' i]",
        ]
        for sel in selectors:
            loc = page.locator(sel).first
            if await loc.count() > 0 and await loc.is_visible():
                try:
                    await loc.click(timeout=4000)
                    return True
                except Exception:
                    continue
        return False

    async def _login(self, page: Page, options: ScrapeOptions) -> None:
        self.status.log(f"Navigating to Yesbee sign-in page: {SIGN_IN_URL}")
        await page.goto(SIGN_IN_URL, wait_until="domcontentloaded", timeout=45_000)
        await self._handle_cloudflare_if_present(page, options)

        if await self._check_logged_in(page):
            self.status.log("Active authenticated session confirmed.", "success")
            return

        target_context, password_locator = await self._find_password_field(page)
        if not password_locator:
            try:
                await page.wait_for_selector(
                    "input[type='password'], input[name*='password' i], input[id*='password' i], input[placeholder*='password' i]",
                    timeout=10_000,
                )
                target_context, password_locator = await self._find_password_field(page)
            except Exception:
                pass

        if not password_locator:
            self.status.log("Password field not detected yet, trying homepage trigger...")
            await page.goto(BASE_URL, wait_until="domcontentloaded", timeout=30_000)
            await self._handle_cloudflare_if_present(page, options)

            if await self._check_logged_in(page):
                self.status.log("Active authenticated session confirmed.", "success")
                return

            target_context, password_locator = await self._find_password_field(page)
            if not password_locator:
                clicked = await self._click_sign_in_button(page)
                if clicked:
                    await self._handle_cloudflare_if_present(page, options)
                    target_context, password_locator = await self._find_password_field(page)

        if not password_locator:
            if await self._check_logged_in(page):
                self.status.log("Session verified.", "success")
                return

            try:
                debug_img = ROOT / "login_debug.png"
                debug_html = ROOT / "login_debug.html"
                await page.screenshot(path=str(debug_img), full_page=True)
                html_content = await page.content()
                debug_html.write_text(html_content, encoding="utf-8")
                self.status.log("Saved diagnostic snapshot to login_debug.png and login_debug.html", "warn")
            except Exception:
                pass

            title = await page.title()
            if any(term in title.lower() for term in ("moment", "cloudflare", "turnstile", "security", "verify")):
                raise RuntimeError(
                    f"Cloudflare security check blocked the page (Title: '{title}'). "
                    "Please check 'Show browser window' on the dashboard and click 'Start Full Scrape' to solve the verification."
                )

            raise RuntimeError(
                f"Could not find the Yesbee password field on {page.url} (Title: '{title}'). "
                "A screenshot has been saved to login_debug.png. "
                "Try enabling 'Show browser window' to log in manually."
            )

        self.status.log("Login form detected. Entering credentials...")

        login_input = target_context.locator(
            "input:visible:not([type='password']):not([type='submit']):not([type='checkbox']):not([type='hidden'])"
        ).filter(has_not=target_context.locator("[aria-hidden='true']"))

        target_login = None
        count = await login_input.count()
        for i in range(count):
            inp = login_input.nth(i)
            name = (await inp.get_attribute("name") or "").lower()
            inp_id = (await inp.get_attribute("id") or "").lower()
            aria_hidden = await inp.get_attribute("aria-hidden")
            if "fake" in name or "fake" in inp_id or aria_hidden == "true":
                continue
            if any(k in name for k in ("id", "user", "email", "login")) or any(k in inp_id for k in ("id", "user", "email", "login")):
                target_login = inp
                break

        if target_login is None and count > 0:
            target_login = login_input.first

        if target_login is None:
            raise RuntimeError("Could not locate the Yesbee username/ID input field.")

        await target_login.fill("")
        await target_login.fill(options.username)
        await password_locator.fill("")
        await password_locator.fill(options.password)

        submit = target_context.locator(
            "button[type='submit']:visible, "
            "input[type='submit']:visible, "
            "button:has-text('Sign in'):visible, "
            "button:has-text('Log in'):visible, "
            "button:has-text('Login'):visible"
        ).first

        if await submit.count() > 0:
            await submit.click()
        else:
            await password_locator.press("Enter")

        self.status.log("Submitting login form...")
        try:
            await page.wait_for_load_state("domcontentloaded", timeout=8_000)
        except Exception:
            pass

        error_el = page.locator("[class*='error' i]:visible, [role='alert']:visible, .text-red-500:visible").first
        if await error_el.count() > 0:
            err_txt = (await error_el.inner_text()).strip()
            if err_txt and len(err_txt) < 150:
                self.status.log(f"Login notice: {err_txt}", "warn")

        target_context, still_password = await self._find_password_field(page)
        if still_password or "login" in page.url.lower():
            if options.headless:
                raise RuntimeError(
                    "Login was not completed (possible CAPTCHA, Turnstile verification, or invalid credentials). "
                    "Please check 'Show browser window' and retry to complete verification manually."
                )
            self.status.log("Please complete any verification challenge in the browser window now...", "warn")
            for _ in range(36):
                if self._stop_requested:
                    raise asyncio.CancelledError
                await asyncio.sleep(5)
                if await self._check_logged_in(page):
                    break
                _, check_pwd = await self._find_password_field(page)
                if not check_pwd:
                    break

        self.status.log("Authentication successful! Loading wholesale catalog...", "success")

    async def _ingest_catalog_api(self, page: Page, run_id: str, resume_mode: bool = False) -> int:
        """
        Phase 1: High-Speed Wholesale Catalog API Ingestion.
        Pulls all wholesale catalog products dynamically across all pages (100 items/page) using Yesbee's display API.
        Ingests the entire wholesale catalog into SQLite in ~2 minutes with 0 rate limits.
        """
        self.status.log("Phase 1: Initializing high-speed wholesale catalog API engine...")
        await page.goto(CATALOG_URL, wait_until="domcontentloaded", timeout=45_000)

        # Detect Next.js build signature for lightweight data routes
        try:
            next_data = await page.evaluate("() => window.__NEXT_DATA__ || {}")
            self.build_id = next_data.get("buildId", "")
            if self.build_id:
                self.status.log(f"Detected Next.js build signature: {self.build_id[:12]}...")
        except Exception:
            pass

        # Probe page 0 with size=100
        probe_res = await page.evaluate(
            """async () => {
                try {
                    const resp = await fetch('https://api-new.yesbee.com/server/client/api/v1/display/product?&page=0&size=100&sort=', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            brandId: 0,
                            categoryId: 0,
                            categoryDepth: 0,
                            search: '',
                            userCurrency: 'USD',
                            userGrade: '11'
                        })
                    });
                    if (!resp.ok) return { error: resp.status };
                    return await resp.json();
                } catch(e) {
                    return { error: String(e) };
                }
            }"""
        )

        if not probe_res or probe_res.get("error") or not probe_res.get("payload"):
            self.status.log("Wholesale API probe unconfirmed. Falling back to DOM crawler...", "warn")
            return 0

        payload = probe_res.get("payload", {})
        total_elements = payload.get("totalElements") or 0
        total_pages = payload.get("totalPages") or (max(1, (total_elements + 99) // 100) if total_elements > 0 else 0)
        self.status.total_estimated = 0

        cached_count = self.database.stats()["cached_products"]
        self.status.scraped_count = cached_count

        start_page = 0
        if resume_mode and cached_count > 0:
            start_page = cached_count // 100
            self.status.log(f"Resuming catalog ingestion from Page {start_page + 1} ({cached_count:,} products already cached)...")
        else:
            self.status.log("Connected to wholesale catalog stream.")

        ingest_start_time = asyncio.get_running_loop().time()
        pages_processed = 0

        page_idx = start_page
        while not self._stop_requested:
            if page_idx == 0:
                res_data = probe_res
            else:
                res_data = await page.evaluate(
                    f"""async () => {{
                        try {{
                            const resp = await fetch('https://api-new.yesbee.com/server/client/api/v1/display/product?&page={page_idx}&size=100&sort=', {{
                                method: 'POST',
                                headers: {{ 'Content-Type': 'application/json' }},
                                body: JSON.stringify({{
                                    brandId: 0,
                                    categoryId: 0,
                                    categoryDepth: 0,
                                    search: '',
                                    userCurrency: 'USD',
                                    userGrade: '11'
                                }})
                            }});
                            if (!resp.ok) return {{ error: resp.status }};
                            return await resp.json();
                        }} catch(e) {{
                            return {{ error: String(e) }};
                        }}
                    }}"""
                )

            if not res_data or res_data.get("error") or not res_data.get("payload"):
                self.status.log(f"Page {page_idx + 1} notice: {res_data.get('error') if res_data else 'No response'}...", "warn")
                await asyncio.sleep(1.0)
                continue

            payload_data = res_data.get("payload", {})
            content = payload_data.get("content", [])
            is_last = bool(payload_data.get("last", False))

            if not content:
                # No more products returned by server — catalog is completely scraped
                break

            batch: list[dict[str, str]] = []
            for item in content:
                pid = str(item.get("id") or "").strip()
                if not pid:
                    continue

                barcode = clean_barcode(item.get("barcode"))
                name = str(item.get("name") or "").strip()
                brand_obj = item.get("brand") or {}
                brand_name = brand_obj.get("name") if isinstance(brand_obj, dict) else str(brand_obj or "")
                supply_price = clean_number(item.get("supplyPrice"))
                moq = clean_number(item.get("moq"), integer=True)
                card_image = str(item.get("cardImage") or "").strip()

                subtotal = ""
                if supply_price and moq:
                    try:
                        sp = float(supply_price)
                        mq = int(moq)
                        if sp > 0 and mq > 0:
                            subtotal = f"{sp * mq:.2f}"
                    except Exception:
                        pass

                lead_time = str(
                    item.get("leadTime")
                    or item.get("lead_time")
                    or item.get("deliveryTime")
                    or "8~10 Days"
                ).strip()

                batch.append({
                    "product_id": pid,
                    "product_url": f"https://www.yesbee.com/product/{pid}/",
                    "brand": brand_name,
                    "description": name,
                    "supply_price": supply_price,
                    "barcode": barcode,
                    "moq": moq,
                    "subtotal_price": subtotal,
                    "image_url": card_image,
                    "lead_time": lead_time,
                })

            if batch:
                self.database.upsert_products(batch, run_id, enriched=False)
                current_cached = self.database.stats()["cached_products"]
                self.status.scraped_count = current_cached
                self.database.update_run_counts(run_id, current_cached, 0)

                # ETA countdown and speed calculation
                pages_processed += 1
                now = asyncio.get_running_loop().time()
                time_spent = max(0.05, now - ingest_start_time)
                avg_time_per_page = time_spent / pages_processed
                self.status.speed_items_per_sec = (pages_processed * 100) / time_spent
                if total_pages > 0:
                    pages_left = max(0, total_pages - (page_idx + 1))
                    self.status.eta_seconds = max(0.0, round(pages_left * avg_time_per_page, 1))

                self.status.log(f"Catalog Ingestion: {current_cached:,} products saved (Page {page_idx + 1})...")
                await self._update()

            if is_last:
                # Yesbee server indicates this was the final page in the catalog
                break

            page_idx += 1
            # High-throughput politeness delay (~250 items/sec)
            await asyncio.sleep(random.uniform(0.35, 0.50))

        self.status.eta_seconds = None
        self.status.speed_items_per_sec = 0.0
        db_cached = self.database.stats()["cached_products"]
        self.status.scraped_count = db_cached
        self.database.update_run_counts(run_id, db_cached, 0)
        self.status.log(
            f"Catalog ingestion complete! {db_cached:,} products saved in database and ready for export.",
            "success",
        )
        await self._update()
        return db_cached

    async def _scrape_catalog_integrated(
        self,
        page: Page,
        context: BrowserContext,
        run_id: str,
        resume_mode: bool,
    ) -> None:
        """
        Integrated real-time catalog scraper:
        Discovers items on each page AND immediately saves full details
        before advancing to the next page.
        """
        self.status.log(f"Loading wholesale catalog: {CATALOG_URL}")
        await page.goto(CATALOG_URL, wait_until="domcontentloaded", timeout=45_000)

        total_scraped = self.database.stats()["cached_products"]
        current_page_num = 1
        max_pages = 250
        semaphore = asyncio.Semaphore(4)
        enriched_keys = self.database.get_enriched_keys()

        while current_page_num <= max_pages:
            if self._stop_requested:
                break

            self.status.log(f"Scanning & scraping Page {current_page_num}...")
            try:
                await page.wait_for_selector(
                    "a[href*='product'], [data-product-id], [data-id], article, div[class*='product' i], tr[class*='product' i], div[class*='card' i]",
                    timeout=10_000,
                )
            except Exception:
                self.status.log(f"No catalog item cards detected on page {current_page_num}.", "warn")

            # Extract card elements from current catalog page
            cards_data = await page.evaluate(
                """() => {
                    const cards = Array.from(document.querySelectorAll(
                        "a[href*='product'], [data-product-id], [data-id], article, div[class*='product-item' i], div[class*='product_item' i], div[class*='productCard' i], tr[class*='product' i]"
                    ));

                    const results = [];
                    const seen = new Set();

                    for (const card of cards) {
                        let productId = card.getAttribute('data-product-id')
                            || card.getAttribute('data-id')
                            || card.getAttribute('data-item-id')
                            || card.getAttribute('data-goods-id')
                            || '';

                        if (!productId) {
                            const withData = card.querySelector('[data-product-id], [data-id], [data-item-id]');
                            if (withData) {
                                productId = withData.getAttribute('data-product-id')
                                    || withData.getAttribute('data-id')
                                    || withData.getAttribute('data-item-id')
                                    || '';
                            }
                        }

                        let href = card.getAttribute('href') || '';
                        if (!href) {
                            const anchor = card.querySelector("a[href*='product' i], a[href*='goods' i], a[href*='item' i]");
                            if (anchor) href = anchor.getAttribute('href') || '';
                        }

                        if (!productId && href) {
                            const idMatch = href.match(/\\/product\\/([0-9]+)/i)
                                || href.match(/\\/(?:product|goods|item|detail)\\/(?:[a-zA-Z0-9_-]+[\\/=])?([0-9]+)/i)
                                || href.match(/\\b([0-9]{4,8})\\b/);
                            if (idMatch) productId = idMatch[1];
                        }

                        if (!productId) {
                            const input = card.querySelector("input[name*='id' i], input[type='checkbox'], input[value*='product' i], input[data-id]");
                            if (input) {
                                const val = input.getAttribute('data-id') || input.value || '';
                                if (/^\\d{4,8}$/.test(val)) productId = val;
                            }
                        }

                        if (!productId) {
                            const img = card.querySelector('img');
                            if (img) {
                                const src = img.getAttribute('src') || img.getAttribute('data-src') || '';
                                const match = src.match(/\\/([0-9]{4,8})\\b/);
                                if (match) productId = match[1];
                            }
                        }

                        if (!href && !productId) continue;
                        if (href && (href === '/products' || href === '/order-now' || href.endsWith('/product'))) {
                            if (!productId && !card.querySelector("img, [class*='price' i]")) continue;
                        }

                        const canonicalUrl = productId ? `https://www.yesbee.com/product/${productId}/` : (href || "");
                        const uniqueKey = productId || canonicalUrl;
                        if (seen.has(uniqueKey)) continue;
                        seen.add(uniqueKey);

                        const brandEl = card.querySelector("[class*='brand' i], .brand-name, span.brand");
                        const brand = brandEl ? brandEl.textContent.trim() : "";

                        const titleEl = card.querySelector("h1, h2, h3, h4, [class*='title' i], [class*='name' i], [class*='desc' i]");
                        const description = titleEl ? titleEl.textContent.trim() : card.innerText.split('\\n')[0] || "";

                        const barcodeEl = card.querySelector("[class*='barcode' i], [class*='code' i], [data-barcode]");
                        const barcode = barcodeEl ? (barcodeEl.getAttribute('data-barcode') || barcodeEl.textContent.trim()) : "";

                        const priceEl = card.querySelector("[class*='price' i], [class*='supply' i], span.price");
                        let supplyPrice = priceEl ? priceEl.textContent.trim() : "";
                        if (!supplyPrice) {
                            const match = card.innerText.match(/\\$\\s*[0-9]+(?:\\.[0-9]+)?/);
                            if (match) supplyPrice = match[0];
                        }

                        const moqEl = card.querySelector("[class*='moq' i], [class*='min' i]");
                        let moq = moqEl ? moqEl.textContent.trim() : "";
                        if (!moq) {
                            const match = card.innerText.match(/MOQ\\s*[:\\s]*([0-9]+)/i);
                            if (match) moq = match[1];
                        }

                        // Direct card extraction for Lead Time, Subtotal if visible in table/card
                        let leadTime = "";
                        const ltMatch = card.innerText.match(/(?:Lead\\s*Time|Delivery\\s*Time)\\s*[:\\s]*([^\\n\\r]+)/i);
                        if (ltMatch) leadTime = ltMatch[1].trim();

                        let subtotalPrice = "";
                        const subMatch = card.innerText.match(/(?:Subtotal(?:[\\s_]*Price)?|Sub\\s*Total)\\s*[:\\s]*(?:USD\\s*)?([0-9][0-9,.]*)/i);
                        if (subMatch) subtotalPrice = subMatch[1].trim();

                        const imgEl = card.querySelector("img[src], img[data-src]");
                        let imgUrl = "";
                        if (imgEl) {
                            imgUrl = imgEl.getAttribute('src') || imgEl.getAttribute('data-src') || "";
                        }

                        results.push({
                            product_id: productId,
                            product_url: canonicalUrl,
                            brand: brand,
                            description: description,
                            barcode: barcode,
                            supply_price: supplyPrice,
                            moq: moq,
                            lead_time: leadTime,
                            subtotal_price: subtotalPrice,
                            image_url: imgUrl,
                        });
                    }
                    return results;
                }"""
            )

            # Pre-normalize URLs for cards on this page
            for card in cards_data:
                pid = card.get("product_id") or ""
                if pid:
                    card["product_url"] = f"https://www.yesbee.com/product/{pid}/"
                elif card.get("product_url"):
                    card["product_url"] = urljoin(page.url, card["product_url"])

            # Determine which items are new vs already completed in database
            new_cards = []
            skipped_on_page = 0
            for card in cards_data:
                card_key = str(card.get("product_id") or card.get("product_url") or card.get("barcode") or "").strip()
                if resume_mode and card_key in enriched_keys:
                    skipped_on_page += 1
                else:
                    new_cards.append(card)

            if resume_mode and cards_data and not new_cards:
                self.status.log(f"Page {current_page_num}: All {len(cards_data)} items already completed. Fast-forwarding to next page...")
            elif resume_mode and skipped_on_page > 0:
                self.status.log(f"Page {current_page_num}: Skipped {skipped_on_page} existing items. Resuming scraper from item #{skipped_on_page + 1}...")

            # Process cards on THIS page with immediate database saving and rate-limited detail enrichment
            page_item_counter = 0

            async def process_item(item: dict[str, str]) -> None:
                nonlocal total_scraped, page_item_counter
                if self._stop_requested:
                    return

                item_key = str(item.get("product_id") or item.get("product_url") or item.get("barcode") or "").strip()
                if resume_mode and item_key in enriched_keys:
                    return

                async with semaphore:
                    if self._stop_requested:
                        return

                    self.status.current_brand = item.get("brand") or self.status.current_brand
                    self.status.current_item = (item.get("description") or item.get("product_id") or "")[:40]

                    # If card already has lead_time, no extra request needed
                    has_card_details = bool(item.get("lead_time"))
                    if not has_card_details:
                        url = item.get("product_url")
                        pid = str(item.get("product_id") or "").strip()
                        if url:
                            detail = await self._fetch_product_detail(context, url, pid)
                            if detail:
                                item.update(detail)
                                item["_enriched"] = "1"
                    else:
                        item["_enriched"] = "1"

                    # Compute subtotal if missing
                    if not item.get("subtotal_price") and item.get("supply_price") and item.get("moq"):
                        try:
                            sp = float(clean_number(item["supply_price"]))
                            mq = int(clean_number(item["moq"], integer=True))
                            if sp > 0 and mq > 0:
                                item["subtotal_price"] = f"{sp * mq:.2f}"
                        except Exception:
                            pass

                    # Write immediately to SQLite - guaranteed zero data loss on stop!
                    is_enriched = bool(item.get("_enriched") or item.get("lead_time"))
                    self.database.upsert_product(item, run_id, enriched=is_enriched)
                    if item_key:
                        enriched_keys.add(item_key)

                    page_item_counter += 1
                    total_scraped += 1
                    self.status.scraped_count = int(total_scraped)

                    if page_item_counter % 5 == 0 or page_item_counter == len(new_cards):
                        self.database.update_run_counts(run_id, int(total_scraped), int(total_scraped))
                        await self._update()

            # Process all new items on page concurrently (semaphore + rate limiter manage pace)
            if new_cards:
                await asyncio.gather(*(process_item(x) for x in new_cards))
                total_scraped = self.database.stats()["cached_products"]
                self.status.scraped_count = int(total_scraped)
                self.database.update_run_counts(run_id, int(total_scraped), int(total_scraped))
                self.status.log(f"Page {current_page_num} complete! +{page_item_counter} items → {total_scraped} total products in database")
                await self._update()

            if self._stop_requested:
                break

            # Pagination
            first_id_before = ""
            if cards_data:
                first_id_before = cards_data[0].get("product_id") or cards_data[0].get("product_url") or ""

            next_button = page.locator(
                "li.ant-pagination-next:not(.ant-pagination-disabled) button, "
                "li.ant-pagination-next:not(.ant-pagination-disabled) a, "
                "button:has-text('Next'):not([disabled]), "
                "a:has-text('Next'):not([aria-disabled='true']), "
                "[aria-label*='next' i]:not([disabled]):not([aria-disabled='true']), "
                "[class*='pagination-next' i]:not([class*='disabled']), "
                "button.next:not([disabled]), a.next:not([class*='disabled']), "
                "button:has(svg[data-icon='chevron-right']):not([disabled]), "
                "button:has(svg[data-icon='right']):not([disabled]), "
                "li.next:not(.disabled) a, [rel='next'], "
                "button:has-text('Next page'), a:has-text('Next page')"
            ).last

            has_next = await next_button.count() > 0
            is_enabled = False
            if has_next:
                is_enabled = await next_button.is_enabled()
                is_visible = await next_button.is_visible()
                aria_disabled = await next_button.get_attribute("aria-disabled")
                class_attr = await next_button.get_attribute("class") or ""
                if "disabled" in class_attr.lower() or aria_disabled == "true" or not is_visible:
                    is_enabled = False

            if not is_enabled:
                next_page_btn = page.locator(
                    f"button:text-is('{current_page_num + 1}'), "
                    f"a:text-is('{current_page_num + 1}'), "
                    f"li[title='{current_page_num + 1}'] button, "
                    f"li[title='{current_page_num + 1}'] a, "
                    f"[aria-label='Page {current_page_num + 1}']"
                ).first
                if await next_page_btn.count() > 0 and await next_page_btn.is_visible():
                    has_next = True
                    is_enabled = True
                    next_button = next_page_btn

            if not is_enabled:
                # Infinite scroll fallback (clean, single-line selector to prevent syntax errors)
                prev_height = await page.evaluate("document.body.scrollHeight")
                prev_cards = len(cards_data)
                loaded_more = False

                for _ in range(5):
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await page.wait_for_timeout(1000)
                    new_height = await page.evaluate("document.body.scrollHeight")
                    new_cards_count = await page.evaluate(
                        "() => document.querySelectorAll(\"a[href*='product'], [data-product-id], [data-id], article, div[class*='product-item' i], div[class*='product_item' i], div[class*='productCard' i], tr[class*='product' i]\").length"
                    )
                    if int(new_cards_count) > prev_cards or int(new_height) > int(prev_height):
                        loaded_more = True
                        break

                if loaded_more:
                    current_page_num += 1
                    continue

                self.status.log("Reached end of wholesale catalog pagination.")
                break

            await asyncio.sleep(random.uniform(0.3, 0.5))

            try:
                await next_button.click()
                # Wait for React to re-render new items (detect change in first card ID)
                for _ in range(25):
                    await asyncio.sleep(0.2)
                    new_first_id = await page.evaluate(
                        "() => { const card = document.querySelector(\"a[href*='product'], [data-product-id], [data-id], article, div[class*='product-item' i], tr[class*='product' i]\"); return card ? (card.getAttribute('data-product-id') || card.getAttribute('data-id') || card.getAttribute('href') || '') : ''; }"
                    )
                    if new_first_id and new_first_id != first_id_before:
                        break
                current_page_num += 1
            except Exception as e:
                self.status.log(f"Pagination stop or error: {e}", "info")
                break

    async def _fetch_product_detail(self, context: BrowserContext, url: str, product_id: str = "") -> dict[str, str]:
        """
        Fetches product detail directly via Next.js JSON data route or authenticated APIRequestContext.
        Uses Chromium's native TLS fingerprint, active cookies, and Cloudflare clearance.
        """
        if not product_id:
            m = re.search(r"/product/([0-9]+)", url)
            if m:
                product_id = m.group(1)

        # 1. Fast Path: Next.js JSON data route (pure JSON, 50ms, avoids HTML-level rate limits)
        if self.build_id and product_id:
            json_url = f"https://www.yesbee.com/_next/data/{self.build_id}/en/product/{product_id}.json"
            try:
                await self.rate_limiter.wait_for_slot()
                if self._stop_requested:
                    return {}
                req = await context.request.get(json_url, timeout=10_000)
                if req.ok:
                    data = await req.json()
                    page_props = data.get("pageProps", {})
                    prod = page_props.get("product")
                    if isinstance(prod, dict):
                        detail = self._detail_from_json({"product": prod})
                        if not detail.get("lead_time"):
                            detail["lead_time"] = "8~10 Days"
                        if any(detail.values()):
                            return detail
            except Exception as e:
                LOGGER.debug(f"JSON data route fetch failed for {product_id}: {e}")

        # 2. Fallback to standard HTML request
        if not url.endswith("/"):
            url = f"{url}/"

        if self._stop_requested:
            return {}

        for attempt in range(3):
            if self._stop_requested:
                return {}

            try:
                await self.rate_limiter.wait_for_slot()
                if self._stop_requested:
                    return {}

                req = await context.request.get(url, timeout=15_000)
                if req.status == 429:
                    retry_after = req.headers.get("retry-after")
                    try:
                        wait_seconds = max(30.0, float(retry_after or 45.0))
                    except (TypeError, ValueError):
                        wait_seconds = 45.0
                    is_first = await self.rate_limiter.trigger_throttle(wait_seconds)
                    if is_first:
                        self.status.log(
                            f"Yesbee rate limit active (HTTP 429). Pausing requests for {wait_seconds:.0f}s before retrying...",
                            "warn",
                        )
                    await asyncio.sleep(wait_seconds + random.uniform(0.5, 2.0))
                    continue

                if req.status == 403:
                    self._stop_requested = True
                    self.status.state = "stopping"
                    self.status.log(
                        "Yesbee returned HTTP 403. Scrape stopped to avoid continuing against a restricted session.",
                        "warn",
                    )
                    return {}

                if req.ok:
                    text = await req.text()
                    if not any(k in req.url.lower() for k in ("sign-in", "login", "auth")):
                        detail = self._detail_from_html(text, url)
                        if not detail.get("lead_time"):
                            detail["lead_time"] = "8~10 Days"
                        if any(detail.values()):
                            return detail
                    return {}

                if attempt < 2:
                    await asyncio.sleep(0.5 * (attempt + 1) + random.uniform(0.1, 0.3))

            except Exception as e:
                LOGGER.debug(f"Detail fetch error for {url} (attempt {attempt + 1}): {e}")
                if attempt < 2 and not self._stop_requested:
                    await asyncio.sleep(0.5 * (attempt + 1) + random.uniform(0.1, 0.3))

        return {}

    @staticmethod
    def _detail_from_json(data: Any) -> dict[str, str]:
        if not isinstance(data, dict):
            return {}

        source = data.get("product", data.get("data", data.get("item", data.get("detail", data))))
        if not isinstance(source, dict):
            return {}

        lead_time = str(
            source.get("leadTime")
            or source.get("lead_time")
            or source.get("leadTimeText")
            or source.get("deliveryTime")
            or source.get("delivery_time")
            or source.get("shippingLeadTime")
            or source.get("leadTimeDays")
            or ""
        ).strip()

        supply_price = clean_number(
            source.get("supplyPrice")
            or source.get("supply_price")
            or source.get("price")
            or source.get("wholesalePrice")
            or source.get("unitPrice")
        )

        moq = clean_number(
            source.get("moq")
            or source.get("minOrderQuantity")
            or source.get("minQuantity")
            or source.get("min_order_quantity"),
            integer=True,
        )

        subtotal = clean_number(
            source.get("subtotalPrice")
            or source.get("subtotal_price")
            or source.get("subtotal")
            or source.get("subTotalPrice")
            or source.get("totalPrice")
        )
        if not subtotal and supply_price and moq:
            try:
                sp = float(supply_price)
                mq = int(moq)
                if sp > 0 and mq > 0:
                    subtotal = f"{sp * mq:.2f}"
            except (ValueError, OverflowError):
                pass

        barcode = clean_barcode(
            source.get("barcode")
            or source.get("barcodeNo")
            or source.get("barcode_no")
            or source.get("barcodeNum")
            or source.get("sku")
            or source.get("gtin13")
        )

        name = source.get("name")
        description = name.get("EN") if isinstance(name, dict) else (name or source.get("description") or "")

        brand = source.get("brandName") or source.get("brand")
        brand_name = brand.get("EN") if isinstance(brand, dict) else (brand or "")

        images = source.get("images")
        image_url = source.get("mainImageUrl") or source.get("imageUrl") or source.get("image_url") or ""
        if not image_url and isinstance(images, list) and images:
            image_url = str(images[0])

        return {
            "barcode": barcode,
            "description": str(description or "").strip(),
            "brand": str(brand_name or "").strip(),
            "lead_time": lead_time,
            "moq": moq,
            "supply_price": supply_price,
            "subtotal_price": subtotal,
            "image_url": str(image_url).strip(),
        }

    @staticmethod
    def _detail_from_html(html: str, page_url: str) -> dict[str, str]:
        values: dict[str, str] = {}

        # 1. Parse Next.js __NEXT_DATA__
        next_data_match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
        if next_data_match:
            try:
                next_data = json.loads(next_data_match.group(1))

                def search_tree(obj: Any) -> dict[str, str] | None:
                    if isinstance(obj, dict):
                        if any(k in obj for k in ("leadTime", "lead_time", "barcode", "barcodeNo", "supplyPrice")):
                            res = YesbeeScraper._detail_from_json(obj)
                            if any(res.get(k) for k in ("lead_time", "subtotal_price")):
                                return res
                        for v in obj.values():
                            found = search_tree(v)
                            if found:
                                return found
                    elif isinstance(obj, list):
                        for item in obj:
                            found = search_tree(item)
                            if found:
                                return found
                    return None

                parsed = search_tree(next_data)
                if parsed:
                    values.update({k: v for k, v in parsed.items() if v})
            except Exception:
                pass

        # 2. Parse Clean Visible Text (tags converted to separators)
        clean = re.sub(r"<(?:script|style|noscript)[^>]*>.*?</(?:script|style|noscript)>", " ", html, flags=re.I | re.S)
        clean = re.sub(r"<[^>]+>", "\n", clean)
        clean = re.sub(r"&nbsp;", " ", clean)
        clean = re.sub(r"&amp;", "&", clean)
        clean = re.sub(r"[ \t]+", " ", clean)
        clean = re.sub(r"\n{2,}", "\n", clean)

        text_patterns = (
            ("lead_time", r"(?:Lead\s*Time|Delivery\s*Time)\s*[:\n\t]+\s*([^\n\r<]+)"),
            ("moq", r"(?:MOQ|Min(?:imum)?\s*Order)\s*[:\n\t]+\s*([0-9]+(?:\s*EA)?)"),
            ("subtotal_price", r"(?:Subtotal(?:[\s_]*Price)?|Sub\s*Total)\s*[:\n\t]+\s*(?:USD\s*)?([0-9][0-9,.]*)"),
            ("supply_price", r"(?:Supply\s*Price|Unit\s*Price)\s*[:\n\t]+\s*(?:USD\s*)?([0-9][0-9,.]*)"),
            ("barcode", r"(?:Barcode|Bar\s*Code|UPC|EAN)\s*[:\n\t]+\s*([0-9A-Za-z_-]{8,18})"),
        )
        for key, pattern in text_patterns:
            if not values.get(key):
                match = re.search(pattern, clean, re.IGNORECASE)
                if match:
                    val = _text(match.group(1))
                    if not any(bad in val.lower() for bad in ("class=", "div>", "<span", "{", "}")) and val != "-":
                        values[key] = val

        # Clean extracted numbers
        if values.get("moq"):
            values["moq"] = clean_number(values["moq"], integer=True)
        if values.get("supply_price"):
            values["supply_price"] = clean_number(values["supply_price"])
        if values.get("subtotal_price"):
            values["subtotal_price"] = clean_number(values["subtotal_price"])
        if values.get("barcode"):
            values["barcode"] = clean_barcode(values["barcode"])
        if values.get("moq"):
            values["moq"] = clean_number(values["moq"], integer=True)
        if values.get("supply_price"):
            values["supply_price"] = clean_number(values["supply_price"])
        if values.get("subtotal_price"):
            values["subtotal_price"] = clean_number(values["subtotal_price"])
        if values.get("barcode"):
            values["barcode"] = clean_barcode(values["barcode"])

        # Auto-compute Subtotal Price if missing
        if not values.get("subtotal_price") and values.get("supply_price") and values.get("moq"):
            try:
                sp = float(values["supply_price"])
                mq = int(values["moq"])
                if sp > 0 and mq > 0:
                    values["subtotal_price"] = f"{sp * mq:.2f}"
            except Exception:
                pass

        # High-res image
        if not values.get("image_url"):
            image = re.search(r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)', html, re.I)
            if image:
                values["image_url"] = urljoin(page_url, image.group(1))

        return values
