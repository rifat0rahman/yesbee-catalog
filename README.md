# Yesbee Wholesale Catalog Exporter

A local FastAPI & Playwright application that logs into Yesbee wholesale catalog, discovers all wholesale products, caches them in SQLite, enriches detailed specifications (Barcode, Lead Time, MOQ, Inbox/Outbox, High-Res Images), and exports Excel-compatible UTF-8 CSV spreadsheets.

---

## Key Features

- **Robust Scraping Engine**:
  - Resilient login flow that filters out hidden honeypot fields.
  - Zero-timeout browser extraction avoiding Playwright locator hang-ups on missing fields.
  - Immediate page-by-page database persistence so zero data is lost if stopped.
  - Cloudflare bypass fallback via Chromium network engine.
  - Next.js `__NEXT_DATA__` and JSON-LD schema parser for high-fidelity product specs.
- **Modern Dashboard UI**:
  - Live metric KPI cards (Cached Products, Enriched Items, Total Brands, Scraper State).
  - Searchable and filterable data table with image thumbnails and click-to-copy barcodes.
  - Collapsible credentials manager with show/hide password toggle.
  - Real-time activity console with timestamped, color-coded log levels (`INFO`, `WARN`, `ERROR`, `SUCCESS`).
  - Toast notifications and run history modal.
- **Resilient SQLite Storage**:
  - Write-Ahead Logging (WAL) and automatic schema migrations.
  - Excel-compatible UTF-8 CSV export with BOM (`\ufeff`).

---

## Windows Distribution

### Source ZIP option

The client can download the project as a ZIP, extract it, and double-click `setup_windows.bat`. Setup can install Python 3.13 automatically, create the virtual environment, install dependencies and Chromium, and create a **Yesbee Catalog** desktop icon.

After setup, double-click that desktop icon whenever the app is needed. It starts the local server at `http://127.0.0.1:8000` and opens the default browser.

Do not include `.env`, `storage_state.json`, `yesbee_catalog.db`, or `.venv` in the ZIP. Never share your credentials or login session.

### Installer option

Download and run `YesbeeCatalogSetup.exe`. The installer creates a **Yesbee Catalog** desktop icon and Start menu entry. Double-click the desktop icon to start the app and open it in the default browser at `http://127.0.0.1:8000`.

---

## Usage Workflow

1. **Enter Credentials**: Open the **Yesbee Credentials** card, enter your username and password, and click **Save Credentials**.
2. **First-time / Verification**: If your Yesbee account requires CAPTCHA, Cloudflare Turnstile, or SMS verification, check **Show browser window** and click **Start Full Scrape**. Complete the challenge in the visible browser window. Session cookies are automatically preserved in `storage_state.json`.
3. **Scrape Modes**:
   - **Start Full Scrape**: Resets the cache and discovers the entire wholesale catalog from scratch.
   - **Resume Scrape**: Preserves already scraped products and only enriches missing product details.
   - **Stop**: Gracefully stops the run while preserving all discovered items.
4. **Export**: Click **Export CSV** to download the catalog ready to open directly in Microsoft Excel, Google Sheets, or Apple Numbers.
