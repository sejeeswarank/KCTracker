# 🪙 KC Tracker — Complete Banking Ledger & Parser Suite

KC Tracker is a premium, secure, and enterprise-ready bank statement parsing and ledger management dashboard. Built for individuals and small businesses, KC Tracker converts disorganized, multi-format financial statements (from PDFs, CSVs, and Excel files) into beautifully structured, normalized ledger accounts. 

Equipped with a local secure password vault, an auto-learning merchant cleaning database, dynamic analytical tools, and a bidirectional Google Drive cloud synchronization engine, KC Tracker is the ultimate self-hosted control center for your financial data.

---

## 🗺️ High-Level System Architecture & Pipelines

The system is split into two halves: a high-speed Python core backend orchestrating mathematical validation, layout-aware PDF extraction, cryptographic operations, and cloud sync; and a gorgeous Bootstrap 5, FullCalendar, and Chart.js frontend dashboard designed for micro-interactions and smooth user workflows.

```mermaid
graph TD
    A[Raw Bank Statement: PDF / CSV / XLS] --> B(Secure Parsing Engine)
    B -->|Decrypt via Local Fernet Vault| C[Raw Table Extraction]
    C --> D[10-Phase Data Normalization Pipeline]
    D -->|Confidence Fingerprinting| E[Merchant Alias & Auto-Learning Database]
    E --> F[Double-Entry Ledger Layout Preview]
    F -->|Custom Descriptions & Verification| G[Isolated SQLite User DB]
    G -->|Official OAuth API| H[Bidirectional Google Drive Sync Folder]
    G --> I[Dynamic Calendar Dashboard & Analytics]
    G --> J[Instant PDF / Excel / TXT Exports]
    K[Admin Approval Loop] -->|Telegram Bot API Notification| H
```

### 1. The 10-Phase Parsing & Normalization Pipeline (`backend/parser.py`)
When you drop a bank statement into the upload zone, the file passes through a multi-layered sanitization pipeline:
1. **File Type Resolution:** Auto-detects the extension and MIME type.
2. **Decryption Check:** If the PDF is password-protected, the server queries the local database for the bank's encrypted credentials, decrypts them in memory using **AES-256 (Fernet)**, and opens the file.
3. **Table Extraction:** Utilizes `pdfplumber` for native text layout detection. If scanned, it falls back to character distance extraction.
4. **Segmentation Filter (`row_segmenter.py`):** Combines multi-line transactions (common in HDFC and SBI statements where the narration wraps onto two or three rows) back into unified rows.
5. **Garbage Sweeper (`garbage_filter.py`):** Trashes empty padding rows, non-transaction lines, header metadata, page count footers, and advertisement text blocks.
6. **Column Mapping Engine (`column_mapper.py`):** Translates varying headers across banks (e.g., `Value Date`, `Transaction Date`, `Post Date`, `Txn Date`) into standardized internal fields (`date`, `description`, `debit`, `credit`, `balance`).
7. **Debit/Credit Realignment (`drcr_classifier.py`):** Resolves bank-specific formatting anomalies. If debits and credits are combined in a single column marked with indicators (e.g., `DR`, `CR`, `+`, `-`), the engine splits and normalizes them.
8. **Mathematical Balance Validator (`balance_validator.py`):** Verifies ledger integrity by ensuring that for every row $N$, $Balance_N = Balance_{N-1} + Credit_N - Debit_N$. Errors are highlighted for correction.
9. **Merchant Alias Translator (`extractor.py` & `merchant_extractor.py`):** Runs complex bank descriptions (e.g., `UPI-PAYTM-UPI239849201@okaxis-WENDYS-RESTAURANT`) through regex cleaning patterns to extract the true Merchant. If the user has saved a manual display name replacement (e.g., mapping `WENDYS-RESTAURANT` to `Wendy's`), it is automatically applied.
10. **Idempotent Deduplicator:** Ensures uploaded transactions do not duplicate existing database records using database constraints on `(date, description, debit, credit, balance)`.

---

## 💻 Technical Stack

- **Backend Architecture:** Python 3.8+, Flask, SQLite3 (multi-tenant isolated database schema)
- **Data Extractor & Engines:** pandas, pdfplumber, PyPDF2
- **Cryptographic Security:** cryptography (Fernet symmetric key cryptography), bcrypt (salted user authentication hashing)
- **Cloud & Messaging Integrations:** Google Drive API v3 (OAuth 2.0 flow), Telegram Bot API (admin approval loops)
- **Frontend Masterpiece:** HTML5, CSS3 (Vanilla glassmorphism & slate-dark variables), Bootstrap 5, FullCalendar v6, Chart.js v4

---

## 📂 Project Structure

```text
KC Tracker/
│
├── app.py                      # Main Flask application entry point, routes, auth wrappers
├── config.py                   # Environment vars, path configs, keys
├── requirements.txt            # Python dependencies
├── credentials.json            # (Required) Google Drive OAuth client secrets
├── token.pickle                # (Auto-generated) Google Drive OAuth session token
│
├── backend/                    # Core Python modules
│   ├── auth.py                 # User authentication (Register/Login/Bcrypt)
│   ├── balance_validator.py    # Math validator ensuring Balance = Prev Balance + CR - DR
│   ├── column_mapper.py        # Maps raw header names to standard internal columns
│   ├── confidence_engine.py    # Assigns confidence scores to parsed rows
│   ├── database.py             # SQLite CRUD ops for user-specific databases
│   ├── drcr_classifier.py      # Fixes debit/credit alignment issues in parsed tables
│   ├── exporter.py             # Generators for PDF, Excel, and TXT ledger exports
│   ├── extractor.py            # Merchant alias application
│   ├── format_detector.py      # Detects PDF table structures and quality
│   ├── garbage_filter.py       # Drops empty rows or non-transaction PDF junk
│   ├── hdfc_parser.py          # highly specialized fast-parser for HDFC PDFs
│   ├── ledger.py               # Formatter generating front-end grouped dictionaries
│   ├── merchant_extractor.py   # Cleans complex transaction narrations to short Merchant Names
│   ├── notifier.py             # Telegram notifier and admin approval checker
│   ├── parser.py               # The Orchestrator for file parsing
│   ├── row_segmenter.py        # Combines multi-line transactions in dirty PDFs
│   ├── security.py             # Fernet encryption/decryption for bank passwords
│   ├── sync_manager.py         # Google Drive Backup/Restore API logic
│   ├── sync.py                 # Helper runner for sync functions
│   └── universal_bank_parser.py# Multi-bank auto-detect parsing engine (25+ banks)
│
├── templates/                  # Frontend HTML (Jinja2) templates
│   ├── base.html               # Master layout with Bootstrap & Sidebar
│   ├── dashboard.html          # FullCalendar view
│   ├── analytics.html          # Dashboard charts for income/expenses/balances
│   ├── profile.html            # User settings, password changes, account stats
│   ├── login.html / register.html
│   ├── upload.html             # Drag-and-drop file upload UI
│   ├── preview.html            # Pre-save validation screen
│   ├── statement.html          # Range-wise statement generation UI
│   ├── summary.html            # Single-day quick summary
│   ├── ledger_details.html     # Single-day full transaction list
│   └── statement_passwords.html# Settings panel to manage encrypted bank passwords
│
├── static/
│   └── css/style.css           # Custom UI styling
│
└── data/                       # Local Storage (Excluded from Git)
    ├── auth.db                 # Master auth database
    ├── users/                  # Directory containing <username>.db files
    ├── exports/                # Temporary directory for generated exports
    └── temp/                   # Temporary directory for file uploads
```

---

## 🎛️ Detailed UI Walkthrough & Button Explanations

Here is a top-to-bottom explanation of every screen, modal, dropdown, and individual button in the KC Tracker application:

### 1. Global Navigation Bar & Sidebar (Always Accessible)
The left-hand sidebar acts as the central command node. Responsive collapsible layouts optimize navigation for both desktop monitors and mobile devices.

#### Core Elements & Buttons:
- **Mobile Toggle Menu Button (`☰`):**
  - *Location:* Top corner (visible only on mobile/tablet viewports).
  - *Action:* Toggles the visibility of the sidebar navigation menu.
- **Sidebar Brand Logo Area:**
  - *Location:* Top of the sidebar.
  - *Details:* Integrates a premium brand white-accent pill wrapping the white-rendered `KC_logo.png` logo.
- **Profile User Dropdown Button (`profileBtn`):**
  - *Location:* Directly below the brand logo in the sidebar.
  - *Visuals:* Displays the user’s first initial in an upper-case circular badge (or custom profile image if uploaded) next to the active username and a subtle downward indicator chevron.
  - *Action:* Launches the floating Profile Settings dropdown list.
- **Profile Dropdown Menu Items:**
  - **`My Profile` Link:** Redirects the user to the Profile Summary page (`/profile`).
  - **`Sync Drive` / `Connect Drive` Button:** 
    - Dynamically updates based on Drive authentication status:
      - *If Unconnected:* Shows a blue **`Connect Google Drive`** button which triggers a modern dialog popup.
      - *If Request Sent:* Shows a warning-styled **`Awaiting approval...`** non-interactive status bar.
      - *If Approved:* Displays a green **`Approved! Connect Drive now`** link which launches the Google OAuth 2.0 browser authorization consent screen.
      - *If Connected:* Displays a grey-accented **`Sync Drive`** button which manually pulls down database changes and pushes up current databases.
  - **`Change Password` Link:** Activates the secure change password dialog modal overlay.
  - **`Logout` Button (Danger Accent):** Triggers the secure session invalidation dialog modal.
- **Sidebar Nav Items (`navHome`, `navUpload`, `navPasswords`, `navStatement`, `navAnalytics`):**
  - *Action:* Immediate transition between pages without losing active web states.

---

### 2. Home Dashboard & Daily Calendar Screen (`/dashboard`)
The initial screen upon loading the app. Implements a responsive interactive monthly calendar view highlighting transaction activity.

```text
+--------------------------------------------------------------+
| [☰] HOME                                     (Profile Drop)  |
+--------------------------------------------------------------+
|                                                              |
|                  <<   May 2026   >>   [Month] [Week]         |
|   +------+------+------+------+------+------+------+         |
|   | Sun  | Mon  | Tue  | Wed  | Thu  | Fri  | Sat  |         |
|   +------+------+------+------+------+------+------+         |
|   | 24   | 25   | 26   | 27   | 28   | 29   | 30   |         |
|   |      |      |      |      |      |      |      |         |
|   |      |      | [Event: Net Credit/Debit green/red] |       |
|   +------+------+------+------+------+------+------+         |
|                                                              |
+--------------------------------------------------------------+
```

#### Core Elements & Buttons:
- **FullCalendar Event Cells:**
  - *Action:* Clicking on any day block containing transaction activity fetches the daily numbers in the background and opens the quick **Daily Summary Modal**.
- **Calendar Navigation Buttons (`prev`, `next`, `today`):**
  - *Location:* Top-left header of the calendar.
  - *Action:* Shifts the active calendar layout backwards or forwards by month or returns focus to the active system date.
- **Calendar View Toggle Buttons (`Month`, `Week`):**
  - *Location:* Top-right header of the calendar.
  - *Action:* Flips the grid resolution between monthly view and detailed hourly/weekly view.
- **Daily Summary Modal (Popup Overlay):**
  - Opens on click. Showcases three clean summary cards highlighting **Total Debit**, **Total Credit**, and **Net Cash Balance** alongside a bank-specific breakdown (e.g., HDFC vs SBI balances).
  - **`View Details` Button (Blue Accent):** Redirects the user directly to the deep-dive ledger screen for that selected date (`/ledger/<date>`).
  - **`Close` Button (Grey Accent):** Closes the summary overlay.
- **Drive Access Request Modal:**
  - **Gmail Address Input Field:** User enters their registered Google account email.
  - **`Send Request` Button (Blue Accent):** Automatically log-stores the email address in `auth.db`, sets user status to "Requested", and securely transmits an instantaneous notification to the systems admin via Telegram Bot API with quick links to approve.
  - **Modal Close Icon (`✕`):** Closes the dialog modal.

---

### 3. Upload Statement Screen (`/upload`)
The ingestion portal. This page processes unstructured formats and prepares them for verification.

```text
+--------------------------------------------------------------+
| UPLOAD STATEMENT                             (Profile Drop)  |
+--------------------------------------------------------------+
|                                                              |
|  Select Bank (for password-protected files):                 |
|  [ Select Bank Dropdown  v ]                                 |
|                                                              |
|  +-------------------------------------------------------+  |
|  |                                                       |  |
|  |                 Drop your file here                   |  |
|  |       or click to browse -- CSV, Excel, PDF           |  |
|  |                                                       |  |
|  +-------------------------------------------------------+  |
|                                                              |
|                     [ Upload & Preview ]                     |
|                                                              |
+--------------------------------------------------------------+
```

#### Core Elements & Buttons:
- **Select Bank Dropdown Select Field:**
  - *Action:* Allows users to match their statement to a saved bank credential. If matched, the server pulls the bank’s encrypted PDF password from the local vault, decrypts it in memory, and bypasses PDF lock sheets.
- **Interactive Drag-and-Drop Zone Button (`uploadZone`):**
  - *Action:* Accepts file dragging or clicking anywhere within the boundary to launch the native browser file explorer. Supports PDF, CSV, XLSX, and XLS formats.
- **`Upload & Preview` Button (Success Blue Accent):**
  - *Details:* Starts disabled. Becomes interactive as soon as a valid file is loaded.
  - *Action:* Securely uploads the file to `data/temp/`, executes parsing pipelines, applies automatic merchant alias logic, caches results in a temporary JSON file (`data/temp/<username>_preview.json`), and redirects the browser to `/preview`.

---

### 4. Upload Preview Screen (`/preview`)
A protective check sheet allowing users to audit parsed transactions before saving them permanently to SQLite.

#### Core Elements & Buttons:
- **Ledger Date Blocks:** Groups transactions dynamically. If a statement has transactions spanning 5 days, it generates 5 isolated blocks.
- **Debit Table & Credit Table (Side-by-Side):**
  - Provides a complete layout view. Debits and Credits are cleanly separated into their respective sides.
- **Description Input Field (`desc-input`):**
  - *Details:* A text input block placed inside each transaction row.
  - *Action:* Allows the user to enter custom descriptions (e.g., "Dinner with friends" or "Office stationary purchase") which are saved as `user_description` alongside the raw narration.
- **`Save All to Ledger` Button (Green Success Accent):**
  - *Action:* Flattens the preview grid, grabs all values from user-edited custom description fields, inserts records bulk-wise into the user's isolated SQLite database, deletes the preview cache file from `data/temp/`, triggers a background sync to Google Drive, and routes back to the dashboard showing a success message.
- **`← Cancel` Button (Grey Outline Accent):**
  - *Action:* Discards the cached upload, deletes the temp file, and returns to the upload screen.

---

### 5. Detailed Daily Ledger Screen (`/ledger/<date>`)
An absolute deep-dive transaction layout displaying double-entry tables side-by-side.

```text
KC Tracker/
│
├── app.py                      # Main Flask application entry point, routes, auth wrappers
├── config.py                   # Environment vars, path configs, keys
├── requirements.txt            # Python dependencies
├── credentials.json            # (Required) Google Drive OAuth client secrets
├── token.pickle                # (Auto-generated) Google Drive OAuth session token
│
├── backend/                    # Core Python modules
│   ├── auth.py                 # User authentication (Register/Login/Bcrypt)
│   ├── balance_validator.py    # Math validator ensuring Balance = Prev Balance + CR - DR
│   ├── column_mapper.py        # Maps raw header names to standard internal columns
│   ├── confidence_engine.py    # Assigns confidence scores to parsed rows
│   ├── database.py             # SQLite CRUD ops for user-specific databases
│   ├── drcr_classifier.py      # Fixes debit/credit alignment issues in parsed tables
│   ├── exporter.py             # Generators for PDF, Excel, and TXT ledger exports
│   ├── extractor.py            # Merchant alias application
│   ├── format_detector.py      # Detects PDF table structures and quality
│   ├── garbage_filter.py       # Drops empty rows or non-transaction PDF junk
│   ├── hdfc_parser.py          # highly specialized fast-parser for HDFC PDFs
│   ├── ledger.py               # Formatter generating front-end grouped dictionaries
│   ├── merchant_extractor.py   # Cleans complex transaction narrations to short Merchant Names
│   ├── parser.py               # The Orchestrator for file parsing
│   ├── row_segmenter.py        # Combines multi-line transactions in dirty PDFs
│   ├── security.py             # Fernet encryption/decryption for bank passwords
│   ├── sync_manager.py         # Google Drive Backup/Restore API logic
│   ├── sync.py                 # Helper runner for sync functions
│   └── universal_bank_parser.py# Multi-bank auto-detect parsing engine (25+ banks)
│
├── templates/                  # Frontend HTML (Jinja2) templates
│   ├── base.html               # Master layout with Bootstrap & Sidebar
│   ├── dashboard.html          # FullCalendar view
│   ├── analytics.html          # Dashboard charts for income/expenses/balances
│   ├── profile.html            # User settings, password changes, account stats
│   ├── login.html / register.html
│   ├── upload.html             # Drag-and-drop file upload UI
│   ├── preview.html            # Pre-save validation screen
│   ├── statement.html          # Range-wise statement generation UI
│   ├── summary.html            # Single-day quick summary
│   ├── ledger_details.html     # Single-day full transaction list
│   └── statement_passwords.html# Settings panel to manage encrypted bank passwords
│
├── static/
│   └── css/style.css           # Custom UI styling
│
└── data/                       # Local Storage (Excluded from Git)
    ├── auth.db                 # Master auth database
    ├── users/                  # Directory containing <username>.db files
    ├── exports/                # Temporary directory for generated exports
    └── temp/                   # Temporary directory for file uploads
```

---

## ⚙️ Setup & Installation

### Prerequisites
*   Python 3.8+
*   Google Drive API Credentials (`credentials.json`)

### 1. Install Dependencies
```bash
# Clone the repository
git clone https://github.com/your-repo/kc-tracker.git
cd "KC Tracker"

# Install all Python libraries
pip install -r requirements.txt
```

### 2. Configure Environment Parameters
Copy the template configuration file into an active environment file:
```bash
cp .env.example .env
```
Open `.env` in a text editor and fill in your unique values:
```ini
# Generate a secret key for Flask session signing
SECRET_KEY="enter-a-highly-random-string-here"

# Generate a 32-url-safe-base64-encoded bytes string for AES encryption.
# You can generate this by running: 
# python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
ENCRYPTION_KEY="your-newly-generated-fernet-key-here="

# (Optional) Telegram Bot API Integration
TELEGRAM_BOT_TOKEN="your-telegram-bot-token"
TELEGRAM_ADMIN_CHAT_ID="your-personal-telegram-chat-id"
```

### 3. Setting Up the Google Drive API
1. Navigate to the [Google Cloud Console](https://console.cloud.google.com/).
2. Create a new project and search for the **Google Drive API** in the library, then click **Enable**.
3. Go to the **OAuth Consent Screen** settings, select **External**, and add your testing Google account email.
4. Click on **Credentials**, select **Create Credentials**, and choose **OAuth Client ID**.
5. Set the Application type to **Desktop App** and click create.
6. Download the resulting JSON credentials file, rename it to `credentials.json`, and place it in the root folder of this project (`KC Tracker/`).

### 4. Running the Local Server
Launch the main application file:
```bash
python app.py
```
- The local server starts at `http://127.0.0.1:5000/`.
- Open your browser, register a user account, and log in.
- The first time a user tries to connect Google Drive, a secure tab launches requesting Google authentication permissions, creating a secure token file. All future syncs happen silently in the background!
