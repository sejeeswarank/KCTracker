# KC Tracker

KC Tracker is a complete bank statement parsing and ledger management system designed to convert disorganized raw transaction data (from PDFs, CSVs, and Excel files) into structured, clean ledgers. It provides features to track income, expenses, view daily summaries, auto-categorize merchants, securely handle banking passwords locally, and seamlessly sync everything to Google Drive.

## 🚀 Features

*   **Multi-format Parsing:** Supports uploading PDF (native and scanned-text PDF fallback), CSV, and Excel (XLSX/XLS) bank statements.
*   **Intelligent Universal PDF Engine:**
    *   Auto-detects banks from PDF content using multi-signal confidence scoring (keywords, column fingerprints, footers).
    *   Supports 25+ major Indian banks out of the box including: HDFC, ICICI, SBI, Axis, Kotak, Bank of Baroda, PNB, YES, IndusInd, IOB, Canara, Federal, Indian Bank, and many more.
    *   Generic fallback parser for unknown banks.
    *   Table extraction via `pdfplumber` with fallback to layout-aware text extraction.
*   **Data Normalization Pipeline:**
    *   Garbage filters for empty/useless rows.
    *   Auto column mapping (`date`, `description`, `debit`, `credit`, `balance`).
    *   Balance validation and Debit/Credit correction.
    *   Automatic merchant name cleaning and learning (Alias mapping).
*   **Per-Bank Balance Tracking:** Automatically segregates and tracks closing balances for each bank independently, displaying them side-by-side in daily views.
*   **Local Secure Password Management:** Bank statement passwords are encrypted using Fernet (symmetric encryption) and stored strictly locally to open protected PDFs. Fully offline parsing ensures privacy.
*   **Google Drive Sync:** Bidirectional synchronization of user data databases (`users/<username>.db`) and the main auth database (`auth.db`) with Google Drive via the official API for seamless backups.
*   **Dynamic Ledger Dashboard & Analytics:**
    *   Interactive FullCalendar showing daily balances.
    *   Visual Analytics charts tracking income, expenses, and balances over time.
    *   Daily Summary and Detailed Ledger views.
    *   Filter statements by dynamic periods (This Month, Current FY, etc.) or custom date ranges.
    *   Ability to manually add missing or cash transactions.
*   **User Profiles:** Manage account details, upload profile pictures, track global account statistics, and change passwords.
*   **Exporting:** Export daily or range-wise ledgers cleanly to **PDF**, **Excel**, and **Plain Text (TXT)**.

---

## 🏗 System Architecture & Workflow

### 1. Data Flow Pipeline
1.  **Upload:** User selects a file (PDF/CSV/Excel) and optionally selects a pre-saved Bank Name to provide the decryption password.
2.  **Parser Engine (`backend/parser.py`):**
    *   Auto-detects the file type.
    *   Decrypts the file if necessary using the locally stored password.
    *   Extracts raw rows.
    *   Puts rows through a 10-phase normalization pipeline (Segmentation, Garbage Filter, Column Mapping, DR/CR Classification, Merchant Extraction, Balance Correction).
3.  **Preview:** Processed transactions are shown to the user in a grouped UI (`preview.html`).
4.  **Database Insert (`backend/database.py`):** The accepted transactions are saved into the SQLite database (`data/users/<username>.db`). Duplicate prevention (`INSERT OR IGNORE`) ensures idempotent uploads based on `(date, description, debit, credit, balance)`.
5.  **Sync (`backend/sync_manager.py`):** Immediately after saving, the user's local database is uploaded to the Google Drive folder `LedgerApp/users/`.
6.  **Viewing:** User accesses Dashboard. The calendar fetches daily summaries from the database. Clicking a date opens the `Ledger` or `Summary` view.
7.  **Exporting (`backend/exporter.py`):** Users can export their ledgers. Temporary files are created, sent to the browser, and instantly deleted from the server.

### 2. Database Design (SQLite)
*   **`data/auth.db`**: Stores registered users and hashed passwords.
*   **`data/users/<username>.db`**: Per-user isolated database.
    *   `transactions`: Core ledger.
    *   `daily_summary`: Auto-calculated balance and totals per day.
    *   `merchant_alias`: Auto-learning system mapping raw bank descriptions to clean Merchant names.
    *   `bank_credentials`: Locally encrypted passwords for opening bank PDFs.
    *   `statement_source`: Metadata about files that were uploaded.

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
pip install -r requirements.txt
```

### 2. Configure Environment
1.  Copy `.env.example` to `.env` (or create a new `.env` file).
2.  Set your secure keys:
```ini
# .env
SECRET_KEY="your-secure-flask-session-key"

# REQUIRED: 32-url-safe-base64-encoded bytes string for Fernet encryption
# Example: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
ENCRYPTION_KEY="your-generated-fernet-key-here="
```

### 3. Google Drive API
1.  Go to Google Cloud Console.
2.  Create a project and enable the **Google Drive API**.
3.  Create **OAuth 2.0 Client IDs** (Desktop App).
4.  Download the JSON file and rename it to `credentials.json`.
5.  Place `credentials.json` in the root folder of the project (`KC Tracker/`).

### 4. Run the Application
```bash
python app.py
```
*   The first time the app runs, it will open a browser window asking you to authenticate with Google.
*   Once authorized, it creates `token.pickle`.
*   The application will be accessible at `http://127.0.0.1:5000/`.

---

## 💻 Tech Stack

- **Backend:** Python 3, Flask, SQLite3
- **Data Processing:** pandas, pdfplumber, PyPDF2
- **Frontend:** HTML5, CSS3, Bootstrap 5, FullCalendar, Chart.js
- **Security:** cryptography (Fernet encryption), bcrypt
- **Cloud Integration:** Google Drive API

---

## 📸 Screenshots

*(Add screenshots of your application here to showcase the dashboard, ledger, and visualizations)*

---

## 🤝 Contributing

Contributions, issues, and feature requests are welcome! 

1. Fork the project.
2. Create your feature branch (`git checkout -b feature/AmazingFeature`).
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`).
4. Push to the branch (`git push origin feature/AmazingFeature`).
5. Open a Pull Request.

---

## 📄 License

This project is licensed under the MIT License.
