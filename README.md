<div align="center">

<img src="assets/kc_logo.png" alt="KC Tracker Logo" width="140"/>

# KC Tracker

**Banking Ledger & Statement Parser Suite**

</div>

<p align="center">
  <img src="https://img.shields.io/badge/version-1.0.0-C9A84C?style=for-the-badge&logo=git&logoColor=white" alt="Version"/>
  <img src="https://img.shields.io/badge/Platform-Windows_10_%2F_11-0078D6?style=for-the-badge&logo=windows&logoColor=white" alt="Platform"/>
  <img src="https://img.shields.io/badge/SQLite-Multi--Tenant-003B57?style=for-the-badge&logo=sqlite&logoColor=white" alt="SQLite"/>
  <img src="https://img.shields.io/badge/AES--256-Encrypted-FF6B6B?style=for-the-badge&logo=letsencrypt&logoColor=white" alt="AES-256"/>
  <img src="https://img.shields.io/badge/license-Private-gray?style=for-the-badge" alt="License"/>
</p>

<p align="center">
  A self-hosted, multi-user financial dashboard that converts raw bank statements (PDF, CSV, Excel) into a structured, searchable ledger. Built for individuals and small businesses — featuring an auto-learning merchant intelligence engine, AES-256 encrypted bank password vault, and a fully offline encrypted backup/restore system, all behind a clean glassmorphism UI.
</p>

---
## Installation


1. Go to the [Releases](../../releases) page
2. Download [KCTracker.Setup.exe](releases/v1.0.0/KCTracker.Setup.exe)
3. Run the installer and follow the on-screen steps
4. Launch **KC Tracker** from the desktop shortcut

The app opens automatically in your default browser at `http://127.0.0.1:5000`.  
A system tray icon lets you reopen or exit the app at any time.


---
## Supported Banks

<p>
  <img src="https://img.shields.io/badge/HDFC-Bank-004C8C?style=flat-square" alt="HDFC"/>
  <img src="https://img.shields.io/badge/BOB-Bank_of_Baroda-F26522?style=flat-square" alt="BOB"/>
  <img src="https://img.shields.io/badge/IOB-Indian_Overseas-0057A8?style=flat-square" alt="IOB"/>
  <img src="https://img.shields.io/badge/Indian-Bank-CC0000?style=flat-square" alt="Indian Bank"/>
  <img src="https://img.shields.io/badge/KVB-Karur_Vysya-6C1F7A?style=flat-square" alt="KVB"/>
</p>

Built-in statement parsers are available for:

**HDFC · BOB · IOB · Indian Bank · KVB**

Additional bank formats will be added in future releases.

---

## Features

- **Statement Parsing** — Upload PDF, CSV, or Excel bank statements and get clean, structured transactions automatically
- **Multi-Bank Support** — Built-in parsers for HDFC, IOB, Indian Bank, BOB, and KVB
- **Interactive Dashboard** — FullCalendar monthly view with green/red day highlights showing net daily cash flow
- **Double-Entry Ledger** — View transactions grouped by date in a debit/credit layout with inline editing
- **Merchant Intelligence** — Auto-cleans UPI/NEFT narrations and learns your custom merchant names over time
- **Analytics** — Monthly income vs expense bar charts and running balance line charts per bank
- **Statement Generator** — Export statements by preset periods (this month, last 3 months, FY, custom range)
- **Export Formats** — Download any ledger view as PDF, Excel, or plain text
- **Encrypted Vault** — Store PDF statement passwords securely — decrypted in-memory only at parse time
- **Offline Backup** — Full encrypted backup and restore with password protection; no cloud required
- **Multi-User** — Each user has an isolated database; no data is shared between accounts
- **Offline & Private** — Runs entirely on your machine; no telemetry, no internet required

---

## UI Walkthrough

### Dashboard
- Monthly calendar with colour-coded days — green for net credit, red for net debit
- Click any day to see a **Daily Summary** (total debit, credit, net balance, per-bank breakdown)
- Click **View Details** to open the full double-entry ledger for that day

### Upload
- Select your bank from the dropdown (pre-filled from your saved vault)
- Drag and drop or browse for a PDF, CSV, or Excel statement (up to 16 MB)
- Review parsed transactions before saving — a diagnostics panel warns if reconciliation fails

### Ledger View
- Side-by-side debit and credit tables for each day
- Inline edit, delete, and save custom merchant display names per transaction
- Previous / Next day navigation

### Analytics
- Monthly income vs expense bar chart
- Running balance line chart
- Per-bank balance history

### Statement Generator
- Choose from preset periods: This Month, Last Month, Last 3/6 Months, This Year, FY Current/Previous, Recent 30 Days, or a custom date range
- View the grouped ledger and export as PDF, Excel, or plain text

### Bank Password Vault
- Save encrypted PDF statement passwords per bank
- Passwords are stored with AES-256 encryption and decrypted only at parse time
- Never included in logs or exports

### Settings & Backup
- **Export Backup** — generates a password-protected `.kctbackup` file containing all your transactions, merchant aliases, and profile image
- **Import Backup** — upload a `.kctbackup` file, preview its contents, then choose **Full Restore** (overwrite) or **Merge** (add new records without touching existing ones)

---

## Security

<p>
  <img src="https://img.shields.io/badge/bcrypt-Password_Hashing-4A90D9?style=flat-square&logo=keycdn&logoColor=white" alt="bcrypt"/>
  <img src="https://img.shields.io/badge/Fernet-AES--256--CBC-FF6B6B?style=flat-square&logo=letsencrypt&logoColor=white" alt="Fernet AES-256"/>
  <img src="https://img.shields.io/badge/PBKDF2-600K_Iterations-6C63FF?style=flat-square&logo=gnuprivacyguard&logoColor=white" alt="PBKDF2"/>
  <img src="https://img.shields.io/badge/Rate_Limited-10_req%2Fmin-orange?style=flat-square&logo=cloudflare&logoColor=white" alt="Rate Limited"/>
</p>

| Concern | Implementation |
|---------|---------------|
| User passwords | bcrypt salted hash — never stored in plain text |
| Bank PDF passwords | AES-256 encrypted at rest; decrypted in-memory only during parsing |
| Session integrity | Server-side signing key; SameSite cookie policy |
| Login brute-force | Rate-limited to 10 requests/min and 50 requests/hr |
| Backup encryption | PBKDF2-HMAC-SHA256 (600,000 iterations) + AES-256 on all `.kctbackup` files |
| Data isolation | Each user has a dedicated database; no cross-user data access |

---

## Export Formats

| Format | Scope |
|--------|-------|
| PDF | Single day or date range |
| Excel (.xlsx) | Single day or date range |
| Plain Text (.txt) | Single day or date range |

Exports are generated on demand, streamed to the browser, and immediately deleted from disk.

---

## Backup Format

KC Tracker uses a custom `.kctbackup` format for offline backup and restore.

- The file header is readable without a password and shows transaction count, date range, and bank list
- The payload is fully encrypted — only decryptable with the backup password you set at export time
- **Full Restore** — overwrites the target account's data completely
- **Merge** — adds new transactions and aliases without touching existing records

---

## Requirements

- Windows 10 or Windows 11 (64-bit)
- 150~200 MB disk space
- A modern browser like (Chrome, Edge, Firefox)



<div align="center">

Made with ♥ for personal finance clarity

</div>