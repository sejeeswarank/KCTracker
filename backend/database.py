"""
Database module — SQLite CRUD operations for user-specific ledger databases.
Each user has their own isolated database at data/users/<username>.db
"""

import os
import re
import sqlite3
from datetime import datetime
from config import USERS_DB_FOLDER

CREATE_INDEX_DATE_SQL = """
    CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(date)
"""


def connect_user_db(username):
    db_path = os.path.join(USERS_DB_FOLDER, username + ".db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# Schema creation
# ---------------------------------------------------------------------------
def create_user_ledger(username):
    """
    Create all tables in the user's ledger database.
    Called once on registration.
    """
    conn = connect_user_db(username)
    cursor = conn.cursor()

    # Core ledger table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            name TEXT,
            description TEXT NOT NULL DEFAULT '',
            user_description TEXT DEFAULT '',
            debit REAL DEFAULT 0,
            credit REAL DEFAULT 0,
            balance REAL,
            source_bank TEXT DEFAULT '',
            created_at TEXT NOT NULL,
            UNIQUE(date, description, debit, credit, balance, source_bank)
        )
    """)

    # Performance index on date
    cursor.execute(CREATE_INDEX_DATE_SQL)

    # Daily summary (auto-updated on transaction insert)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_summary (
            date TEXT PRIMARY KEY,
            total_debit REAL DEFAULT 0,
            total_credit REAL DEFAULT 0,
            closing_balance REAL,
            transaction_count INTEGER DEFAULT 0
        )
    """)

    # Statement source (upload metadata — no PDF stored)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS statement_source (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_name TEXT,
            upload_date TEXT NOT NULL,
            start_date TEXT,
            end_date TEXT,
            transaction_count INTEGER DEFAULT 0
        )
    """)

    # Merchant alias table (auto-learning name correction)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS merchant_alias (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            raw_text TEXT UNIQUE NOT NULL,
            display_name TEXT NOT NULL
        )
    """)

    # Bank credentials (encrypted passwords for statement parsing)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bank_credentials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bank_name TEXT UNIQUE NOT NULL,
            encrypted_password TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)

    # Metadata (key-value store for sync time, DB version, etc.)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)

    # --- Schema migrations for existing databases ---
    _migrate_add_columns(cursor)

    conn.commit()
    conn.close()


def _migrate_add_columns(cursor):
    """Migrate legacy schema to current schema.
    Adds missing columns and recreates the table if the UNIQUE constraint
    needs to include source_bank.
    """
    cols = _get_existing_columns(cursor)
    if not cols:
        return

    if _needs_unique_constraint_migration(cursor):
        _recreate_transactions_table(cursor, cols)
        return

    _add_missing_columns(cursor, cols)


def _get_existing_columns(cursor):
    """Get the set of existing columns in the transactions table."""
    try:
        cursor.execute("PRAGMA table_info(transactions)")
        return {row[1] for row in cursor.fetchall()}
    except Exception:
        return set()


def _needs_unique_constraint_migration(cursor):
    """Check if the table was created without source_bank in the UNIQUE constraint."""
    cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='transactions'")
    create_sql_row = cursor.fetchone()
    return create_sql_row and "source_bank" not in create_sql_row[0]


def _recreate_transactions_table(cursor, cols):
    """Recreate the transactions table to update the UNIQUE constraint."""
    # Ensure all columns exist before migrating
    for col, coltype in [
        ("balance", "REAL"),
        ("name", "TEXT"),
        ("user_description", "TEXT DEFAULT ''"),
        ("source_bank", "TEXT DEFAULT ''"),
    ]:
        if col not in cols:
            try:
                cursor.execute(f"ALTER TABLE transactions ADD COLUMN {col} {coltype}")
            except Exception:
                pass

    # Recreate table with new unique constraint including source_bank
    cursor.execute("SELECT id, date, name, description, user_description, debit, credit, balance, source_bank, created_at FROM transactions")
    existing = cursor.fetchall()
    cursor.execute("DROP TABLE transactions")
    cursor.execute("""
        CREATE TABLE transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            name TEXT,
            description TEXT NOT NULL DEFAULT '',
            user_description TEXT DEFAULT '',
            debit REAL DEFAULT 0,
            credit REAL DEFAULT 0,
            balance REAL,
            source_bank TEXT DEFAULT '',
            created_at TEXT NOT NULL,
            UNIQUE(date, description, debit, credit, balance, source_bank)
        )
    """)
    cursor.execute(CREATE_INDEX_DATE_SQL)
    for row in existing:
        try:
            cursor.execute(
                "INSERT OR IGNORE INTO transactions (id, date, name, description, user_description, debit, credit, balance, source_bank, created_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                row
            )
        except Exception:
            pass


def _add_missing_columns(cursor, cols):
    """Add standard missing columns to the transactions table."""
    if "balance" not in cols:
        cursor.execute("ALTER TABLE transactions ADD COLUMN balance REAL")
    if "name" not in cols:
        cursor.execute("ALTER TABLE transactions ADD COLUMN name TEXT")
    if "user_description" not in cols:
        cursor.execute("ALTER TABLE transactions ADD COLUMN user_description TEXT DEFAULT ''")
    if "source_bank" not in cols:
        cursor.execute("ALTER TABLE transactions ADD COLUMN source_bank TEXT DEFAULT ''")


# ---------------------------------------------------------------------------
# Transaction CRUD
# ---------------------------------------------------------------------------
def insert_transaction(username, date, name, description, debit, credit, balance=None):
    """Insert a single transaction (INSERT OR IGNORE to prevent duplicates)."""
    conn = connect_user_db(username)
    cursor = conn.cursor()
    created_at = datetime.now().isoformat()
    cursor.execute(
        """INSERT OR IGNORE INTO transactions
           (date, name, description, debit, credit, balance, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (date, name, description, float(debit), float(credit),
         float(balance) if balance is not None else None, created_at),
    )
    conn.commit()
    conn.close()


def _normalize_date(date_str):
    """Normalize a date string to ISO format (YYYY-MM-DD)."""
    if not date_str:
        return date_str
    s = str(date_str).strip()
    # Already ISO: 2026-02-01
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        return s
    # DD-MM-YYYY or DD/MM/YYYY
    m = re.match(r"^(\d{2})[/-](\d{2})[/-](\d{4})$", s)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    # DD/MM/YY
    m = re.match(r"^(\d{2})[/-](\d{2})[/-](\d{2})$", s)
    if m:
        year = int(m.group(3))
        year = year + 2000 if year < 100 else year
        return f"{year:04d}-{m.group(2)}-{m.group(1)}"
    # Pandas Timestamp string: 2026-02-01 00:00:00
    m = re.match(r"^(\d{4}-\d{2}-\d{2})", s)
    if m:
        return m.group(1)
    return s


def _make_txn_fingerprint(iso_date, description, debit, credit, balance, bank):
    """
    Build a hashable deduplication key for a transaction.
    Uses a sentinel string for NULL balance so that two NULL-balance rows
    with identical other fields are correctly identified as duplicates.
    SQLite's UNIQUE constraint cannot do this because NULL != NULL in SQL.
    """
    bal_key = "__NULL__" if balance is None else round(float(balance), 6)
    return (
        iso_date,
        (description or "").strip(),
        round(float(debit), 6),
        round(float(credit), 6),
        bal_key,
        (bank or "").strip(),
    )


def insert_transactions_bulk(username, transactions, source_bank=""):
    """
    Insert multiple transactions at once (INSERT OR IGNORE).
    - Normalizes all dates to ISO (YYYY-MM-DD)
    - Stores source_bank to track which bank each transaction came from
    - Auto-updates daily_summary for affected dates
    - Deduplicates in Python before inserting so NULL-balance rows are not
      double-inserted (SQLite UNIQUE treats NULL != NULL, so two rows with
      balance=NULL and identical other fields would both pass INSERT OR IGNORE).
    """
    conn = connect_user_db(username)
    cursor = conn.cursor()
    created_at = datetime.now().isoformat()
    affected_dates = set()

    # ── Load existing fingerprints for affected dates ──────────────────────
    # We load them lazily per date as we encounter new dates.
    existing_fingerprints: set = set()
    loaded_dates: set = set()

    def _load_existing(iso_date):
        if iso_date in loaded_dates:
            return
        loaded_dates.add(iso_date)
        cursor.execute(
            "SELECT description, debit, credit, balance, source_bank FROM transactions WHERE date = ?",
            (iso_date,)
        )
        for row in cursor.fetchall():
            fp = _make_txn_fingerprint(
                iso_date,
                row[0], row[1], row[2], row[3], row[4]
            )
            existing_fingerprints.add(fp)

    for txn in transactions:
        iso_date = _normalize_date(str(txn["date"]))
        affected_dates.add(iso_date)

        _load_existing(iso_date)

        bank = source_bank or txn.get("source_bank", "")
        debit  = float(txn.get("debit", 0))
        credit = float(txn.get("credit", 0))
        balance = float(txn["balance"]) if txn.get("balance") is not None else None
        description = txn.get("description", "")

        fp = _make_txn_fingerprint(iso_date, description, debit, credit, balance, bank)

        # Skip if already exists (in DB or already seen in this batch)
        if fp in existing_fingerprints:
            continue

        existing_fingerprints.add(fp)  # mark so batch duplicates are also caught

        cursor.execute(
            """INSERT OR IGNORE INTO transactions
               (date, name, description, user_description, debit, credit, balance, source_bank, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                iso_date,
                txn.get("name", ""),
                description,
                txn.get("user_description", ""),
                debit,
                credit,
                balance,
                bank,
                created_at,
            ),
        )

    conn.commit()

    for d in affected_dates:
        _update_daily_summary(cursor, d)
    conn.commit()
    conn.close()


def _get_per_bank_balances(cursor, date):
    """
    Return {bank_name: closing_balance} for DISPLAY split only.
    closing_balance per bank = last transaction balance value for that bank on that date.
    Returns {} if only one bank (caller shows normal single balance bar).
    Returns {bank_A: x, bank_B: y} if 2+ banks exist on that date.
    """
    # Safety: ensure column exists
    try:
        cursor.execute("SELECT source_bank FROM transactions LIMIT 1")
    except Exception:
        cursor.execute("ALTER TABLE transactions ADD COLUMN source_bank TEXT DEFAULT ''")
        cursor.connection.commit()

    cursor.execute("""
        SELECT COALESCE(source_bank, '') as bank, balance
        FROM transactions
        WHERE date = ? AND balance IS NOT NULL
        ORDER BY id ASC
    """, (date,))
    rows = cursor.fetchall()
    if not rows:
        return {}

    bank_last = {}
    for row in rows:
        bank = row[0].strip() if row[0] and row[0].strip() else "Unknown"
        bank_last[bank] = float(row[1])  # last one per bank wins

    return bank_last


def _update_daily_summary(cursor, date):
    """Recalculate and upsert daily_summary for a specific date."""
    cursor.execute("""
        SELECT COALESCE(SUM(debit), 0),
               COALESCE(SUM(credit), 0),
               COUNT(*)
        FROM transactions WHERE date = ?
    """, (date,))
    total_debit, total_credit, count = cursor.fetchone()

    # Closing balance = last transaction's balance for this date
    cursor.execute("""
        SELECT balance FROM transactions
        WHERE date = ? AND balance IS NOT NULL
        ORDER BY id DESC LIMIT 1
    """, (date,))
    row = cursor.fetchone()
    closing = row[0] if row else None

    cursor.execute("""
        INSERT OR REPLACE INTO daily_summary
        (date, total_debit, total_credit, closing_balance, transaction_count)
        VALUES (?, ?, ?, ?, ?)
    """, (date, total_debit, total_credit, closing, count))


def record_statement_source(username, file_name, txn_count, start_date=None, end_date=None):
    """Record metadata about an uploaded statement."""
    conn = connect_user_db(username)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO statement_source (file_name, upload_date, start_date, end_date, transaction_count)
        VALUES (?, ?, ?, ?, ?)
    """, (file_name, datetime.now().isoformat(), start_date, end_date, txn_count))
    conn.commit()
    conn.close()


def get_transactions_by_date(username, date):
    """Get all transactions for a specific date."""
    conn = connect_user_db(username)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM transactions WHERE date = ? ORDER BY id ASC", (date,)
    )
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows


def update_transaction(username, txn_id, fields):
    """Update specific fields of a transaction by ID."""
    allowed = {"name", "user_description", "debit", "credit", "balance"}
    updates = {k: v for k, v in fields.items() if k in allowed}
    if not updates:
        return False
    set_clause = ", ".join(f"{k} = ?" for k in updates)
    values = list(updates.values()) + [txn_id]
    conn = connect_user_db(username)
    cursor = conn.cursor()
    cursor.execute(f"UPDATE transactions SET {set_clause} WHERE id = ?", values)
    # Refresh daily_summary for the affected date
    cursor.execute("SELECT date FROM transactions WHERE id = ?", (txn_id,))
    row = cursor.fetchone()
    if row:
        _update_daily_summary(cursor, row["date"])
    conn.commit()
    conn.close()
    return True


def delete_transaction(username, txn_id):
    """Delete a transaction by ID and refresh daily_summary."""
    conn = connect_user_db(username)
    cursor = conn.cursor()
    # Get date before deleting
    cursor.execute("SELECT date FROM transactions WHERE id = ?", (txn_id,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        return False
    txn_date = row["date"]
    cursor.execute("DELETE FROM transactions WHERE id = ?", (txn_id,))
    _update_daily_summary(cursor, txn_date)
    conn.commit()
    conn.close()
    return True


def get_transactions_by_range(username, start_date, end_date):
    """Get all transactions between two dates (inclusive)."""
    conn = connect_user_db(username)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM transactions WHERE date BETWEEN ? AND ? ORDER BY date ASC, id ASC",
        (start_date, end_date),
    )
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows


def get_recent_transactions(username, days=30):
    """Get all transactions from the last N days, ordered newest first (DESC)."""
    from datetime import date, timedelta
    today = date.today()
    since = (today - timedelta(days=days)).isoformat()
    conn = connect_user_db(username)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM transactions WHERE date >= ? ORDER BY date DESC, id DESC",
        (since,),
    )
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows


def get_summary_by_date(username, date):
    """Get total debit, credit, closing balance, and per-bank balances for a date."""
    conn = connect_user_db(username)
    cursor = conn.cursor()

    cursor.execute(
        """SELECT COALESCE(SUM(debit), 0) as total_debit,
                  COALESCE(SUM(credit), 0) as total_credit
           FROM transactions WHERE date = ?""",
        (date,),
    )
    row = cursor.fetchone()
    total_debit = row["total_debit"]
    total_credit = row["total_credit"]

    # Per-bank balances (display split only — does NOT affect closing_balance)
    bank_balances = _get_per_bank_balances(cursor, date)

    # Closing balance = last transaction's balance field from the bank statement
    cursor.execute(
        "SELECT balance FROM transactions WHERE date = ? AND balance IS NOT NULL ORDER BY id DESC LIMIT 1",
        (date,),
    )
    bal_row = cursor.fetchone()
    closing_balance = float(bal_row[0]) if bal_row else (total_credit - total_debit)

    conn.close()
    return {
        "total_debit":   total_debit,
        "total_credit":  total_credit,
        "balance":       closing_balance,
        "bank_balances": bank_balances,
    }


def get_all_dates_summary(username):
    """Get a summary per date for the calendar/dashboard view."""
    conn = connect_user_db(username)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT date,
               COALESCE(SUM(debit), 0)  as total_debit,
               COALESCE(SUM(credit), 0) as total_credit
        FROM transactions
        GROUP BY date
        ORDER BY date ASC
    """)
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows


def get_all_merchant_aliases(username):
    """Get all merchant aliases for the user."""
    conn = connect_user_db(username)
    cursor = conn.cursor()
    cursor.execute("SELECT raw_text, display_name FROM merchant_alias")
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows


def set_merchant_alias(username, raw_text, display_name):
    """Insert or update a merchant alias."""
    conn = connect_user_db(username)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO merchant_alias (raw_text, display_name)
        VALUES (?, ?)
        ON CONFLICT(raw_text) DO UPDATE SET display_name = excluded.display_name
    """, (raw_text, display_name))
    conn.commit()
    conn.close()


def add_bank_credential(username, bank_name, encrypted_password):
    """Add or update a bank credential."""
    conn = connect_user_db(username)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO bank_credentials (bank_name, encrypted_password, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(bank_name) DO UPDATE SET
            encrypted_password = excluded.encrypted_password,
            updated_at = excluded.updated_at
    """, (bank_name, encrypted_password, datetime.now().isoformat()))
    conn.commit()
    conn.close()


def get_bank_credential(username, bank_name):
    """Get the encrypted password for a specific bank."""
    conn = connect_user_db(username)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT encrypted_password FROM bank_credentials WHERE bank_name = ?",
        (bank_name,)
    )
    row = cursor.fetchone()
    conn.close()
    return row["encrypted_password"] if row else None


def get_all_bank_credentials(username):
    """Get all saved bank credentials."""
    conn = connect_user_db(username)
    cursor = conn.cursor()
    cursor.execute("SELECT id, bank_name, updated_at FROM bank_credentials ORDER BY bank_name")
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows


def delete_bank_credential(username, credential_id):
    """Delete a bank credential by ID."""
    conn = connect_user_db(username)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM bank_credentials WHERE id = ?", (credential_id,))
    conn.commit()
    conn.close()


def rebuild_daily_summary(username):
    """Rebuild all daily_summary rows from scratch."""
    conn = connect_user_db(username)
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT date FROM transactions")
    dates = [row[0] for row in cursor.fetchall()]
    for d in dates:
        _update_daily_summary(cursor, d)
    conn.commit()
    conn.close()


def get_bank_balances_over_time(username):
    """
    Return per-bank balance history for charting.
    Each entry: {date, bank, balance}
    """
    conn = connect_user_db(username)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT date,
               COALESCE(source_bank, 'Unknown') as bank,
               balance
        FROM transactions
        WHERE balance IS NOT NULL
        ORDER BY date ASC, id ASC
    """)
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows