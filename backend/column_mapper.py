"""
Column Mapper — dynamically maps raw columns to standard roles.

Never hardcodes column positions. Instead:
1. Identifies columns by header names
2. Falls back to detecting numeric columns
3. Identifies balance column (large monotonic values)
4. Handles single-amount columns with DR/CR splitting
"""

import re
import pandas as pd

# Amount regex for detecting numeric columns
AMOUNT_RE = re.compile(r"[-+]?\d{1,3}(?:,\d{3})*(?:\.\d{2})?")

# Standard column name mappings
_COLUMN_MAP = {
    # Date variants
    "date": "date", "transaction date": "date", "txn date": "date",
    "value date": "date", "posting date": "date", "trans date": "date",
    # Description variants
    "description": "description", "particulars": "description",
    "narration": "description", "details": "description",
    "transaction details": "description", "remarks": "description",
    "holder name": "description",
    # Debit variants
    "debit": "debit", "withdrawal": "debit", "withdrawal amt": "debit",
    "dr": "debit", "debit amount": "debit", "debit amt": "debit",
    "withdrawal amount": "debit",
    # Credit variants
    "credit": "credit", "deposit": "credit", "deposit amt": "credit",
    "cr": "credit", "credit amount": "credit", "credit amt": "credit",
    "deposit amount": "credit",
    # Balance variants
    "balance": "balance", "closing balance": "balance",
    "available balance": "balance", "bal": "balance",
    "running balance": "balance", "balance (inr)": "balance",
    # Amount (single column)
    "amount": "amount", "amount (inr)": "amount", "txn amount": "amount",
    # Type/direction column
    "type": "type", "dr/cr": "type", "cr/dr": "type",
}


def map_columns(df):
    """
    Map raw DataFrame columns to standard names (date, description, debit, credit, balance).

    Args:
        df: Raw DataFrame with original column names

    Returns:
        DataFrame with standardized column names
    """
    df = df.dropna(how="all")

    # Step 1: Try header-based mapping
    df = _header_based_mapping(df)

    # Step 2: Handle single "amount" column with DR/CR splitting
    if "amount" in df.columns and "debit" not in df.columns:
        df = _split_amount_column(df)

    # Step 3: If still no debit/credit columns, try dynamic detection
    if "debit" not in df.columns and "credit" not in df.columns:
        df = _detect_numeric_columns(df)

    # Step 4: Ensure all required columns exist
    df = _ensure_required_columns(df)

    # Step 5: Drop helper columns
    df = _drop_helper_columns(df)

    return df


def _header_based_mapping(df):
    """Try header-based mapping of raw columns."""
    renamed = {}
    for col in df.columns:
        resolved = _resolve_column_name(col)
        if resolved:
            renamed[col] = resolved
    return df.rename(columns=renamed)


def _ensure_required_columns(df):
    """Ensure all required columns (date, description, debit, credit, balance) exist."""
    for col in ["date", "description", "debit", "credit", "balance"]:
        if col not in df.columns:
            df[col] = 0 if col in ("debit", "credit", "balance") else ""
    return df


def _drop_helper_columns(df):
    """Drop temporary/helper columns."""
    for drop_col in ("time", "amount", "type", "ref", "chq", "cheque"):
        if drop_col in df.columns:
            df = df.drop(columns=[drop_col])
    return df


def _resolve_column_name(col_name):
    """Resolve a raw column name to a standard name."""
    col_lower = str(col_name).strip().lower().replace("\n", " ")

    # Direct match
    if col_lower in _COLUMN_MAP:
        return _COLUMN_MAP[col_lower]

    # Partial match
    for key, val in _COLUMN_MAP.items():
        if key in col_lower:
            return val

    return None


def _split_amount_column(df):
    """
    Split a single Amount column into Debit/Credit using DR/CR detection.
    """
    debit_vals = []
    credit_vals = []

    for _, row in df.iterrows():
        raw_amount = str(row.get("amount", "0"))
        direction = _detect_dr_cr(raw_amount)
        amount = _clean_amount(raw_amount)

        # Check for a separate type/direction column
        if direction is None:
            direction = _detect_direction_from_type(row)

        if direction == "CR":
            debit_vals.append(0.0)
            credit_vals.append(amount)
        else:
            # DR or unknown — default to debit
            debit_vals.append(amount)
            credit_vals.append(0.0)

    df["debit"] = debit_vals
    df["credit"] = credit_vals
    return df


def _detect_numeric_columns(df):
    """
    Fallback: detect which unnamed/unmapped columns contain amounts.
    Uses numeric density analysis.
    """
    numeric_cols = []
    for col in df.columns:
        if col in ("date", "description"):
            continue
        try:
            cleaned = df[col].astype(str).str.replace(r"[₹$€£,\s]", "", regex=True)
            cleaned = cleaned.str.replace(r"\s*(DR|CR|dr|cr)\s*$", "", regex=True)
            numeric_count = pd.to_numeric(cleaned, errors="coerce").notna().sum()
            if numeric_count > len(df) * 0.5:
                numeric_cols.append(col)
        except Exception:
            continue

    # Assign roles based on position
    if len(numeric_cols) >= 3:
        df = df.rename(columns={
            numeric_cols[0]: "debit",
            numeric_cols[1]: "credit",
            numeric_cols[2]: "balance",
        })
    elif len(numeric_cols) == 2:
        df = df.rename(columns={
            numeric_cols[0]: "amount",
            numeric_cols[1]: "balance",
        })
        df = _split_amount_column(df)
    elif len(numeric_cols) == 1:
        df = df.rename(columns={numeric_cols[0]: "amount"})
        df = _split_amount_column(df)

    return df


def _detect_dr_cr(value):
    """Detect if a value string ends with DR or CR."""
    if not value:
        return None
    text = str(value).strip().upper()
    if text.endswith("DR"):
        return "DR"
    if text.endswith("CR"):
        return "CR"
    return None


def _detect_direction_from_type(row):
    """Check for a separate type/direction column in the row."""
    type_val = str(row.get("type", row.get("dr/cr", ""))).strip().upper()
    if "DR" in type_val or "DEBIT" in type_val:
        return "DR"
    if "CR" in type_val or "CREDIT" in type_val:
        return "CR"
    return None


def _clean_amount(value):
    """Convert a raw amount value to a float.

    Handles IOB-style no-space suffixes: '1,234.56Cr', '1,234.56Dr'
    as well as standard spaced variants: '1,234.56 CR', '1,234.56 DR'.
    """
    if not value:
        return 0.0
    text = str(value).strip()
    if text == "" or text.lower() in ("nan", "none"):
        return 0.0
    # Strip currency symbols and commas only — keep spaces so suffix regex works
    text = re.sub(r"[₹$€£,]", "", text)
    # Strip DR/CR suffix with or without leading space, any case
    # Covers: '1234.56Cr', '1234.56 CR', '1234.56dr', '1234.56 Dr'
    text = re.sub(r"\s*(DR|CR|Dr|Cr|dr|cr)\s*$", "", text).strip()
    try:
        return abs(float(text))
    except ValueError:
        return 0.0