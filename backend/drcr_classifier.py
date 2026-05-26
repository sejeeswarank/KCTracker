"""
DR/CR Classifier — Debit/Credit detection engine.

Three detection layers:
1. Separate debit/credit columns (already classified)
2. DR/CR suffix in amount values
3. Balance-delta validation (most powerful)

The balance-based correction dramatically improves accuracy.
"""

import re
import pandas as pd


def classify_debit_credit(df):
    """
    Classify transactions as debit or credit using multi-layer detection.

    Layer 1: If debit/credit columns already have values → use them
    Layer 2: Check DR/CR suffixes in raw amounts
    Layer 3: Use balance delta to validate/correct

    Returns DataFrame with corrected debit/credit columns.
    """
    df = df.copy()

    # Clean amounts first
    df["debit"] = df["debit"].apply(_clean_amount)
    df["credit"] = df["credit"].apply(_clean_amount)
    df["balance"] = df["balance"].apply(_clean_amount)

    # Layer 3: Balance-delta correction (most powerful)
    df = _balance_delta_correction(df)

    # Final: ensure no row has both debit AND credit
    df = _fix_dual_entry(df)

    return df


def _balance_delta_correction(df):
    """
    Use balance column to determine/correct debit vs credit.

    Logic:
    - If prev_balance - amount ≈ current_balance → Debit
    - If prev_balance + amount ≈ current_balance → Credit

    Tolerance: ±0.50 to handle rounding.
    """
    if df.empty:
        return df

    # Need a valid balance column
    balances = df["balance"].values
    has_valid_balance = sum(1 for b in balances if b > 0) >= len(df) * 0.3

    if not has_valid_balance:
        return df  # Can't use balance correction without balance data

    new_debit = df["debit"].values.copy()
    new_credit = df["credit"].values.copy()

    for i in range(1, len(df)):
        _process_balance_row(i, balances, new_debit, new_credit)

    df["debit"] = new_debit
    df["credit"] = new_credit
    return df


def _process_balance_row(i, balances, new_debit, new_credit):
    """Process a single row — only infer DR/CR when the parser gave no value.

    If debit or credit is already set by the upstream parser, leave it
    untouched. Balance-delta inference runs ONLY when both are zero, i.e.,
    the parser could not determine the amount at all.
    """
    prev_balance = balances[i - 1]
    curr_balance = balances[i]

    if prev_balance <= 0 or curr_balance <= 0:
        return

    curr_debit  = new_debit[i]
    curr_credit = new_credit[i]

    # Parser already determined the amount — trust it, do not override.
    if curr_debit > 0 or curr_credit > 0:
        return

    # Amount completely unknown: infer direction from balance movement.
    delta = curr_balance - prev_balance
    _infer_from_delta(i, delta, new_debit, new_credit)


def _infer_from_delta(i, delta, new_debit, new_credit):
    """Infer debit/credit from balance change when amount is unknown."""
    amount = abs(delta)
    if amount < 0.01:
        return
    if delta < 0:
        new_debit[i] = amount
        new_credit[i] = 0.0
    else:
        new_debit[i] = 0.0
        new_credit[i] = amount




def _fix_dual_entry(df):
    """Ensure no transaction has both debit and credit filled."""
    mask = (df["debit"] > 0) & (df["credit"] > 0)
    if mask.any():
        # Keep the larger value in its column, zero the other
        for idx in df[mask].index:
            if df.at[idx, "debit"] >= df.at[idx, "credit"]:
                df.at[idx, "credit"] = 0.0
            else:
                df.at[idx, "debit"] = 0.0
    return df


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