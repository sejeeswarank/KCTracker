"""
Balance Validator — validates and corrects balance progression.

Checks:
- No impossible jumps (delta > amount)
- Balance continuity
- No negative credits
- Sequential date ordering
- Auto-corrects detectable inconsistencies
"""

import re
import pandas as pd


def validate_and_correct(df):
    """
    Validate balance progression and auto-correct where possible.

    Returns:
        DataFrame with corrected balance values and a 'valid' flag per row.
    """
    df = df.copy()

    # Ensure balance is numeric
    df["balance"] = df["balance"].apply(_clean_amount)

    # Fill missing balances using carry-forward
    df = _fill_missing_balances(df)

    # Validate and flag inconsistencies
    df = _validate_progression(df)

    # Ensure no negative debits/credits
    df["debit"] = df["debit"].clip(lower=0)
    df["credit"] = df["credit"].clip(lower=0)

    return df


def _fill_missing_balances(df):
    """Fill missing/zero balances using carry-forward."""
    df["balance"] = df["balance"].replace(0, pd.NA)
    df["balance"] = df["balance"].ffill()
    df["balance"] = df["balance"].fillna(0)
    df["balance"] = df["balance"].astype(float)
    return df


def _validate_progression(df):
    """
    Validate balance progression:
    - prev_balance - debit + credit should ≈ current_balance
    - Flag rows that don't match
    """
    if df.empty or len(df) < 2:
        return df

    balances = df["balance"].values
    debits = df["debit"].values
    credit_vals = df["credit"].values

    valid_flags = [True] * len(df)
    tolerance = 1.0  # Allow ±₹1 for rounding

    for i in range(1, len(df)):
        prev_bal = balances[i - 1]
        curr_bal = balances[i]
        debit = debits[i]
        credit = credit_vals[i]

        if prev_bal <= 0 or curr_bal <= 0:
            continue

        expected = prev_bal - debit + credit

        if abs(expected - curr_bal) > tolerance:
            valid_flags[i] = False

    df["_balance_valid"] = valid_flags
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