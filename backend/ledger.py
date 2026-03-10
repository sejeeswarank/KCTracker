"""
Ledger module — generates structured ledger data from database.
Does NOT store formatted ledger. Purely dynamic generation.
Export functions are in backend/exporter.py.
"""

from backend.database import get_transactions_by_date, _get_per_bank_balances, connect_user_db


def _extract_ledger_transactions(rows):
    debit_list, credit_list = [], []
    total_debit, total_credit = 0.0, 0.0
    debit_sno, credit_sno = 1, 1

    for row in rows:
        user_desc = row.get("user_description", "")
        if row["debit"] > 0:
            debit_list.append({
                "id":          row["id"],
                "sno":         debit_sno,
                "name":        row.get("name", ""),
                "description": user_desc,
                "narration":   row["description"],
                "amount":      row["debit"],
            })
            total_debit += row["debit"]
            debit_sno   += 1

        if row["credit"] > 0:
            credit_list.append({
                "id":          row["id"],
                "sno":         credit_sno,
                "name":        row.get("name", ""),
                "description": user_desc,
                "narration":   row["description"],
                "amount":      row["credit"],
            })
            total_credit += row["credit"]
            credit_sno   += 1

    return debit_list, credit_list, total_debit, total_credit

def _score_sequence_consistency(seq):
    s = 0
    for i in range(1, len(seq)):
        exp = round(seq[i-1]["balance"] - seq[i]["debit"] + seq[i]["credit"], 2)
        if abs(exp - round(seq[i]["balance"], 2)) < 1.0:
            s += 1
    return s

def _calculate_bank_closing_balance(brows):
    with_order = [r for r in brows if r.get("stmt_order") is not None]
    if with_order:
        last = max(with_order, key=lambda r: r["stmt_order"])
        return float(last["balance"])

    if len(brows) == 1:
        return float(brows[0]["balance"])

    asc = _score_sequence_consistency(brows)
    desc = _score_sequence_consistency(list(reversed(brows)))
    return float(brows[0]["balance"] if desc > asc else brows[-1]["balance"])

def _calculate_total_closing_balance(rows, total_credit, total_debit):
    if not rows:
        return 0.0

    bank_rows = {}
    for row in rows:
        if row.get("balance") is None:
            continue
        bank = (row.get("source_bank") or "").strip() or "Unknown"
        bank_rows.setdefault(bank, []).append(row)

    if not bank_rows:
        return total_credit - total_debit

    closing_balance = 0.0
    for brows in bank_rows.values():
        closing_balance += _calculate_bank_closing_balance(brows)

    return closing_balance

def generate_ledger(username, selected_date):
    """
    Generate a structured ledger object for a specific date.
    closing_balance = sum of each bank's last balance on that date (handles multi-bank correctly).
    bank_balances   = per-bank split dict for display only (empty if single bank).
    """
    rows = get_transactions_by_date(username, selected_date)

    debit_list, credit_list, total_debit, total_credit = _extract_ledger_transactions(rows)
    closing_balance = _calculate_total_closing_balance(rows, total_credit, total_debit)

    # bank_balances = display split only (empty = show single balance bar)
    conn   = connect_user_db(username)
    cursor = conn.cursor()
    bank_balances = _get_per_bank_balances(cursor, selected_date)
    conn.close()

    return {
        "date":            selected_date,
        "debit":           debit_list,
        "credit":          credit_list,
        "total_debit":     total_debit,
        "total_credit":    total_credit,
        "closing_balance": closing_balance,
        "bank_balances":   bank_balances,
    }
