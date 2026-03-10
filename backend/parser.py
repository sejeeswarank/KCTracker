"""
Parser module — orchestrator for the professional parsing pipeline.

Pipeline:
    PDF → Raw Extraction → Format Detection → Row Segmentation →
    Garbage Filter → Column Mapping → DR/CR Classification →
    Balance Validation → Merchant Extraction → Confidence Scoring →
    Final Structured Output

CSV and Excel parsing is handled directly with pandas.
"""

import re
import os
import pandas as pd

from backend.format_detector import detect_format, measure_table_quality
from backend.row_segmenter import segment_rows
from backend.garbage_filter import filter_garbage
from backend.column_mapper import map_columns
from backend.drcr_classifier import classify_debit_credit
from backend.balance_validator import validate_and_correct
from backend.merchant_extractor import clean_merchant_name
from backend.confidence_engine import score_transactions


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_DATE_RE = re.compile(
    r"\d{2}[-/]\d{2}[-/]\d{2,4}"
    r"|\d{1,2}-(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-\d{2,4}",
    re.IGNORECASE
)

# Table quality threshold — below this, fall back to text extraction
_TABLE_QUALITY_THRESHOLD = 0.4

# Currency symbol stripping pattern (used across multiple functions)
_CURRENCY_STRIP_RE = r"[₹$€£,\s]"


# ---------------------------------------------------------------------------
# File type check
# ---------------------------------------------------------------------------
def allowed_file(filename):
    """Check if the uploaded file has an allowed extension."""
    from config import ALLOWED_EXTENSIONS
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


# ---------------------------------------------------------------------------
# Auto-detect file type and dispatch
# ---------------------------------------------------------------------------
def parse_statement(file_path, password=None):
    """
    Auto-detect file type and parse accordingly.
    Returns (list of transaction dicts, detected_bank_name string).
    PDF: bank name auto-detected from content.
    CSV/Excel: bank name is empty string.
    """
    ext = file_path.rsplit(".", 1)[-1].lower()
    if ext == "pdf":
        return parse_pdf(file_path, password=password)   # already returns (txns, bank)
    elif ext == "csv":
        return parse_csv(file_path), ""
    elif ext in ("xlsx", "xls"):
        return parse_excel(file_path, password=password), ""
    return [], ""


# ---------------------------------------------------------------------------
# CSV / Excel parsers
# ---------------------------------------------------------------------------
def parse_csv(file_path):
    """Parse a CSV bank statement."""
    df = pd.read_csv(file_path)
    df.columns = [str(c).strip().lower() for c in df.columns]
    return _normalize_pipeline(df)


def parse_excel(file_path, password=None):
    """Parse an Excel bank statement (supports password-protected files)."""
    if password:
        df = _decrypt_excel(file_path, password)
    else:
        df = pd.read_excel(file_path)
    df.columns = [c.strip().lower() for c in df.columns]
    return _normalize_pipeline(df)


def _decrypt_excel(file_path, password):
    """Decrypt a password-protected Excel file and return DataFrame."""
    import msoffcrypto
    import io
    with open(file_path, "rb") as f:
        decrypted = io.BytesIO()
        office_file = msoffcrypto.OfficeFile(f)
        office_file.load_key(password=password)
        office_file.decrypt(decrypted)
        decrypted.seek(0)
        return pd.read_excel(decrypted)


# ---------------------------------------------------------------------------
# PRODUCTION-GRADE PDF PARSER
# ---------------------------------------------------------------------------

def parse_pdf(file_path, password=None):
    """
    Parse a digital PDF bank statement.

    Strategy:
      1. Auto-detect bank using universal_bank_parser.detect_bank()
      2. HDFC → use dedicated hdfc_parser (highest accuracy, specialized)
      3. All other banks → use universal_bank_parser (scalable, bank-aware)
      4. If universal parser yields nothing → fall back to generic table pipeline

    Adding a new bank: only requires updating universal_bank_parser.BANK_SIGNATURES.
    No changes needed here.
    """
    try:
        import pdfplumber
    except ImportError:
        return [], ""

    bank_code = "GENERIC"
    try:
        with pdfplumber.open(file_path, password=password) as pdf:
            from backend.universal_bank_parser import detect_bank, parse_universal

            bank_code = detect_bank(pdf)
            print(f"[Parser] Bank detected: {bank_code}")

            # ── HDFC: use specialized fast-path parser ────────────
            if bank_code == "HDFC":
                try:
                    from backend.hdfc_parser import parse_hdfc_text
                    raw_rows = parse_hdfc_text(pdf)
                    if raw_rows:
                        print(f"[Parser] HDFC fast-path: {len(raw_rows)} rows")
                        df = pd.DataFrame(raw_rows)
                        return _normalize_pipeline(df, {
                            "mode": "dual_column",
                            "has_balance": True,
                            "confidence": 0.95,
                        }), bank_code
                except Exception as e:
                    print(f"[Parser] HDFC fast-path failed: {e}, trying universal")

            # ── All other banks: universal parser ─────────────────
            raw_rows = parse_universal(pdf, bank_code=bank_code)
            if raw_rows:
                print(f"[Parser] Universal parser: {len(raw_rows)} rows for {bank_code}")
                df = pd.DataFrame(raw_rows)
                print(f"[Parser] DataFrame columns: {list(df.columns)}")
                print(f"[Parser] DataFrame sample:\n{df.head(2)}")
                return _normalize_pipeline(df, {
                    "mode": "dual_column",
                    "has_balance": True,
                    "confidence": 0.90,
                }), bank_code

    except Exception as e:
        print(f"[Parser] Universal parse failed: {e}, falling back to generic table pipeline")

    # ── Final fallback: generic table extraction ──────────────────
    print("[Parser] Using generic table pipeline fallback")
    raw_rows, headers = _extract_raw_pdf(file_path, password)
    if not raw_rows:
        return [], bank_code

    format_info = detect_format(raw_rows, headers)
    print(f"[Parser] Format: {format_info['mode']}, "
          f"balance={format_info['has_balance']}, "
          f"confidence={format_info['confidence']:.2f}")

    segmented = segment_rows(raw_rows)
    filtered = filter_garbage(segmented)
    if not filtered:
        return [], bank_code

    print(f"[Parser] {len(raw_rows)} raw -> {len(segmented)} segmented -> {len(filtered)} filtered")
    df = pd.DataFrame(filtered)
    return _normalize_pipeline(df, format_info), bank_code


def _extract_raw_pdf(file_path, password):
    """
    Phase 1: Extract raw rows from PDF.
    Tries table extraction first, falls back to text.
    Returns (rows, headers).
    """
    import pdfplumber

    all_rows = []
    detected_headers = None

    try:
        with pdfplumber.open(file_path, password=password) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                quality = measure_table_quality(tables)

                if quality >= _TABLE_QUALITY_THRESHOLD and tables:
                    rows, hdrs = _parse_tables(tables)
                    all_rows.extend(rows)
                    if hdrs and not detected_headers:
                        detected_headers = hdrs
                else:
                    # Fallback: layout-aware text extraction
                    text_rows = _extract_text_rows(page)
                    all_rows.extend(text_rows)

    except Exception as e:
        raise ValueError(f"Failed to open PDF: {str(e)}")

    return all_rows, detected_headers


def _parse_tables(tables):
    """Parse pdfplumber tables into row dicts."""
    rows = []
    headers = None

    for table in tables:
        if not table:
            continue
        raw_headers, valid_header, data_rows, headers = _detect_headers(
            table, headers
        )
        for row in data_rows:
            if not row or not any(cell for cell in row if cell):
                continue
            parsed = _process_data_row(row, raw_headers, valid_header)
            if isinstance(parsed, list):
                rows.extend(parsed)
            else:
                rows.append(parsed)

    return rows, headers


def _detect_headers(table, prev_headers):
    """Detect headers from the first row of a table, or reuse previous."""
    first_row = table[0]
    first_cell = str(first_row[0]).strip() if first_row and first_row[0] else ""
    first_is_data = bool(_DATE_RE.search(first_cell))

    if not first_is_data and len(table) >= 2:
        raw_headers = [
            str(h).strip().lower().replace("\n", " ") if h else ""
            for h in first_row
        ]
        valid = sum(1 for h in raw_headers if h) >= 3
        if valid and not prev_headers:
            prev_headers = raw_headers
        return raw_headers, valid, table[1:], prev_headers

    return prev_headers, prev_headers is not None, table, prev_headers


def _process_data_row(row, raw_headers, valid_header):
    """Process a single data row — explode if packed, else build dict."""
    has_packed = any(cell and "\n" in str(cell) for cell in row)
    if has_packed:
        return _explode_packed_row(row, raw_headers if valid_header else None)

    cleaned = [str(cell).strip() if cell else "" for cell in row]
    if valid_header and raw_headers and len(cleaned) == len(raw_headers):
        return dict(zip(raw_headers, cleaned))
    if len(cleaned) >= 5:
        return {"date": cleaned[0], "description": cleaned[1],
                "debit": cleaned[2], "credit": cleaned[3], "balance": cleaned[4]}
    if len(cleaned) >= 4:
        return {"date": cleaned[0], "description": cleaned[1],
                "amount": cleaned[2], "balance": cleaned[3]}
    return {"date": "", "description": " ".join(c for c in cleaned if c)}


def _clean_description(text):
    """Clean a transaction description: non-printable chars, hyphens, whitespace."""
    if not text:
        return ""
    text = re.sub(r"[^\x20-\x7E\u00A0-\u024F]", "", text)
    text = re.sub(r"-{2,}", "-", text)
    text = re.sub(r"\s{2,}", " ", text)
    return text.strip(" -")


def _explode_packed_row(row, headers):
    """Explode a packed row into individual transaction dicts."""
    split_cells = [str(cell).split("\n") if cell else [""] for cell in row]
    col_map = _build_column_map(headers, len(split_cells))

    date_lines = split_cells[col_map.get("date", 0)]
    desc_col = col_map.get("description", 1)
    desc_lines = split_cells[desc_col] if desc_col < len(split_cells) else [""]

    balance_vals = _extract_numeric_lines(split_cells, col_map.get("balance"))
    num_txns = len(balance_vals) if balance_vals else sum(
        1 for d in date_lines if _DATE_RE.search(d.strip())
    )
    if num_txns == 0:
        return []

    txn_descs = _align_descriptions_to_dates(desc_lines, num_txns)
    result = []
    date_idx = 0

    for i in range(num_txns):
        txn = {"debit": "", "credit": ""}
        txn["date"], date_idx = _find_next_date(date_lines, date_idx)
        txn["description"] = txn_descs[i] if i < len(txn_descs) else ""
        txn["balance"] = balance_vals[i] if i < len(balance_vals) else ""
        _compute_packed_amounts(txn, i, balance_vals)
        if i == 0 and not txn["debit"] and not txn["credit"]:
            _apply_first_txn_fallback(txn, split_cells, col_map)
        result.append(txn)

    return result


def _find_next_date(date_lines, date_idx):
    """Find the next valid date in date_lines starting from date_idx."""
    while date_idx < len(date_lines):
        d = date_lines[date_idx].strip()
        date_idx += 1
        if _DATE_RE.search(d):
            return d, date_idx
    return "", date_idx


def _compute_packed_amounts(txn, i, balance_vals):
    """Compute debit/credit from balance delta for packed rows."""
    if i <= 0 or i >= len(balance_vals):
        return
    prev_bal = _parse_num(balance_vals[i - 1])
    curr_bal = _parse_num(balance_vals[i])
    if prev_bal <= 0 or curr_bal <= 0:
        return
    delta = curr_bal - prev_bal
    amount = abs(delta)
    if amount >= 0.01:
        if delta < 0:
            txn["debit"] = f"{amount:.2f}"
        else:
            txn["credit"] = f"{amount:.2f}"


def _apply_first_txn_fallback(txn, split_cells, col_map):
    """For first packed transaction, try raw withdrawal/deposit columns."""
    debit_vals = _extract_numeric_lines(split_cells, col_map.get("debit"))
    credit_vals = _extract_numeric_lines(split_cells, col_map.get("credit"))
    if debit_vals and _parse_num(debit_vals[0]) < 10_000_000:
        txn["debit"] = debit_vals[0]
    elif credit_vals and _parse_num(credit_vals[0]) < 10_000_000:
        txn["credit"] = credit_vals[0]


def _build_column_map(headers, num_cols):
    """Build column index map from headers."""
    if not headers:
        default = {"date": 0, "description": 1, "debit": 2, "credit": 3}
        if num_cols > 4:
            default["balance"] = num_cols - 1
        return default

    col_map = {}
    for i, h in enumerate(headers):
        h_lower = h.strip().lower()
        if "date" in h_lower and "value" not in h_lower:
            col_map["date"] = i
        elif h_lower in ("narration", "description", "particulars", "details"):
            col_map["description"] = i
        elif any(k in h_lower for k in ("withdrawal", "debit")):
            col_map["debit"] = i
        elif any(k in h_lower for k in ("deposit", "credit")):
            col_map["credit"] = i
        elif "balance" in h_lower:
            col_map["balance"] = i
    return col_map


def _align_descriptions_to_dates(desc_lines, num_txns):
    """
    Align multi-line narration lines to transactions using prefix anchoring.

    Each transaction's narration starts with a known prefix (UPI-, NEFT-, etc.).
    Lines without prefixes are continuations of the previous transaction.
    """
    if not desc_lines or num_txns == 0:
        return [""] * num_txns

    # Known transaction start prefixes
    prefixes = (
        "UPI-", "NEFT-", "NEFT/", "IMPS-", "IMPS/", "RTGS-", "RTGS/",
        "ATM-", "ATM/", "POS-", "POS/", "INB-", "INB/", "MOB-", "MOB/",
        "NET-", "NET/", "ATW-", "NFS-", "NFS/", "BIL-", "BIL/",
        "ACH-", "ACH/", "ECS-", "ECS/", "MMT-", "MMT/",
        "BY TRANSFER", "TO TRANSFER", "BY CLG", "TO CLG",
        "INT.PD", "INTEREST", "CHQ DEP", "CASH DEP",
    )

    # Group lines by prefix anchors
    groups = []
    current = []

    for line in desc_lines:
        stripped = line.strip()
        if not stripped:
            continue
        upper = stripped.upper()
        if any(upper.startswith(p) for p in prefixes) and current:
            # New transaction starts — save previous group
            groups.append(" ".join(current))
            current = [stripped]
        else:
            current.append(stripped)

    # Don't forget the last group
    if current:
        groups.append(" ".join(current))

    # If prefix grouping found the right count, use it
    if len(groups) == num_txns:
        return groups

    # If we got more groups than transactions, merge extras into last
    if len(groups) > num_txns:
        merged = groups[:num_txns - 1]
        merged.append(" ".join(groups[num_txns - 1:]))
        return merged

    # If fewer groups (some txns don't start with a prefix), pad with empty
    while len(groups) < num_txns:
        groups.append("")
    return groups


def _parse_num(val):
    """Parse a numeric string, removing currency symbols and commas."""
    try:
        return float(re.sub(_CURRENCY_STRIP_RE, "", str(val)))
    except (ValueError, TypeError):
        return 0.0


def _extract_numeric_lines(split_cells, col_idx):
    """Extract numeric values from a specific column's split lines."""
    if col_idx is None or col_idx >= len(split_cells):
        return []
    values = []
    for line in split_cells[col_idx]:
        line = line.strip()
        if not line:
            continue
        cleaned = re.sub(_CURRENCY_STRIP_RE, "", line)
        cleaned = re.sub(r"\s*(DR|CR|dr|cr)\s*$", "", cleaned)
        try:
            float(cleaned)
            values.append(line)
        except ValueError:
            continue
    return values


def _extract_text_rows(page):
    """Fallback: extract text with layout and parse line-by-line."""
    text = page.extract_text(layout=True)
    if not text:
        text = page.extract_text()
    if not text:
        return []

    rows = []
    for line in text.split("\n"):
        line = line.strip()
        if not line:
            continue
        match = _DATE_RE.match(line)
        if match:
            rows.append(_build_text_row_dict(match, line))
        elif rows:
            rows.append({"date": "", "description": line})
    return rows


def _build_text_row_dict(match, line):
    """Build a row dict from a date-matched text line."""
    rest = line[match.end():].strip()
    parts = rest.split()
    if len(parts) < 3:
        return {"date": match.group(), "description": rest}

    amounts = _extract_trailing_amounts(parts)
    desc_parts = parts[:len(parts) - len(amounts)]
    row_dict = {"date": match.group(), "description": " ".join(desc_parts)}
    _assign_amounts(row_dict, amounts)
    return row_dict


def _assign_amounts(row_dict, amounts):
    """Assign amount values to a row dict by position."""
    if len(amounts) >= 3:
        row_dict["debit"] = amounts[0]
        row_dict["credit"] = amounts[1]
        row_dict["balance"] = amounts[2]
    elif len(amounts) == 2:
        row_dict["amount"] = amounts[0]
        row_dict["balance"] = amounts[1]
    elif len(amounts) == 1:
        row_dict["amount"] = amounts[0]


def _extract_trailing_amounts(parts):
    """Extract numeric values from the end of a parts list."""
    amounts = []
    for part in reversed(parts):
        cleaned = re.sub(_CURRENCY_STRIP_RE, "", part)
        cleaned = re.sub(r"\s*(DR|CR|dr|cr)\s*$", "", cleaned)
        try:
            float(cleaned)
            amounts.insert(0, part)
        except ValueError:
            break
    return amounts


# ---------------------------------------------------------------------------
# Normalization Pipeline (shared by CSV, Excel, PDF)
# ---------------------------------------------------------------------------
def _normalize_pipeline(df, format_info=None):
    """
    Full normalization pipeline:
    Phase 5: Column Mapping
    Phase 6: DR/CR Classification
    Phase 7: Merchant Extraction
    Phase 8: Balance Validation
    Phase 9: Confidence Scoring
    Phase 10: Final Output
    """
    df = df.copy()

    # Phase 5: Column Mapping
    df = map_columns(df, format_info)

    # Clean dates
    df = _clean_dates(df)
    df = _clean_descriptions(df)

    if df.empty:
        return []

    # Phase 6: DR/CR Classification (balance-delta correction)
    df = classify_debit_credit(df)

    # Remove zero rows (no debit or credit)
    df = df[(df["debit"] != 0) | (df["credit"] != 0)]

    if df.empty:
        return []

    # Format dates to DD-MM-YYYY
    df = _format_dates(df)

    # Phase 8: Balance Validation
    df = validate_and_correct(df)

    # Phase 7: Merchant Name Extraction
    df["name"] = df["description"].apply(clean_merchant_name)

    # Phase 9: Confidence Scoring
    df = score_transactions(df)

    # Phase 10: Final Output
    output_cols = ["date", "name", "description", "debit", "credit", "balance"]
    if "confidence" in df.columns:
        output_cols.append("confidence")

    # Drop internal helper columns
    for col in ("_balance_valid",):
        if col in df.columns:
            df = df.drop(columns=[col])

    # Add stmt_order = original row position in the PDF (0-based)
    # This is the ground truth for finding closing balance regardless of bank print order
    df = df.reset_index(drop=True)
    df["stmt_order"] = df.index
    output_cols.append("stmt_order")

    return df[output_cols].to_dict(orient="records")


# ---------------------------------------------------------------------------
# Date cleaning helpers
# ---------------------------------------------------------------------------
def _clean_dates(df):
    """Extract first valid date and remove rows without dates."""
    df["date"] = df["date"].astype(str).str.strip()
    df["date"] = df["date"].apply(_extract_first_date)
    df = df[df["date"] != ""]
    return df[df["date"].str.len() > 4]


def _extract_first_date(date_str):
    """Extract the first valid date from a string."""
    if not date_str or date_str.lower() in ("nan", "none", ""):
        return ""
    match = _DATE_RE.search(date_str)
    return match.group() if match else ""


def _clean_descriptions(df):
    """Clean descriptions and remove empty ones."""
    df["description"] = df["description"].astype(str).apply(_clean_description)
    return df[df["description"] != ""]


def _clean_description(desc):
    """Clean a description string."""
    if not desc or desc.lower() in ("nan", "none"):
        return ""
    desc = re.sub(r"[^\x20-\x7E\u0900-\u097F]", " ", desc)
    desc = re.sub(r"\s+", " ", desc)
    desc = re.sub(r"-{2,}", "-", desc)
    return desc.strip()


def _format_dates(df):
    """Parse dates and format to DD-MM-YYYY. Handles DD-Mon-YY (IOB) and DD/MM/YY formats."""
    def _parse_single_date(d):
        import re as _re
        d = str(d).strip()
        # DD-Mon-YY or DD-Mon-YYYY (e.g. 24-Feb-26, 09-Mar-2026)
        m = _re.match(r"(\d{1,2})-(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-(\d{2,4})", d, _re.IGNORECASE)
        if m:
            day, mon, yr = m.group(1), m.group(2), m.group(3)
            if len(yr) == 2:
                yr = "20" + yr
            try:
                import datetime
                return datetime.datetime.strptime(f"{day}-{mon}-{yr}", "%d-%b-%Y")
            except Exception:
                pass
        # Standard pandas parse for everything else
        try:
            import pandas as _pd
            return _pd.to_datetime(d, dayfirst=True, errors="coerce")
        except Exception:
            return None

    df["date"] = df["date"].apply(_parse_single_date)
    df = df.dropna(subset=["date"])
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%d-%m-%Y")
    return df


# ---------------------------------------------------------------------------
# Ledger grouping — for preview and date-wise rendering
# ---------------------------------------------------------------------------
def _preprocess_ledger_df(transactions):
    df = pd.DataFrame(transactions)

    if "name" not in df.columns:
        df["name"] = df["description"].apply(clean_merchant_name)

    # Use user_description (clean) if available, else empty string — matches generate_ledger behaviour
    if "user_description" not in df.columns:
        df["user_description"] = ""
    df["user_description"] = df["user_description"].fillna("")

    # Replace NaN with proper defaults
    for col in ("debit", "credit", "balance"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    for col in ("description", "name", "date"):
        if col in df.columns:
            df[col] = df[col].fillna("")
            
    return df

def _get_true_closing_balance_df(group):
    """
    Find true end-of-day closing balance from a DataFrame group.

    Strategy 1 — stmt_order (best, always correct):
      stmt_order = original row position in the PDF statement.
      Last row by stmt_order = last transaction of the day = closing balance.
      Works for ALL banks: IOB (newest-first), HDFC (oldest-first), any future bank.

    Strategy 2 — balance-delta math (fallback for old data without stmt_order):
      Try ASC and DESC row order, pick whichever satisfies
      balance[i] = balance[i-1] - debit[i] + credit[i] more consistently.
      Last row in the correct order = closing balance.
    """
    if "balance" not in group.columns:
        return 0.0

    # Strategy 1: stmt_order present
    if "stmt_order" in group.columns and group["stmt_order"].notna().any():
        ordered = group.sort_values("stmt_order")
        last_bal = ordered.iloc[-1]["balance"]
        if pd.notna(last_bal) and last_bal != 0:
            return float(last_bal)

    # Strategy 2: balance-delta math
    bal = group["balance"].values
    dr  = group["debit"].values  if "debit"  in group.columns else [0] * len(group)
    cr  = group["credit"].values if "credit" in group.columns else [0] * len(group)

    def count_consistent(b, d, c):
        score = 0
        for i in range(1, len(b)):
            if abs(round(b[i-1] - d[i] + c[i], 2) - round(b[i], 2)) < 1.0:
                score += 1
        return score

    asc_score  = count_consistent(bal, dr, cr)
    desc_score = count_consistent(bal[::-1], dr[::-1], cr[::-1])

    return float(bal[0]) if desc_score > asc_score else float(bal[-1])


def _extract_bank_balances(group):
    """
    Per-bank closing balances.
    Uses stmt_order as ground truth when available, falls back to math.
    Works universally for all banks regardless of PDF print order.
    """
    bank_balances = {}
    if "balance" not in group.columns:
        return bank_balances

    has_source_bank = "source_bank" in group.columns
    if not has_source_bank:
        closing = _get_true_closing_balance_df(group)
        if closing:
            bank_balances["Unknown"] = closing
        return bank_balances

    banks = group["source_bank"].fillna("").apply(lambda x: str(x).strip() or "Unknown")
    for bank in banks.unique():
        bank_group = group[banks == bank]
        if bank_group.empty:
            continue
        closing = _get_true_closing_balance_df(bank_group)
        if closing:
            bank_balances[bank] = closing

    return bank_balances


def _make_ledger_records(df_, amount_col):
    records = []
    for _, row in df_.iterrows():
        records.append({
            "S.No":        int(row["S.No"]),
            "name":        str(row.get("name", "")),
            "description": str(row.get("user_description", "")),
            amount_col:    float(row[amount_col]),
        })
    return records


def _process_ledger_group(date, group):
    debit_df = group[group["debit"] > 0].copy()
    credit_df = group[group["credit"] > 0].copy()

    debit_df["S.No"] = range(1, len(debit_df) + 1)
    credit_df["S.No"] = range(1, len(credit_df) + 1)

    bank_balances = _extract_bank_balances(group)
    closing_bal   = sum(bank_balances.values()) if bank_balances else _get_true_closing_balance_df(group)

    return {
        "date":            str(date),
        "debits":          _make_ledger_records(debit_df,  "debit"),
        "credits":         _make_ledger_records(credit_df, "credit"),
        "total_debit":     float(debit_df["debit"].sum()),
        "total_credit":    float(credit_df["credit"].sum()),
        "closing_balance": float(closing_bal),
        "bank_balances":   bank_balances,
        "raw_rows":        group.to_dict(orient="records"),
    }

def group_transactions_for_ledger(transactions):
    """
    Takes a list of normalized transaction dicts.
    Returns grouped by Date with Debits/Credits, Totals, Closing Balance.
    """
    if not transactions:
        return []

    df = _preprocess_ledger_df(transactions)
    grouped_ledger = []

    for date, group in df.groupby("date", sort=False):
        grouped_ledger.append(_process_ledger_group(date, group))

    return grouped_ledger