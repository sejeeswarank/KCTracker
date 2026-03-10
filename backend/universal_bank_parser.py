"""
universal_bank_parser.py
Universal Bank Statement Parser for KC Tracker.

Auto-detects the bank from PDF content using multi-signal confidence scoring:
  - Bank name keywords
  - Column header fingerprints (unique to each bank)
  - Footer/watermark phrases
  - Narration pattern signatures

Currently supports:
  - HDFC Bank              - IOB (Indian Overseas Bank)
  - ICICI Bank             - Canara Bank
  - SBI                    - Union Bank of India
  - Axis Bank              - Bank of India
  - Kotak Mahindra Bank    - Federal Bank
  - Bank of Baroda         - IDBI Bank
  - Punjab National Bank   - Karnataka Bank
  - Yes Bank               - South Indian Bank
  - IndusInd Bank          - UCO Bank
  - Central Bank of India  - Dhanlaxmi Bank
  - Tamil Nadu Mercantile  - City Union Bank
  - Generic (fallback for any unknown bank)

Architecture:
  detect_bank() → confidence scoring → best match → parse_with_config()
  All banks share the same normalization pipeline after extraction.
"""

import re
import pandas as pd

# ---------------------------------------------------------------------------
# Shared string constants (avoids SonarQube S1192 duplicate literal warnings)
# ---------------------------------------------------------------------------
_S_TXN_REMARKS    = "TRANSACTION REMARKS"
_S_IOB            = "INDIAN OVERSEAS BANK"
_S_DEBIT_RS       = "DEBIT(RS)"
_S_CREDIT_RS      = "CREDIT(RS)"
_S_FEDERAL        = "THE FEDERAL BANK"
_S_TMB            = "TMB BANK"
_S_REF_NO         = "REF NO"
_S_DEBIT          = "DEBIT"
_S_CREDIT         = "CREDIT"
_S_BALANCE        = "BALANCE"
_S_PARTICULARS    = "PARTICULARS"

# ---------------------------------------------------------------------------
# Amount / Date patterns
# ---------------------------------------------------------------------------
_AMOUNT_RE   = re.compile(r"[\d,]+\.\d{2}")
_CURRENCY_RE = re.compile(r"[₹$€£,\s]")

# Date patterns for different banks
_DATE_DMY_SLASH  = re.compile(r"\b(\d{2}/\d{2}/\d{4})\b")          # DD/MM/YYYY
_DATE_DMY_DASH   = re.compile(r"\b(\d{2}-\d{2}-\d{4})\b")           # DD-MM-YYYY
_DATE_DMY_SHORT  = re.compile(r"\b(\d{2}/\d{2}/\d{2})\b")           # DD/MM/YY
_DATE_LONG       = re.compile(                                        # DD Mon YYYY
    r"\b(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4})\b",
    re.IGNORECASE
)

# Date pattern: DD-Mon-YY (e.g. 24-Feb-26, 09-Feb-26)
_DATE_DMY_MON_SHORT = re.compile(
    r"\b(\d{1,2}-(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-\d{2})\b",
    re.IGNORECASE
)

_MONTH_MAP = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04",
    "may": "05", "jun": "06", "jul": "07", "aug": "08",
    "sep": "09", "oct": "10", "nov": "11", "dec": "12",
}

# ---------------------------------------------------------------------------
# Bank Signatures — used to auto-detect bank from PDF text
# Add new banks here. Keywords are checked against first 2 pages (uppercase).
# ---------------------------------------------------------------------------
BANK_SIGNATURES = {
    "HDFC": {
        "keywords": ["HDFC BANK", "HDFCBANK"],
        "confirm":  ["WITHDRAWALAMT", "DEPOSITAMT", "CLOSINGBALANCE", "VALUEDT"],
    },
    "ICICI": {
        "keywords": ["ICICI BANK", "ICICIBANKLTD", "ICICIBANK"],
        "confirm":  [_S_TXN_REMARKS, "WITHDRAWALAMOUNT", "S.NO.VALUEDATE"],
    },
    "SBI": {
        "keywords": ["STATE BANK OF INDIA", "SBI YONO", "ONLINESBI"],
        "confirm":  [],
    },
    "AXIS": {
        "keywords": ["AXIS BANK", "AXISBANK"],
        "confirm":  ["TRANDATE", "TRANSACTIONREMARKS", "WITHDRAWAL"],
    },
    "KOTAK": {
        "keywords": ["KOTAK MAHINDRA", "KOTAK BANK"],
        "confirm":  [],
    },
    "BOB": {
        "keywords": ["BANK OF BARODA", "BANKOFBARODA", "BARODABANK", "BOB"],
        "confirm":  ["DEBITAMOUNT", "CREDITAMOUNT", "RUNNINGBALANCE"],
    },
    "PNB": {
        "keywords": ["PUNJAB NATIONAL BANK", "PNB"],
        "confirm":  [],
    },
    "YES": {
        "keywords": ["YES BANK", "YESBANK"],
        "confirm":  [],
    },
    "INDUSIND": {
        "keywords": ["INDUSIND BANK", "INDUSINDBK"],
        "confirm":  [],
    },
    "IOB": {
        "keywords": [_S_IOB, "INDIANOVERSEAS", "IOB"],
        "confirm":  [_S_DEBIT_RS, _S_CREDIT_RS, "EFFECTIVEAVAILABLEBALANCE"],
    },
    "CANARA": {
        "keywords": ["CANARA BANK", "CANARABANK"],
        "confirm":  [],
    },
    "UNION": {
        "keywords": ["UNION BANK OF INDIA", "UNIONBANK"],
        "confirm":  [],
    },
    "BOI": {
        "keywords": ["BANK OF INDIA", "BANKOFINDIA"],
        "confirm":  [],
    },
    "FEDERAL": {
        "keywords": ["FEDERAL BANK", "FEDERALBANK", _S_FEDERAL],
        "confirm":  ["FEDERALBANK", "ALUVA", "FEDERALNETBANKING"],
    },
    "IDBI": {
        "keywords": ["IDBI BANK", "IDBIBANK"],
        "confirm":  [],
    },
    "INDIANBANK": {
        "keywords": ["INDIAN BANK", "INDIANBANK"],
        "confirm":  [_S_DEBIT, _S_CREDIT, _S_PARTICULARS],
    },
    "KARNATAKA": {
        "keywords": ["KARNATAKA BANK", "KTKBANK"],
        "confirm":  [],
    },
    "SIB": {
        "keywords": ["SOUTH INDIAN BANK", "SOUTHINDIANBANK"],
        "confirm":  [],
    },
    "UCO": {
        "keywords": ["UCO BANK", "UCOBANK"],
        "confirm":  [],
    },
    "CENTRAL": {
        "keywords": ["CENTRAL BANK OF INDIA", "CENTRALBANKOFIN"],
        "confirm":  [],
    },
    "DHAN": {
        "keywords": ["DHANLAXMI BANK", "DHANBANK", "DLBANK"],
        "confirm":  [],
    },
    "TNMB": {
        "keywords": ["TAMIL NADU MERCANTILE", "TNMBANK", _S_TMB, "TMBBANK"],
        "confirm":  ["TAMILNADUMERCANTILE", "TUTICORIN", "TMBBANK"],
    },
    "CUB": {
        "keywords": ["CITY UNION BANK", "CITYUNIONBANK", "CUB"],
        "confirm":  ["CITYUNIONBANK"],
    },
    "RBL": {
        "keywords": ["RBL BANK", "RATNAKAR BANK"],
        "confirm":  [],
    },
    "BANDHAN": {
        "keywords": ["BANDHAN BANK", "BANDHANBANK"],
        "confirm":  [],
    },
    "EQUITAS": {
        "keywords": ["EQUITAS SMALL FINANCE", "EQUITASBANK"],
        "confirm":  [],
    },
    "UJJIVAN": {
        "keywords": ["UJJIVAN SMALL FINANCE", "UJJIVANBANK"],
        "confirm":  [],
    },
}

# ---------------------------------------------------------------------------
# Column header fingerprints — unique column name patterns per bank
# Used as additional confidence signal in bank detection
# ---------------------------------------------------------------------------
BANK_COLUMN_FINGERPRINTS = {
    "HDFC":      ["WITHDRAWALAMT", "DEPOSITAMT", "WITHDRAWAL AMT", "DEPOSIT AMT",
                  "CHQREFNO", "VALUEDT", "CLOSINGBALANCE", "HDFCBANK"],
    "ICICI":     [_S_TXN_REMARKS, "CHEQUE NUMBER", "S NO", "VALUE DATE",
                  "WITHDRAWAL AMOUNT", "DEPOSIT AMOUNT", "ICICIBANKLTD", "ICICIBANK"],
    "SBI":       ["TXN DATE", "VALUE DATE", "DESCRIPTION", _S_REF_NO, _S_DEBIT, _S_CREDIT,
                  "STATEBANKOFINDIA", "SBIBANK", "YONOSBI", "ONLINESBI"],
    "AXIS":      ["TRAN DATE", _S_TXN_REMARKS, "WITHDRAWAL", "DEPOSIT",
                  "AXISBANK", "AXIS BANK LIMITED", "CHQ NO", "TRAN ID"],
    "IOB":       [_S_DEBIT_RS, _S_CREDIT_RS, "BALANCE(RS)", "TRANSACTION TYPE",
                  _S_PARTICULARS, _S_REF_NO, "CHEQUE NO"],
    "KOTAK":     ["INSTRUMENT NO", "TRANSACTION ID", _S_DEBIT, _S_CREDIT,
                  "KOTAKMAHINDRA", "KOTAK MAHINDRA BANK", "KOTAKBANK"],
    "BOB":       ["DEBIT AMOUNT", "CREDIT AMOUNT", "RUNNING BALANCE", "TRANSACTION DATE"],
    "PNB":       [_S_DEBIT, _S_CREDIT, "NARRATION", _S_REF_NO],
    "CANARA":    ["DR AMOUNT", "CR AMOUNT", _S_BALANCE, "CANARABANK",
                  "CANARA BANK", "CBSSUPPORT", "DRAWAL"],
    "FEDERAL":   [_S_DEBIT, _S_CREDIT, _S_PARTICULARS, _S_BALANCE,
                  "FEDERALBANK", _S_FEDERAL, "FEDERAL BANK LIMITED",
                  "CHERANALLUR", "FEDERALNETBANKING"],
    "KARNATAKA": ["DR", "CR", _S_PARTICULARS, _S_BALANCE],
    "SIB":       [_S_DEBIT, _S_CREDIT, "DESCRIPTION", _S_BALANCE],
    "CUB":       [_S_DEBIT, _S_CREDIT, "NARRATION", _S_BALANCE],
    "TNMB":      [_S_DEBIT, _S_CREDIT, _S_PARTICULARS, _S_BALANCE,
                  "TAMILNADUMERCANTILE", _S_TMB, "TMBBANK", "TUTICORIN"],
    "INDIANBANK": [_S_DEBIT, _S_CREDIT, _S_PARTICULARS, _S_BALANCE, "VALUEDATE"],
}

# Footer/watermark phrases unique to each bank
BANK_FOOTER_SIGNATURES = {
    "IOB":       ["EFFECTIVE AVAILABLE BALANCE", _S_IOB, "IOB.IN",
                  "ANNA SALAI", "CHENNAI 600002"],
    "HDFC":      ["CLOSING BALANCE INCLUDES", "CONTENTS OF THIS STATEMENT",
                  "HDFC BANK LIMITED", "HDFCBANK.COM", "REGD OFFICE"],
    "SBI":       ["THIS IS A COMPUTER GENERATED", "STATE BANK OF INDIA"],
    "ICICI":     ["ICICI BANK LIMITED", "ICICIPRULIFE", "ICICIBANK.COM",
                  "ICICI BANK TOWERS", "BANDRA KURLA"],
    "AXIS":      ["AXIS BANK LIMITED", "AXISBANK.COM", "AXIS BANK LTD",
                  "BOMBAY DYEING MILLS"],
    "KOTAK":     ["KOTAK MAHINDRA BANK", "KOTAK.COM", "KOTAK MAHINDRA BANK LTD",
                  "KOTAK811"],
    "FEDERAL":   [_S_FEDERAL, "FEDERAL BANK LIMITED", "FEDERALBANK.NET",
                  "ALUVA", "FEDERAL BANK LTD"],
    "KARNATAKA": ["KARNATAKA BANK LIMITED"],
    "SIB":       ["THE SOUTH INDIAN BANK"],
    "CUB":       ["CITY UNION BANK"],
    "TNMB":      ["TAMIL NADU MERCANTILE BANK", "TMBBANK.IN", _S_TMB,
                  "TAMILNADUMERCANTILEBANK", "TUTICORIN"],
    "UCO":       ["UCO BANK"],
    "CENTRAL":   ["CENTRAL BANK OF INDIA"],
    "BANDHAN":   ["BANDHAN BANK"],
    "EQUITAS":   ["EQUITAS SMALL FINANCE BANK"],
    "UJJIVAN":   ["UJJIVAN SMALL FINANCE BANK"],
    "INDIANBANK": ["INDIAN BANK", "INDIANBANK.IN"],
    "BOB":       ["BANK OF BARODA", "BANKOFBARODA.IN", "BOB WORLD"],
}

# ---------------------------------------------------------------------------
# Bank-specific skip patterns — lines to ignore during parsing
# ---------------------------------------------------------------------------
_COMMON_SKIPS = [
    re.compile(r"Account\s*(No|Number|Name|Branch|Statement)", re.IGNORECASE),
    re.compile(r"Opening\s*Balance", re.IGNORECASE),
    re.compile(r"Closing\s*Balance", re.IGNORECASE),
    re.compile(r"Total\s*(Debit|Credit|Amount)", re.IGNORECASE),
    re.compile(r"Page\s*\d+\s*(of\s*\d+)?", re.IGNORECASE),
    re.compile(r"Generated\s*On", re.IGNORECASE),
    re.compile(r"IFSC\s*(Code)?", re.IGNORECASE),
    re.compile(r"Statement\s*(of\s*Account|Period|Summary)", re.IGNORECASE),
    re.compile(r"Branch\s*Name", re.IGNORECASE),
    re.compile(r"Customer\s*(ID|Name|No)", re.IGNORECASE),
    re.compile(r"Nomination", re.IGNORECASE),
    re.compile(r"https?://", re.IGNORECASE),
    re.compile(r"GST(N|IN)?\s*:", re.IGNORECASE),
    re.compile(r"^\s*$"),
    re.compile(r"^\s*[-=_]{5,}\s*$"),  # separator lines
]

BANK_SKIP_PATTERNS = {
    "HDFC": [
        re.compile(r"HDFC\s*BANK\s*LIMITED", re.IGNORECASE),
        re.compile(r"Closing\s*balance\s*includes", re.IGNORECASE),
        re.compile(r"Contents\s*of\s*this\s*statement", re.IGNORECASE),
        re.compile(r"State\s*account\s*branch", re.IGNORECASE),
        re.compile(r"Registered\s*Office", re.IGNORECASE),
        re.compile(r"From\s*:\s*\d{2}/\d{2}", re.IGNORECASE),
        re.compile(r"AccountBranch|AccountNo", re.IGNORECASE),
        re.compile(r"JOINT\s*HOLDERS", re.IGNORECASE),
        re.compile(r"^\s*Date\s+Narration", re.IGNORECASE),
    ],
    "ICICI": [
        re.compile(r"ICICI\s*BANK", re.IGNORECASE),
        re.compile(r"S\.?\s*No\.?\s*Value\s*Date", re.IGNORECASE),
        re.compile(r"Transaction\s*Remarks", re.IGNORECASE),
        re.compile(r"Withdrawal\s*Deposit\s*Balance", re.IGNORECASE),
        re.compile(r"^\s*Cheque\s*Number", re.IGNORECASE),
    ],
    "SBI": [
        re.compile(r"State\s*Bank\s*of\s*India", re.IGNORECASE),
        re.compile(r"Txn\s*Date\s+Value\s*Date", re.IGNORECASE),
        re.compile(r"Ref\s*No\.\s*/\s*Cheque", re.IGNORECASE),
        re.compile(r"SBI\s*(YONO|BANK|NET)", re.IGNORECASE),
        re.compile(r"^\s*Debit\s+Credit\s+Balance", re.IGNORECASE),
    ],
    "AXIS": [
        re.compile(r"AXIS\s*BANK", re.IGNORECASE),
        re.compile(r"Tran\s*Date\s+Chq", re.IGNORECASE),
        re.compile(r"Transaction\s*Remarks\s+Withdrawal", re.IGNORECASE),
    ],
    "KOTAK": [
        re.compile(r"KOTAK\s*MAHINDRA", re.IGNORECASE),
        re.compile(r"Instrument\s*No", re.IGNORECASE),
    ],
    "IOB": [
        re.compile(r"INDIAN OVERSEAS BANK", re.IGNORECASE),
        re.compile(r"Date\s*\(Value", re.IGNORECASE),
        re.compile(r"Particulars\s+Ref", re.IGNORECASE),
        re.compile(r"Transaction\s*Type", re.IGNORECASE),
        re.compile(r"Debit\s*\(Rs\)", re.IGNORECASE),
        re.compile(r"Effective\s*available\s*balance", re.IGNORECASE),
        re.compile(r"Branch\s*Code", re.IGNORECASE),
        re.compile(r"^\(\d{1,2}-", re.IGNORECASE),   # skip (24-Feb-26) value date lines
    ],
    "CANARA": [
        re.compile(r"CANARA BANK", re.IGNORECASE),
    ],
    "UNION": [
        re.compile(r"UNION BANK", re.IGNORECASE),
    ],
    "KARNATAKA": [
        re.compile(r"KARNATAKA\s*BANK", re.IGNORECASE),
        re.compile(r"Tran\s*Date\s+Particulars", re.IGNORECASE),
    ],
    "SIB": [
        re.compile(r"SOUTH\s*INDIAN\s*BANK", re.IGNORECASE),
    ],
    "UCO": [
        re.compile(r"UCO\s*BANK", re.IGNORECASE),
    ],
    "CENTRAL": [
        re.compile(r"CENTRAL\s*BANK\s*OF\s*INDIA", re.IGNORECASE),
    ],
    "CUB": [
        re.compile(r"CITY\s*UNION\s*BANK", re.IGNORECASE),
        re.compile(r"CUB\s*Account", re.IGNORECASE),
    ],
    "TNMB": [
        re.compile(r"TAMIL\s*NADU\s*MERCANTILE", re.IGNORECASE),
        re.compile(r"TMB\s*Bank", re.IGNORECASE),
    ],
    "BANDHAN": [
        re.compile(r"BANDHAN\s*BANK", re.IGNORECASE),
    ],
    "EQUITAS": [
        re.compile(r"EQUITAS\s*SMALL\s*FINANCE", re.IGNORECASE),
    ],
    "UJJIVAN": [
        re.compile(r"UJJIVAN\s*SMALL\s*FINANCE", re.IGNORECASE),
    ],
    "RBL": [
        re.compile(r"RBL\s*BANK", re.IGNORECASE),
        re.compile(r"RATNAKAR\s*BANK", re.IGNORECASE),
    ],
    "INDIANBANK": [
        re.compile(r"INDIAN\s*BANK", re.IGNORECASE),
        re.compile(r"Date\s+Particulars", re.IGNORECASE),
        re.compile(r"Debit\s+Credit\s+Balance", re.IGNORECASE),
        re.compile(r"Value\s*Date", re.IGNORECASE),
    ],
    "GENERIC": [],
}

# ---------------------------------------------------------------------------
# Bank date format hints — which patterns to try first
# ---------------------------------------------------------------------------
BANK_DATE_FORMATS = {
    "HDFC":    [_DATE_DMY_SHORT, _DATE_DMY_SLASH],   # HDFC uses DD/MM/YY
    "ICICI":   [_DATE_DMY_SLASH],
    "SBI":     [_DATE_LONG, _DATE_DMY_SLASH],         # SBI uses DD Mon YYYY
    "AXIS":    [_DATE_DMY_DASH, _DATE_DMY_SLASH],     # Axis uses DD-MM-YYYY
    "KOTAK":   [_DATE_DMY_SLASH, _DATE_DMY_DASH],
    "BOB":     [_DATE_DMY_SLASH],
    "PNB":     [_DATE_DMY_SLASH],
    "YES":     [_DATE_DMY_SLASH, _DATE_DMY_DASH],
    "INDUSIND":[_DATE_DMY_SLASH, _DATE_DMY_DASH],
    "IOB":     [_DATE_DMY_MON_SHORT, _DATE_DMY_SLASH, _DATE_DMY_DASH],
    "CANARA":  [_DATE_DMY_SLASH, _DATE_DMY_DASH],
    "UNION":   [_DATE_DMY_SLASH, _DATE_DMY_DASH],
    "BOI":     [_DATE_DMY_SLASH, _DATE_DMY_DASH],
    "FEDERAL": [_DATE_DMY_SLASH, _DATE_DMY_DASH],
    "IDBI":    [_DATE_DMY_SLASH, _DATE_DMY_DASH],
    "KARNATAKA": [_DATE_DMY_SLASH, _DATE_DMY_DASH],
    "SIB":       [_DATE_DMY_SLASH, _DATE_DMY_DASH],
    "UCO":       [_DATE_DMY_SLASH, _DATE_DMY_DASH],
    "CENTRAL":   [_DATE_DMY_SLASH, _DATE_DMY_DASH],
    "DHAN":      [_DATE_DMY_SLASH, _DATE_DMY_DASH],
    "TNMB":      [_DATE_DMY_SLASH, _DATE_DMY_DASH],
    "CUB":       [_DATE_DMY_SLASH, _DATE_DMY_DASH],
    "RBL":       [_DATE_DMY_SLASH, _DATE_DMY_DASH],
    "BANDHAN":   [_DATE_DMY_SLASH, _DATE_DMY_DASH],
    "EQUITAS":   [_DATE_DMY_SLASH, _DATE_DMY_DASH],
    "UJJIVAN":   [_DATE_DMY_SLASH, _DATE_DMY_DASH],
    "INDIANBANK": [_DATE_DMY_SLASH, _DATE_DMY_DASH, _DATE_DMY_SHORT],
    "GENERIC": [_DATE_DMY_MON_SHORT, _DATE_DMY_SLASH, _DATE_DMY_DASH, _DATE_DMY_SHORT, _DATE_LONG],
}


# ---------------------------------------------------------------------------
# Step 1: Bank Detection — multi-signal confidence scoring
# ---------------------------------------------------------------------------
def _calculate_bank_score(bank_code, sig, header_text, footer_text):
    score = 0
    # Signal 1: Primary keyword match (strong — 10 pts each)
    for kw in sig["keywords"]:
        if kw.replace(" ", "") in header_text:
            score += 10
            break  # one match is enough

    # Signal 2: Confirmation keywords (5 pts each)
    for ck in sig.get("confirm", []):
        if ck.replace(" ", "") in header_text:
            score += 5

    # Signal 3: Column header fingerprints (3 pts each)
    for col_hint in BANK_COLUMN_FINGERPRINTS.get(bank_code, []):
        if col_hint.replace(" ", "") in header_text:
            score += 3

    # Signal 4: Footer/watermark phrases (4 pts each)
    for footer_hint in BANK_FOOTER_SIGNATURES.get(bank_code, []):
        if footer_hint.replace(" ", "") in footer_text.replace(" ", ""):
            score += 4
            
    return score


def detect_bank(pdf):
    """
    Auto-detect the bank from PDF text using multi-signal confidence scoring.

    Signals used (in order of strength):
      1. Bank name keywords in header (primary)
      2. Confirmation keywords (column headers unique to bank)
      3. Column header fingerprints (BANK_COLUMN_FINGERPRINTS)
      4. Footer/watermark phrases (BANK_FOOTER_SIGNATURES)

    Returns bank code string e.g. 'HDFC', 'ICICI', 'SBI', or 'GENERIC'.
    """
    try:
        # Extract text from first 2 pages (header detection)
        header_text_raw = ""
        for page in pdf.pages[:2]:
            text = page.extract_text() or ""
            header_text_raw += text + " "

        # Also read last page for footers
        footer_text_raw = ""
        if len(pdf.pages) >= 1:
            footer_text_raw = pdf.pages[-1].extract_text() or ""

        # Normalised versions for matching
        header_text = header_text_raw.upper().replace(" ", "")
        footer_text  = footer_text_raw.upper()

        scores = {}

        for bank_code, sig in BANK_SIGNATURES.items():
            score = _calculate_bank_score(bank_code, sig, header_text, footer_text)
            if score > 0:
                scores[bank_code] = score

        if scores:
            best = max(scores, key=lambda k: scores[k])
            print(f"[UniversalParser] Detection scores: { dict(sorted(scores.items(), key=lambda x: -x[1])[:5]) }")
            print(f"[UniversalParser] Best match: {best} (score={scores[best]})")
            return best

    except Exception as e:
        print(f"[UniversalParser] Bank detection error: {e}")

    return "GENERIC"


# ---------------------------------------------------------------------------
# Step 2: Parse using detected bank config
# ---------------------------------------------------------------------------
def parse_universal(pdf, bank_code=None):
    """
    Main entry point. Auto-detects bank if not provided, then parses.

    Args:
        pdf: opened pdfplumber PDF object
        bank_code: optional override (e.g. 'HDFC', 'ICICI')

    Returns:
        List of transaction dicts: {date, description, debit, credit, balance}
    """
    if not bank_code:
        bank_code = detect_bank(pdf)

    print(f"[UniversalParser] Bank detected: {bank_code}")

    # ── Step 1: Try table extraction first (works for IOB, most structured banks)
    table_txns = _parse_via_tables(pdf, bank_code)
    if table_txns:
        print(f"[UniversalParser] Table extraction: {len(table_txns)} rows")
        return table_txns

    # ── Step 2: Fall back to line-by-line text extraction
    skip_patterns = _COMMON_SKIPS + BANK_SKIP_PATTERNS.get(bank_code, [])
    date_formats = BANK_DATE_FORMATS.get(bank_code, BANK_DATE_FORMATS["GENERIC"])

    all_lines = []
    for page in pdf.pages:
        text = page.extract_text(layout=True)
        if text:
            all_lines.extend(text.split("\n"))

    raw_txns = _parse_lines(all_lines, skip_patterns, date_formats)

    if not raw_txns:
        print(f"[UniversalParser] No transactions found for bank={bank_code}, trying GENERIC")
        skip_patterns = _COMMON_SKIPS
        date_formats = BANK_DATE_FORMATS["GENERIC"]
        raw_txns = _parse_lines(all_lines, skip_patterns, date_formats)

    return _compute_amounts(raw_txns)


def _process_table(table, date_formats, results):
    # Find header row to identify columns
    header_idx, col_map = _find_table_header(table)
    if col_map is None:
        return

    # Parse data rows
    for row in table[header_idx + 1:]:
        if not row or all(not str(c).strip() for c in row if c is not None):
            continue

        txn = _parse_table_row(row, col_map, date_formats)
        if txn:
            results.append(txn)


def _parse_via_tables(pdf, bank_code):
    """
    Extract transactions using pdfplumber table extraction.
    Works well for IOB and other banks with clear table borders.
    Returns list of transaction dicts or [] if extraction fails.
    """
    import re as _re

    date_formats = BANK_DATE_FORMATS.get(bank_code, BANK_DATE_FORMATS["GENERIC"])
    results = []

    for page in pdf.pages:
        tables = page.extract_tables()
        if not tables:
            continue

        for table in tables:
            if table and len(table) >= 2:
                _process_table(table, date_formats, results)

    return _detect_and_normalize_direction(results)


_COL_ALIASES = {
    "date": ["date", "value date", "txn date", "transaction date"],
    "description": ["particulars", "narration", "description", "details", "remarks"],
    "debit": ["debit", "debit(rs)", "withdrawal", "dr", "debit amount", "debit amt"],
    "credit": ["credit", "credit(rs)", "deposit", "cr", "credit amount", "credit amt"],
    "balance": ["balance", "balance(rs)", "closing balance", "running balance"],
}

def _map_single_cell(cell, col_map, j):
    for std_col, aliases in _COL_ALIASES.items():
        if std_col in col_map:
            continue
        for a in aliases:
            if a in cell:
                col_map[std_col] = j
                break

def _map_row_cols(cells):
    col_map = {}
    for j, cell in enumerate(cells):
        _map_single_cell(cell, col_map, j)
    return col_map

def _find_table_header(table):
    """
    Find the header row in a table and map columns.
    Returns (header_row_index, col_map_dict) or (0, None) if not found.
    """
    for i, row in enumerate(table[:5]):  # Header usually in first 5 rows
        if not row:
            continue
        cells = [str(c).strip().lower().replace("\n", " ") if c else "" for c in row]
        col_map = _map_row_cols(cells)
        # Need at least date + description + one amount column
        if "date" in col_map and "description" in col_map and (
            "debit" in col_map or "credit" in col_map or "balance" in col_map
        ):
            return i, col_map

    return 0, None


def _get_cell_val(row, idx):
    if idx is None or idx >= len(row):
        return ""
    return str(row[idx]).strip() if row[idx] is not None else ""

def _parse_table_amt(val):
    v = str(val).replace(",", "").replace("-", "").replace("₹", "").strip()
    try:
        return abs(float(v)) if v else 0.0
    except ValueError:
        return 0.0

def _is_dash(val):
    return str(val).strip() in ("-", "", "None", "nil", "NIL")

def _extract_table_date(raw_date, date_formats):
    if not raw_date:
        return None
    for fmt in date_formats:
        m = fmt.search(raw_date)
        if m:
            return _normalize_date(m.group(1))
    return None

def _parse_table_row(row, col_map, date_formats):
    """Parse a single table row into a transaction dict."""
    # Date
    raw_date = _get_cell_val(row, col_map.get("date"))
    date_str = _extract_table_date(raw_date, date_formats)
    if not date_str:
        return None

    # Description
    desc = _get_cell_val(row, col_map.get("description"))
    if not desc:
        return None

    # Amounts
    debit_raw = _get_cell_val(row, col_map.get("debit"))
    credit_raw = _get_cell_val(row, col_map.get("credit"))
    balance_raw = _get_cell_val(row, col_map.get("balance"))

    debit = 0.0 if _is_dash(debit_raw) else _parse_table_amt(debit_raw)
    credit = 0.0 if _is_dash(credit_raw) else _parse_table_amt(credit_raw)
    balance = _parse_table_amt(balance_raw)

    # Skip rows with no amounts at all
    if debit < 0.001 and credit < 0.001 and balance < 0.001:
        return None

    # Narration-based debit/credit correction.
    # IOB (and some other banks) embed /CR/ or /DR/ in the narration text.
    # pdfplumber sometimes misaligns columns when narration text wraps to the
    # next line, causing the amount to land in the wrong debit/credit column.
    # Use the explicit /CR/ or /DR/ marker to correct any such mismatch.
    desc_upper = desc.upper()
    if "/CR/" in desc_upper and debit > 0.0 and credit < 0.001:
        # Narration says CREDIT but column value landed in debit — swap
        credit, debit = debit, 0.0
    elif "/DR/" in desc_upper and credit > 0.0 and debit < 0.001:
        # Narration says DEBIT but column value landed in credit — swap
        debit, credit = credit, 0.0

    return {
        "date": date_str,
        "description": desc.replace("\n", " ").strip(),
        "debit": debit,
        "credit": credit,
        "balance": balance,
    }


# ---------------------------------------------------------------------------
# Step 3: Line-by-line parsing
# ---------------------------------------------------------------------------
def _parse_lines(lines, skip_patterns, date_formats):
    """Parse PDF text lines into raw transaction list."""
    transactions = []
    current_txn = None

    for line in lines:
        if _should_skip(line, skip_patterns):
            continue

        date_str, date_end = _extract_date(line, date_formats)

        if date_str:
            if current_txn:
                transactions.append(current_txn)
            current_txn = _build_txn(date_str, line, date_end, date_formats)
        elif current_txn and line.strip():
            # Continuation line — append to narration if it's not pure numbers
            stripped = line.strip()
            if not _is_numeric_only(stripped):
                current_txn["narration"] += " " + stripped

    if current_txn:
        transactions.append(current_txn)

    return transactions


def _should_skip(line, skip_patterns):
    """Check if a line should be skipped."""
    stripped = line.strip()
    if not stripped:
        return True
    for pat in skip_patterns:
        if pat.search(stripped):
            return True
    return False


def _extract_date(line, date_formats):
    """
    Try each date format in order and return (date_str, match_end).
    Returns (None, 0) if no date found.
    """
    for fmt in date_formats:
        m = fmt.search(line)
        if m:
            raw = m.group(1)
            normalized = _normalize_date(raw)
            return normalized, m.end()
    return None, 0


def _normalize_date(date_str):
    """Normalize any date format to DD/MM/YYYY."""
    # DD-Mon-YY → DD/MM/20YY (e.g. 24-Feb-26 → 24/02/2026)
    mon_short = re.match(r"(\d{1,2})-(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-(\d{2})$", date_str, re.IGNORECASE)
    if mon_short:
        day, mon, yr = mon_short.groups()
        mon_num = _MONTH_MAP.get(mon.lower(), "01")
        return f"{day.zfill(2)}/{mon_num}/20{yr}"

    # Already DD/MM/YYYY or DD/MM/YY
    if re.match(r"\d{2}/\d{2}/\d{4}", date_str):
        return date_str
    # DD/MM/YY → DD/MM/20YY
    if re.match(r"\d{2}/\d{2}/\d{2}$", date_str):
        parts = date_str.split("/")
        return f"{parts[0]}/{parts[1]}/20{parts[2]}"
    # DD-MM-YYYY → DD/MM/YYYY
    if re.match(r"\d{2}-\d{2}-\d{4}", date_str):
        return date_str.replace("-", "/")
    # DD Mon YYYY → DD/MM/YYYY
    m = _DATE_LONG.match(date_str)
    if m:
        parts = date_str.strip().split()
        if len(parts) == 3:
            day, mon, year = parts
            mon_num = _MONTH_MAP.get(mon.lower(), "01")
            return f"{day.zfill(2)}/{mon_num}/{year}"
    return date_str


def _build_txn(date_str, line, date_end, date_formats):
    """Build a raw transaction dict from a date-anchored line."""
    rest = line[date_end:].strip()

    # Some banks have a second date (value date) — skip it
    second_date, second_end = _extract_date(rest, date_formats)
    if second_date:
        rest = rest[second_end:].strip()

    # Find all amounts in the rest of the line
    amounts = _AMOUNT_RE.findall(rest)

    # Balance is the rightmost amount
    balance = _parse_num(amounts[-1]) if amounts else 0.0

    # Narration is text before the first amount
    narration = rest
    if amounts:
        first_pos = rest.find(amounts[0])
        if first_pos > 0:
            narration = rest[:first_pos].strip()

    # Detect explicit debit/credit columns using right-to-left scan
    debit_explicit, credit_explicit, balance = _extract_explicit_amounts(rest, balance)

    # Clean up narration
    narration = _clean_narration(narration)

    return {
        "date": date_str,
        "narration": narration,
        "amounts": amounts,
        "balance": balance,
        "debit_explicit": debit_explicit,
        "credit_explicit": credit_explicit,
    }


def _collect_trailing_tokens(rest):
    trailing = []
    for tok in reversed(rest.split()):
        cleaned = tok.replace(",", "")
        if tok == "-":
            trailing.append(tok)
            if len(trailing) >= 3:
                break
            continue
            
        try:
            float(cleaned)
            trailing.append(tok)
        except ValueError:
            if trailing:
                break
        if len(trailing) >= 3:
            break
    return trailing

def _extract_explicit_amounts(rest, initial_balance):
    """Scan tokens from RIGHT to extract explicit debit, credit and balance."""
    debit_explicit, credit_explicit = None, None
    balance = initial_balance
    trailing = _collect_trailing_tokens(rest)

    if len(trailing) >= 3:
        bal_tok, c_tok, d_tok = trailing[0], trailing[1], trailing[2]
        balance = _parse_num(bal_tok)
        debit_explicit = _parse_num(d_tok) if d_tok != "-" else 0.0
        credit_explicit = _parse_num(c_tok) if c_tok != "-" else 0.0
    elif len(trailing) == 2:
        narration_check = rest[:rest.rfind(trailing[1])].strip()
        if narration_check.endswith("-"):
            debit_explicit = 0.0
            credit_explicit = _parse_num(trailing[1])
            balance = _parse_num(trailing[0])

    return debit_explicit, credit_explicit, balance


def _clean_narration(narration):
    """Remove common noise from narration text."""
    # Remove leading serial numbers (e.g., "1 ", "23 ")
    narration = re.sub(r"^\d+\s+", "", narration)
    # Remove trailing reference/cheque numbers (10+ digits)
    narration = re.sub(r"\s+\d{10,}\s*$", "", narration)
    # Remove short alphanumeric ref codes like S24397224, S93720728
    narration = re.sub(r"\s+[A-Z]\d{6,}\b", "", narration)
    # Remove "Transfer", "Withdrawal", "Deposit" transaction type words
    narration = re.sub(r"\s+(Transfer|Withdrawal|Deposit|NEFT|UPI|IMPS)\s*$", "", narration, flags=re.IGNORECASE)
    # Remove trailing " - " or " -" (IOB empty column marker)
    narration = re.sub(r"\s+-\s*$", "", narration)
    # Remove IFSC codes
    narration = re.sub(r"\b[A-Z]{4}0[A-Z0-9]{6}\b", "", narration)
    return narration.strip()


def _is_numeric_only(text):
    """Check if a string is purely numeric (no real narration content)."""
    cleaned = text.replace(",", "").replace(".", "").replace(" ", "").replace("-", "")
    return cleaned.isdigit()


# ---------------------------------------------------------------------------
# Step 4: Compute debit/credit from balance delta
# ---------------------------------------------------------------------------
def _calculate_txn_amounts(txn, prev_bal, i, use_explicit):
    """Determine debit and credit for a single transaction."""
    curr_bal = txn["balance"]
    
    # Handle explicit amounts
    if use_explicit and (txn.get("debit_explicit") is not None or txn.get("credit_explicit") is not None):
        return txn.get("debit_explicit") or 0.0, txn.get("credit_explicit") or 0.0

    # Handle derived amounts based on balance delta
    if i > 0 and prev_bal > 0 and curr_bal > 0:
        delta = curr_bal - prev_bal
        amount = abs(delta)
        if amount < 0.01:
            return 0.0, 0.0
        return (amount, 0.0) if delta < 0 else (0.0, amount)

    # Fallback to last detected amount
    amounts = txn.get("amounts", [])
    if len(amounts) >= 2:
        return _parse_num(amounts[-2]), 0.0
    return 0.0, 0.0


def _compute_amounts(raw_txns):
    """
    Derive debit/credit amounts.
    First normalizes statement direction so rows are always oldest-first.
    Uses explicit column values if detected (IOB style), otherwise balance delta.
    """
    if not raw_txns:
        return []

    # Normalize to chronological order (oldest first) before any processing.
    # This ensures stmt_order assigned in parser.py is always oldest=0, newest=last.
    raw_txns = _detect_and_normalize_direction(raw_txns)

    explicit_count = sum(
        1 for t in raw_txns
        if t.get("debit_explicit") is not None or t.get("credit_explicit") is not None
    )
    use_explicit = explicit_count > len(raw_txns) * 0.5

    results = []
    
    for i, txn in enumerate(raw_txns):
        prev_bal = raw_txns[i - 1]["balance"] if i > 0 else 0.0
        debit, credit = _calculate_txn_amounts(txn, prev_bal, i, use_explicit)
        
        results.append({
            "date": txn["date"],
            "description": txn["narration"],
            "debit": round(debit, 2),
            "credit": round(credit, 2),
            "balance": round(txn["balance"], 2),
        })

    return results


# ---------------------------------------------------------------------------
# Statement direction detection + normalization
# ---------------------------------------------------------------------------
def _detect_and_normalize_direction(txns):
    """
    Detect whether a statement is newest-first (descending) or oldest-first
    (ascending) by checking the date order of the parsed transactions.

    Uses the actual DATE values — not balance math — so it works even if
    balance values are wrong or missing. Date order is always reliable since
    it comes directly from the PDF cells.

    Logic:
      - Count how many consecutive date pairs are ascending (d[i] <= d[i+1])
        vs descending (d[i] >= d[i+1]).
      - If majority are descending → newest-first → REVERSE so oldest comes first.
      - If majority are ascending (or equal) → already chronological → no change.

    After normalization, row index 0 = oldest transaction, last row = closing balance.
    This is the ground truth that stmt_order is built on in parser.py.
    """
    if len(txns) < 2:
        return txns

    from datetime import datetime

    def _parse_date(d):
        for fmt in ("%d/%m/%Y", "%d/%m/%y", "%d-%m-%Y"):
            try:
                return datetime.strptime(d, fmt)
            except ValueError:
                continue
        return None

    dates = [_parse_date(t.get("date", "")) for t in txns]
    valid_pairs = [(dates[i], dates[i+1]) for i in range(len(dates)-1)
                   if dates[i] and dates[i+1]]

    if not valid_pairs:
        return txns

    asc_count  = sum(1 for a, b in valid_pairs if a <= b)
    desc_count = sum(1 for a, b in valid_pairs if a >= b)

    if desc_count > asc_count:
        print(f"[UniversalParser] Statement is newest-first — reversing to chronological order")
        return list(reversed(txns))

    return txns  # already chronological


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------
def _parse_num(val):
    """Parse a numeric string to float."""
    try:
        return float(_CURRENCY_RE.sub("", str(val)))
    except (ValueError, TypeError):
        return 0.0


# ---------------------------------------------------------------------------
# How to add a new bank (example)
# ---------------------------------------------------------------------------
# 1. Add to BANK_SIGNATURES:
#    "CANARA": {
#        "keywords": ["CANARA BANK"],
#        "confirm": [],
#    }
#
# 2. Optionally add specific skip patterns to BANK_SKIP_PATTERNS:
#    "CANARA": [
#        re.compile(r"Canara\s*Bank\s*Ltd", re.IGNORECASE),
#    ]
#
# 3. Optionally add date format hint to BANK_DATE_FORMATS:
#    "CANARA": [_DATE_DMY_SLASH],
#
# That's it — no other code changes needed!
