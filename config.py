import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ---------------------------------------------------------------------------
# Project root
# ---------------------------------------------------------------------------
_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

# ---------------------------------------------------------------------------
# Google Drive API credentials (OAuth)
# ---------------------------------------------------------------------------
CREDENTIALS_FILE = os.path.join(_PROJECT_ROOT, "credentials.json")
TOKEN_FILE = os.path.join(_PROJECT_ROOT, "token.json")

# ---------------------------------------------------------------------------
# Local data directory (working copies — synced to Drive via API)
# ---------------------------------------------------------------------------
DRIVE_BASE_PATH = os.path.join(_PROJECT_ROOT, "data", "LedgerApp/")

AUTH_DB_PATH = os.path.join(_PROJECT_ROOT, "data", "auth.db")
USERS_DB_FOLDER = os.path.join(_PROJECT_ROOT, "data", "users/")
EXPORT_FOLDER = os.path.join(_PROJECT_ROOT, "data", "exports/")
TEMP_FOLDER = os.path.join(_PROJECT_ROOT, "data", "temp/")

# ---------------------------------------------------------------------------
# Flask secret key for session management
# REQUIRED: Must be set in .env file. No hardcoded fallback.
# ---------------------------------------------------------------------------
_secret = os.environ.get("SECRET_KEY")
if not _secret:
    raise RuntimeError(
        "SECRET_KEY is not set. Add it to your .env file.\n"
        "Generate one with: python -c \"import secrets; print(secrets.token_hex(32))\""
    )
SECRET_KEY: str = _secret

# ---------------------------------------------------------------------------
# Fernet encryption key for bank statement passwords
# Stored locally — NEVER synced to Drive.
# REQUIRED: Must be set in .env file. No hardcoded fallback.
# ---------------------------------------------------------------------------
_enc_key = os.environ.get("ENCRYPTION_KEY", "").strip()
if not _enc_key:
    raise RuntimeError(
        "ENCRYPTION_KEY is not set. Add it to your .env file.\n"
        "Generate one with: python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\""
    )

# Strip Python bytes-literal wrapping if accidentally saved as b'...'
if (_enc_key.startswith("b'") and _enc_key.endswith("'")) or \
   (_enc_key.startswith('b"') and _enc_key.endswith('"')):
    _enc_key = _enc_key[2:-1]

ENCRYPTION_KEY = _enc_key.encode('utf-8')

# Allowed upload extensions
ALLOWED_EXTENSIONS = {"csv", "xlsx", "xls", "pdf"}


def ensure_directories():
    """Create all required directories if they don't exist."""
    directories = [USERS_DB_FOLDER, EXPORT_FOLDER, TEMP_FOLDER]
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
    print("[KC Tracker] Data directory initialized.")
