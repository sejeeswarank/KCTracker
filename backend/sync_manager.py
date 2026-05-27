"""
sync_manager.py
Google Drive API sync manager for KC Tracker.
Handles OAuth authentication and bidirectional file sync with Google Drive.
"""

import os
import io
import json
from datetime import datetime, timezone

from config import CREDENTIALS_FILE, TOKEN_FILE, DRIVE_BASE_PATH, USERS_DB_FOLDER
import os as _os
_TOKEN_DIR = _os.path.dirname(TOKEN_FILE)  # same folder as token.json

# Google API imports
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

# Scope: manage files created by this app
SCOPES = ["https://www.googleapis.com/auth/drive.file"]

# Drive MIME types
FOLDER_MIME = "application/vnd.google-apps.folder"

# Database file name constant
_AUTH_DB_NAME = "auth.db"


# ---------------------------------------------------------------------------
# Step 1: Authentication
# ---------------------------------------------------------------------------
def _user_token_file(username):
    """Return per-user token path, e.g. token_sejee.json"""
    base = _os.path.splitext(TOKEN_FILE)[0]  # strip .json
    return f"{base}_{username}.json"


def authenticate_drive(username=None):
    """
    Authenticate with Google Drive using OAuth 2.0.
    Token is stored per-user so each account links its own Google account.
    New users (no token file) always get the OAuth browser prompt.
    """
    token_file = _user_token_file(username) if username else TOKEN_FILE
    creds = None

    # Load saved token for this specific user
    if os.path.exists(token_file):
        try:
            creds = Credentials.from_authorized_user_file(token_file, SCOPES)
        except Exception:
            creds = None

    # Refresh or create new credentials
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not os.path.exists(CREDENTIALS_FILE):
                raise FileNotFoundError(
                    f"OAuth credentials file not found: {CREDENTIALS_FILE}. "
                    "Please place your credentials.json in the project root."
                )
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            # prompt=select_account forces Google to ask which account every new login
            creds = flow.run_local_server(port=0, prompt="select_account")

        # Save token per user
        with open(token_file, "w", encoding="utf-8") as token:
            token.write(creds.to_json())

    service = build("drive", "v3", credentials=creds)
    return service


# ---------------------------------------------------------------------------
# Step 2: Folder management
# ---------------------------------------------------------------------------
def get_or_create_folder(service, folder_name, parent_id=None):
    """
    Find or create a folder in Google Drive.
    Returns the folder ID.
    """
    query = f"name = '{folder_name}' and mimeType = '{FOLDER_MIME}' and trashed = false"
    if parent_id:
        query += f" and '{parent_id}' in parents"

    results = service.files().list(
        q=query, spaces="drive", fields="files(id,name)", pageSize=1
    ).execute()
    files = results.get("files", [])

    if files:
        return files[0]["id"]

    metadata = {
        "name": folder_name,
        "mimeType": FOLDER_MIME,
    }
    if parent_id:
        metadata["parents"] = [parent_id]

    folder = service.files().create(body=metadata, fields="id").execute()
    return folder["id"]


# ---------------------------------------------------------------------------
# Step 3: Upload file
# ---------------------------------------------------------------------------
def upload_file(service, local_path, drive_name, folder_id):
    """
    Upload a local file to Google Drive.
    If the file already exists in the folder, update it.
    Returns the file ID.
    """
    if not os.path.exists(local_path):
        return None

    query = f"name = '{drive_name}' and '{folder_id}' in parents and trashed = false"
    results = service.files().list(
        q=query, spaces="drive", fields="files(id, name)", pageSize=1
    ).execute()
    existing = results.get("files", [])

    media = MediaFileUpload(local_path, resumable=True)

    if existing:
        file_id = existing[0]["id"]
        updated = service.files().update(
            fileId=file_id, media_body=media
        ).execute()
        return updated["id"]

    metadata = {
        "name": drive_name,
        "parents": [folder_id],
    }
    created = service.files().create(
        body=metadata, media_body=media, fields="id"
    ).execute()
    return created["id"]


# ---------------------------------------------------------------------------
# Step 4: Download file
# ---------------------------------------------------------------------------
def download_file(service, drive_name, local_path, folder_id):
    """
    Download a file from Google Drive to a local path.
    Overwrites the local file if it exists.
    Returns True on success, False if file not found on Drive.
    """
    query = f"name = '{drive_name}' and '{folder_id}' in parents and trashed = false"
    results = service.files().list(
        q=query, spaces="drive", fields="files(id, name)", pageSize=1
    ).execute()
    files = results.get("files", [])

    if not files:
        return False

    file_id = files[0]["id"]

    request_obj = service.files().get_media(fileId=file_id)
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request_obj)

    done = False
    while not done:
        _, done = downloader.next_chunk()

    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    with open(local_path, "wb") as f:
        f.write(buffer.getvalue())

    return True


# ---------------------------------------------------------------------------
# Step 5 & 6: High-level sync helpers
# ---------------------------------------------------------------------------
def _get_folder_ids(service):
    """
    Get or create the LedgerApp and users folder IDs.
    Returns (ledgerapp_folder_id, users_folder_id).
    """
    root_id = get_or_create_folder(service, "LedgerApp")
    users_id = get_or_create_folder(service, "users", parent_id=root_id)
    return root_id, users_id


def sync_upload(local_path, drive_name, subfolder=None, username=None):
    """
    High-level: authenticate and upload a single file to Drive.
    subfolder: "users" to upload into LedgerApp/users/, None for LedgerApp/
    """
    try:
        service = authenticate_drive(username)
        root_id, users_id = _get_folder_ids(service)
        folder_id = users_id if subfolder == "users" else root_id
        upload_file(service, local_path, drive_name, folder_id)
        return {"success": True, "message": f"Uploaded {drive_name} to Drive."}
    except Exception as e:
        return {"success": False, "message": f"Upload failed: {str(e)}"}


def sync_download(drive_name, local_path, subfolder=None, username=None):
    """
    High-level: authenticate and download a single file from Drive.
    subfolder: "users" to download from LedgerApp/users/, None for LedgerApp/
    """
    try:
        service = authenticate_drive(username)
        root_id, users_id = _get_folder_ids(service)
        folder_id = users_id if subfolder == "users" else root_id
        found = download_file(service, drive_name, local_path, folder_id)
        if found:
            return {"success": True, "message": f"Downloaded {drive_name} from Drive."}
        return {"success": True, "message": f"{drive_name} not found on Drive (new file)."}
    except Exception as e:
        return {"success": False, "message": f"Download failed: {str(e)}"}


# ---------------------------------------------------------------------------
# Step 7: Full sync
# ---------------------------------------------------------------------------
def sync_all(username):
    """
    Full bidirectional sync for a user:
    1. Upload auth.db to Drive
    2. Upload username.db to Drive
    3. Download auth.db from Drive
    4. Download username.db from Drive
    Returns a status dict.
    """
    from config import AUTH_DB_PATH

    if not is_connected(username):
        return {
            "success": False,
            "message": "Google Drive is not connected yet. Please request access and connect Drive first.",
            "details": [],
        }

    auth_db = AUTH_DB_PATH
    user_db = os.path.join(USERS_DB_FOLDER, f"{username}.db")
    user_db_name = f"{username}.db"

    results = []

    try:
        service = authenticate_drive(username)
        root_id, users_id = _get_folder_ids(service)

        if os.path.exists(auth_db):
            upload_file(service, auth_db, _AUTH_DB_NAME, root_id)
            results.append(f"{_AUTH_DB_NAME} uploaded")

        if os.path.exists(user_db):
            upload_file(service, user_db, user_db_name, users_id)
            results.append(f"{user_db_name} uploaded")

        download_file(service, _AUTH_DB_NAME, auth_db, root_id)
        results.append(f"{_AUTH_DB_NAME} downloaded")

        download_file(service, user_db_name, user_db, users_id)
        results.append(f"{user_db_name} downloaded")

        return {
            "success": True,
            "message": "Sync completed successfully.",
            "details": results,
        }
    except Exception as e:
        return {
            "success": False,
            "message": f"Sync failed: {str(e)}",
            "details": results,
        }


def sync_download_on_login(username):
    """
    Download the latest databases from Drive when user logs in.
    Called automatically after successful authentication.
    """
    from config import AUTH_DB_PATH

    auth_db = AUTH_DB_PATH
    user_db = os.path.join(USERS_DB_FOLDER, f"{username}.db")

    try:
        service = authenticate_drive(username)
        root_id, users_id = _get_folder_ids(service)

        download_file(service, _AUTH_DB_NAME, auth_db, root_id)
        download_file(service, f"{username}.db", user_db, users_id)

        return {"success": True, "message": "Data synced from Drive."}
    except Exception as e:
        return {"success": False, "message": f"Drive sync skipped: {str(e)}"}


def sync_upload_after_change(username):
    """
    Upload databases to Drive after any data change.
    Called after insert/delete/update operations.
    """
    from config import AUTH_DB_PATH

    if not is_connected(username):
        return {"success": True, "message": "Drive not connected. Changes saved locally only."}

    auth_db = AUTH_DB_PATH
    user_db = os.path.join(USERS_DB_FOLDER, f"{username}.db")

    try:
        service = authenticate_drive(username)
        root_id, users_id = _get_folder_ids(service)

        if os.path.exists(auth_db):
            upload_file(service, auth_db, _AUTH_DB_NAME, root_id)

        if os.path.exists(user_db):
            upload_file(service, user_db, f"{username}.db", users_id)

        return {"success": True, "message": "Changes synced to Drive."}
    except Exception as e:
        return {"success": False, "message": f"Drive upload skipped: {str(e)}"}


# ---------------------------------------------------------------------------
# Step 8: Connection check
# ---------------------------------------------------------------------------
def is_connected(username):
    """
    Check if the user has already linked a Google Drive account.
    Returns True if a valid token file exists for this user.
    """
    return bool(username) and os.path.exists(_user_token_file(username))
