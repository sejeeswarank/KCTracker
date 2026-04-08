import json
import os
import shutil
import time
from collections import defaultdict, deque
from datetime import date, datetime, timedelta
from typing import Any
from urllib.parse import urlencode

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.background import BackgroundTask
from starlette.middleware.sessions import SessionMiddleware

from config import AUTH_DB_PATH, SECRET_KEY, TEMP_FOLDER, ensure_directories
from backend.auth import initialize_auth_db, login_user, register_user
from backend.database import (
    add_bank_credential,
    create_user_ledger,
    delete_bank_credential,
    delete_transaction,
    get_all_bank_credentials,
    get_all_dates_summary,
    get_bank_balances_over_time,
    get_bank_credential,
    get_recent_transactions,
    get_summary_by_date,
    get_transactions_by_range,
    insert_transactions_bulk,
    rebuild_daily_summary,
    record_statement_source,
    set_merchant_alias,
    update_transaction,
)
from backend.exporter import export_day_ledger, export_range_ledger
from backend.extractor import apply_merchant_aliases
from backend.ledger import generate_ledger
from backend.notifier import is_approved, notify_drive_request, poll_approvals
from backend.parser import allowed_file, group_transactions_for_ledger, parse_statement
from backend.security import decrypt_password as decrypt_bank_pw
from backend.security import encrypt_password as encrypt_bank_pw
from backend.sync_manager import is_connected, sync_all, sync_upload_after_change


_TPL_REGISTER = "register.html"
_TPL_UPLOAD = "upload.html"
_TPL_STATEMENT = "statement.html"
_TPL_SETTINGS = "statement_passwords.html"

app = FastAPI(title="KC Tracker")
app.add_middleware(SessionMiddleware, secret_key=SECRET_KEY, same_site="lax")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

_rate_limit_buckets: dict[str, deque[float]] = defaultdict(deque)


def _startup_init() -> None:
    ensure_directories()
    try:
        from backend.sync_manager import sync_download

        sync_download("auth.db", AUTH_DB_PATH, subfolder=None)
        print("[KC Tracker] auth.db synced from Drive.")
    except Exception as exc:
        print(f"[KC Tracker] Drive sync skipped: {exc}")

    initialize_auth_db()


_startup_init()


def flash(request: Request, message: str, category: str = "info") -> None:
    flashes = list(request.session.get("_flashes", []))
    flashes.append({"category": category, "message": message})
    request.session["_flashes"] = flashes


def get_flashed_messages(request: Request, with_categories: bool = False) -> list[Any]:
    flashes = request.session.pop("_flashes", [])
    if with_categories:
        return [(item.get("category", "info"), item.get("message", "")) for item in flashes]
    return [item.get("message", "") for item in flashes]


def _route_url(request: Request, endpoint: str, **params: Any) -> str:
    if endpoint == "static":
        filename = str(params.pop("filename", "")).lstrip("/")
        return str(request.url_for("static", path=filename))

    path_params: dict[str, str] = {}
    route = next((route for route in request.app.router.routes if getattr(route, "name", None) == endpoint), None)
    if route is None:
        raise KeyError(f"Unknown route: {endpoint}")

    route_param_names = set(getattr(route, "param_convertors", {}).keys())
    query_params: dict[str, Any] = {}
    for key, value in params.items():
        if key in route_param_names:
            path_params[key] = str(value)
        else:
            query_params[key] = value

    url = str(request.url_for(endpoint, **path_params))
    if query_params:
        url += "?" + urlencode(query_params)
    return url


def render_view(request: Request, template_name: str, context: dict[str, Any] | None = None, status_code: int = 200):
    context = dict(context or {})
    username = request.session.get("username")
    drive_connected = False
    drive_requested = bool(request.session.get("drive_requested", False))
    drive_approved = False
    if username:
        poll_approvals()
        drive_connected = is_connected(username)
        drive_approved = is_approved(username)

    context.update(
        {
            "request": request,
            "drive_connected": drive_connected,
            "drive_requested": drive_requested,
            "drive_approved": drive_approved,
            "get_flashed_messages": lambda with_categories=False: get_flashed_messages(request, with_categories),
            "url_for": lambda endpoint, **params: _route_url(request, endpoint, **params),
        }
    )
    return templates.TemplateResponse(request, template_name, context, status_code=status_code)


def redirect_to(request: Request, endpoint: str, status_code: int = 303, **params: Any) -> RedirectResponse:
    return RedirectResponse(url=_route_url(request, endpoint, **params), status_code=status_code)


def _client_key(request: Request) -> str:
    return request.client.host if request.client else "unknown"


def _is_rate_limited(bucket: str, limit: int, window_seconds: int) -> bool:
    now = time.monotonic()
    q = _rate_limit_buckets[bucket]
    while q and q[0] <= now - window_seconds:
        q.popleft()
    if len(q) >= limit:
        return True
    q.append(now)
    return False


def _enforce_login(request: Request) -> str | None:
    username = request.session.get("username")
    if not username:
        flash(request, "Please log in first.", "warning")
        return None

    if not request.session.get("schema_checked"):
        try:
            create_user_ledger(username)
            request.session["schema_checked"] = True
        except Exception as exc:
            print(f"Error checking schema: {exc}")

    return username


def _format_display_date(iso_date: str) -> str:
    try:
        if iso_date and len(iso_date) >= 10 and iso_date[4] == "-":
            parts = iso_date.split("-")
            return f"{parts[2]}-{parts[1]}-{parts[0]}"
    except (ValueError, IndexError):
        pass
    return iso_date


def _validate_registration(username: str, password: str, confirm: str) -> str | None:
    if not username or not password:
        return "Please fill in all fields."
    if password != confirm:
        return "Passwords do not match."
    if len(password) < 4:
        return "Password must be at least 4 characters."
    return None


def _render_upload(request: Request, username: str, bank_names: list[str]):
    return render_view(request, _TPL_UPLOAD, {"username": username, "bank_names": bank_names})


def _get_stmt_password(username: str, bank_name: str) -> tuple[str | None, str | None]:
    if not bank_name:
        return None, None
    encrypted = get_bank_credential(username, bank_name)
    if not encrypted:
        return None, f"No password found for '{bank_name}'. Please add it in Settings."
    try:
        return decrypt_bank_pw(encrypted), None
    except Exception:
        return None, "Failed to decrypt bank password."


def _cleanup_preview_file(preview_path: str) -> None:
    if preview_path and os.path.exists(preview_path):
        try:
            os.remove(preview_path)
        except Exception:
            pass


def _send_and_cleanup(filepath: str) -> FileResponse:
    return FileResponse(
        filepath,
        filename=os.path.basename(filepath),
        background=BackgroundTask(lambda: os.path.exists(filepath) and os.remove(filepath)),
    )


def _get_latest_balance_for_statement(username: str, all_dates: list[dict[str, Any]]) -> tuple[float, str]:
    latest_balance = 0.0
    latest_date_str = ""
    if all_dates:
        latest = all_dates[-1]
        latest_date_str = latest.get("date", "")
        try:
            day_summary = get_summary_by_date(username, latest_date_str)
            latest_balance = day_summary.get("balance", 0.0)
        except Exception:
            pass
    return latest_balance, latest_date_str


def _resolve_period_dates(period: str | None) -> tuple[str | None, str | None]:
    if not period:
        return None, None

    if period.startswith("month_"):
        import calendar as _cal

        ym = period[6:]
        try:
            year, month = int(ym[:4]), int(ym[5:7])
            last_day = _cal.monthrange(year, month)[1]
            return date(year, month, 1).isoformat(), date(year, month, last_day).isoformat()
        except (ValueError, IndexError):
            return None, None

    today = date.today()
    first_this = today.replace(day=1)
    last_prev = first_this - timedelta(days=1)
    fy_start_year = today.year if today.month >= 4 else today.year - 1

    periods = {
        "this_month": (first_this.isoformat(), today.isoformat()),
        "last_month": (last_prev.replace(day=1).isoformat(), last_prev.isoformat()),
        "last_3_months": ((today - timedelta(days=90)).isoformat(), today.isoformat()),
        "last_6_months": ((today - timedelta(days=180)).isoformat(), today.isoformat()),
        "this_year": (today.replace(month=1, day=1).isoformat(), today.isoformat()),
        "fy_current": (date(fy_start_year, 4, 1).isoformat(), today.isoformat()),
        "fy_previous": (date(fy_start_year - 1, 4, 1).isoformat(), date(fy_start_year, 3, 31).isoformat()),
        "recent_30": ((today - timedelta(days=30)).isoformat(), today.isoformat()),
    }
    return periods.get(period, (None, None))


def _build_month_options(all_dates: list[dict[str, Any]]) -> list[dict[str, str]]:
    months = sorted({item["date"][:7] for item in all_dates}, reverse=True)
    result = []
    for ym in months:
        try:
            dt = datetime.strptime(ym, "%Y-%m")
            result.append({"value": f"month_{ym}", "label": dt.strftime("%b %Y")})
        except Exception:
            pass
    return result


async def _handle_statement_post(
    request: Request,
    username: str,
    latest_balance: float,
    latest_date_str: str,
    month_options: list[dict[str, str]],
):
    form = await request.form()
    period = str(form.get("period", ""))
    start_date = str(form.get("start_date", "")).strip()
    end_date = str(form.get("end_date", "")).strip()

    if period and period != "custom":
        start_date, end_date = _resolve_period_dates(period)

    base_context = {
        "username": username,
        "latest_balance": latest_balance,
        "latest_date": latest_date_str,
        "month_options": month_options,
    }
    if not start_date or not end_date:
        flash(request, "Please select a valid period or date range.", "danger")
        return render_view(request, _TPL_STATEMENT, base_context)

    transactions = get_transactions_by_range(username, start_date, end_date)
    if not transactions:
        flash(request, "No transactions found for the selected range.", "warning")
        return render_view(request, _TPL_STATEMENT, base_context)

    grouped = group_transactions_for_ledger(transactions)
    base_context.update(
        {
            "grouped": grouped,
            "start_date": start_date,
            "end_date": end_date,
            "period": period,
        }
    )
    return render_view(request, _TPL_STATEMENT, base_context)


@app.get("/", name="index")
async def index(request: Request):
    if request.session.get("username"):
        return redirect_to(request, "dashboard")
    return redirect_to(request, "login")


@app.api_route("/login", methods=["GET", "POST"], name="login")
async def login(request: Request):
    if request.method == "POST":
        bucket_base = f"login:{_client_key(request)}"
        if _is_rate_limited(f"{bucket_base}:minute", 10, 60) or _is_rate_limited(f"{bucket_base}:hour", 50, 3600):
            flash(request, "Too many login attempts. Please try again later.", "danger")
            return render_view(request, "login.html")

        form = await request.form()
        username = str(form.get("username", "")).strip()
        password = str(form.get("password", ""))

        if not username or not password:
            flash(request, "Please fill in all fields.", "danger")
            return render_view(request, "login.html")

        success, result = login_user(username, password)
        if success:
            request.session.clear()
            request.session["username"] = username
            create_user_ledger(username)
            flash(request, "Login successful!", "success")
            return redirect_to(request, "dashboard")

        flash(request, str(result), "danger")

    return render_view(request, "login.html")


@app.api_route("/register", methods=["GET", "POST"], name="register")
async def register(request: Request):
    if request.method == "POST":
        if _is_rate_limited(f"register:{_client_key(request)}:hour", 5, 3600):
            flash(request, "Too many registration attempts. Please try again later.", "danger")
            return render_view(request, _TPL_REGISTER)

        form = await request.form()
        username = str(form.get("username", "")).strip()
        password = str(form.get("password", ""))
        confirm = str(form.get("confirm_password", ""))

        error = _validate_registration(username, password, confirm)
        if error:
            flash(request, error, "danger")
            return render_view(request, _TPL_REGISTER)

        success, message = register_user(username, password)
        if success:
            create_user_ledger(username)
            flash(request, "Registration successful! Please log in.", "success")
            return redirect_to(request, "login")

        flash(request, message, "danger")

    return render_view(request, _TPL_REGISTER)


@app.get("/logout", name="logout")
async def logout(request: Request):
    request.session.clear()
    flash(request, "You have been logged out.", "info")
    return redirect_to(request, "login")


@app.post("/request-drive-access", name="request_drive_access")
async def request_drive_access(request: Request):
    username = _enforce_login(request)
    if not username:
        return redirect_to(request, "login")
    form = await request.form()
    gmail = str(form.get("gmail", "")).strip()
    if not gmail or "@" not in gmail:
        flash(request, "Please enter a valid Gmail address.", "danger")
        return redirect_to(request, "dashboard")

    success, _ = notify_drive_request(username, gmail)
    request.session["drive_requested"] = True
    if success:
        flash(request, "Request sent! You'll be notified here once admin approves your access.", "info")
    else:
        flash(request, "Request logged, but admin notification failed. Contact admin directly.", "warning")
    return redirect_to(request, "dashboard")


@app.get("/connect-drive", name="connect_drive")
async def connect_drive(request: Request):
    username = _enforce_login(request)
    if not username:
        return redirect_to(request, "login")
    try:
        from backend.sync_manager import authenticate_drive

        authenticate_drive(username)
        request.session.pop("drive_requested", None)
        flash(request, "Google Drive connected successfully! Your data will now sync.", "success")
    except Exception:
        flash(request, "Connection failed. Make sure admin has approved your access first.", "danger")
    return redirect_to(request, "dashboard")


@app.get("/dashboard", name="dashboard")
async def dashboard(request: Request):
    username = _enforce_login(request)
    if not username:
        return redirect_to(request, "login")
    return render_view(request, "dashboard.html", {"username": username})


@app.get("/api/events", name="api_events")
async def api_events(request: Request):
    username = _enforce_login(request)
    if not username:
        return redirect_to(request, "login")
    summaries = get_all_dates_summary(username)
    events = []
    for item in summaries:
        balance = item["total_credit"] - item["total_debit"]
        color = "#28a745" if balance >= 0 else "#dc3545"
        events.append(
            {
                "start": item["date"],
                "display": "background",
                "backgroundColor": color + "30",
                "extendedProps": {
                    "total_debit": item["total_debit"],
                    "total_credit": item["total_credit"],
                    "balance": balance,
                },
            }
        )
    return JSONResponse(events)


@app.get("/summary/{date}", name="summary")
async def summary(request: Request, date: str):
    username = _enforce_login(request)
    if not username:
        return redirect_to(request, "login")
    summary_data = get_summary_by_date(username, date)
    return render_view(
        request,
        "summary.html",
        {"date": _format_display_date(date), "summary": summary_data, "username": username},
    )


@app.get("/api/summary/{date}", name="api_summary")
async def api_summary(request: Request, date: str):
    username = _enforce_login(request)
    if not username:
        return redirect_to(request, "login")
    summary_data = get_summary_by_date(username, date)
    return JSONResponse(
        {
            "total_debit": summary_data.get("total_debit", 0.0),
            "total_credit": summary_data.get("total_credit", 0.0),
            "balance": summary_data.get("balance", 0.0),
            "bank_balances": summary_data.get("bank_balances", {}),
        }
    )


@app.get("/ledger/{date}", name="ledger_details")
async def ledger_details(request: Request, date: str):
    username = _enforce_login(request)
    if not username:
        return redirect_to(request, "login")
    ledger = generate_ledger(username, date)
    all_dates = [item["date"] for item in get_all_dates_summary(username)]
    prev_date = None
    next_date = None
    if date in all_dates:
        idx = all_dates.index(date)
        if idx > 0:
            prev_date = all_dates[idx - 1]
        if idx < len(all_dates) - 1:
            next_date = all_dates[idx + 1]

    bank_names = [item["bank_name"] for item in get_all_bank_credentials(username)]
    return render_view(
        request,
        "ledger_details.html",
        {
            "date": _format_display_date(date),
            "iso_date": date,
            "ledger": ledger,
            "username": username,
            "prev_date": prev_date,
            "next_date": next_date,
            "bank_names": bank_names,
        },
    )


@app.api_route("/upload", methods=["GET", "POST"], name="upload")
async def upload(request: Request):
    username = _enforce_login(request)
    if not username:
        return redirect_to(request, "login")
    saved_banks = get_all_bank_credentials(username)
    bank_names = [item["bank_name"] for item in saved_banks]

    if request.method == "GET":
        return _render_upload(request, username, bank_names)

    form = await request.form()
    file = form.get("file")
    bank_name = str(form.get("bank_name", "")).strip()
    if not getattr(file, "filename", "") or not hasattr(file, "file"):
        flash(request, "No file selected.", "warning")
        return _render_upload(request, username, bank_names)

    if not allowed_file(file.filename):
        flash(request, "Unsupported file type. Please upload a PDF, CSV, or Excel file.", "danger")
        return _render_upload(request, username, bank_names)

    stmt_password, err = _get_stmt_password(username, bank_name)
    if err:
        flash(request, err, "danger")
        return _render_upload(request, username, bank_names)

    temp_path = os.path.join(TEMP_FOLDER, "temp_statement" + os.path.splitext(file.filename)[1])
    with open(temp_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    try:
        result = parse_statement(temp_path, password=stmt_password)
        transactions, detected_bank = result if isinstance(result, tuple) else (result, "")
        resolved_bank = (detected_bank or bank_name).strip()

        if not transactions:
            flash(request, "No transactions found in the file.", "warning")
            return _render_upload(request, username, bank_names)

        transactions = apply_merchant_aliases(transactions, username=username)
        preview_path = os.path.join(TEMP_FOLDER, f"{username}_preview.json")
        with open(preview_path, "w", encoding="utf-8") as preview_file:
            json.dump(transactions, preview_file)
        request.session["preview_file"] = preview_path
        request.session["upload_filename"] = file.filename
        request.session["upload_bank_name"] = resolved_bank
        return redirect_to(request, "preview")
    except ValueError as exc:
        flash(request, str(exc), "danger")
        return _render_upload(request, username, bank_names)
    except Exception as exc:
        flash(request, f"Error parsing file: {exc}", "danger")
        return _render_upload(request, username, bank_names)
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)


@app.api_route("/preview", methods=["GET", "POST"], name="preview")
async def preview(request: Request):
    username = _enforce_login(request)
    if not username:
        return redirect_to(request, "login")
    if request.method == "POST":
        preview_path = str(request.session.get("preview_file", ""))
        form = await request.form()
        data_json = str(form.get("transactions_data", "[]"))
        try:
            transactions = json.loads(data_json)
        except json.JSONDecodeError:
            _cleanup_preview_file(preview_path)
            flash(request, "Invalid data format.", "danger")
            return redirect_to(request, "upload")

        if not transactions:
            _cleanup_preview_file(preview_path)
            flash(request, "No transactions to save.", "warning")
            return redirect_to(request, "upload")

        source_bank = str(request.session.pop("upload_bank_name", ""))
        try:
            insert_transactions_bulk(username, transactions, source_bank=source_bank)
        except Exception as exc:
            _cleanup_preview_file(preview_path)
            flash(request, f"Error saving transactions: {exc}", "danger")
            return redirect_to(request, "upload")

        dates = [item.get("date", "") for item in transactions if item.get("date")]
        record_statement_source(
            username,
            file_name=str(request.session.pop("upload_filename", "unknown")),
            txn_count=len(transactions),
            start_date=min(dates) if dates else None,
            end_date=max(dates) if dates else None,
        )

        request.session.pop("preview_file", None)
        _cleanup_preview_file(preview_path)
        sync_upload_after_change(username)
        flash(request, f"Successfully saved {len(transactions)} transactions.", "success")
        return redirect_to(request, "dashboard")

    preview_path = str(request.session.get("preview_file", ""))
    transactions = []
    if preview_path and os.path.exists(preview_path):
        with open(preview_path, "r", encoding="utf-8") as preview_file:
            transactions = json.load(preview_file)

    if not transactions:
        flash(request, "No data to preview. Please upload a file first.", "warning")
        return redirect_to(request, "upload")

    grouped_ledger = group_transactions_for_ledger(transactions)
    return render_view(request, "preview.html", {"grouped_ledger": grouped_ledger, "username": username})


@app.get("/api/ledger/{date}", name="api_ledger")
async def api_ledger(request: Request, date: str):
    username = _enforce_login(request)
    if not username:
        return redirect_to(request, "login")
    return JSONResponse(generate_ledger(username, date))


@app.get("/api/statement", name="api_statement")
async def api_statement(request: Request):
    username = _enforce_login(request)
    if not username:
        return redirect_to(request, "login")
    start = request.query_params.get("start", "")
    end = request.query_params.get("end", "")
    if not start or not end:
        return JSONResponse({"error": "start and end parameters required"}, status_code=400)
    return JSONResponse(get_transactions_by_range(username, start, end))


@app.post("/api/add-transaction/{date}", name="add_transaction_manual")
async def add_transaction_manual(request: Request, date: str):
    username = _enforce_login(request)
    if not username:
        return redirect_to(request, "login")
    try:
        data = await request.json()
    except Exception:
        data = {}

    txn_type = str(data.get("type", "credit"))
    name = str(data.get("name", "")).strip()
    description = str(data.get("description", "")).strip()
    source_bank = str(data.get("bank", "")).strip()
    try:
        amount = float(data.get("amount", 0))
    except (TypeError, ValueError):
        return JSONResponse({"success": False, "error": "Invalid amount"}, status_code=400)

    if amount <= 0:
        return JSONResponse({"success": False, "error": "Amount must be greater than 0"}, status_code=400)
    if not name:
        return JSONResponse({"success": False, "error": "Name is required"}, status_code=400)

    debit = amount if txn_type == "debit" else 0.0
    credit = amount if txn_type == "credit" else 0.0
    insert_transactions_bulk(
        username,
        [
            {
                "date": date,
                "name": name,
                "description": description,
                "user_description": description,
                "debit": debit,
                "credit": credit,
                "balance": None,
                "source_bank": source_bank,
            }
        ],
        source_bank=source_bank,
    )
    sync_upload_after_change(username)
    return JSONResponse({"success": True})


@app.post("/api/update/{txn_id}", name="update_txn")
async def update_txn(request: Request, txn_id: int):
    username = _enforce_login(request)
    if not username:
        return redirect_to(request, "login")
    try:
        data = await request.json()
    except Exception:
        data = {}
    updated = update_transaction(username, txn_id, data)
    if updated:
        sync_upload_after_change(username)
    return JSONResponse({"success": bool(updated)})


@app.post("/api/delete/{txn_id}", name="delete_txn")
async def delete_txn(request: Request, txn_id: int):
    username = _enforce_login(request)
    if not username:
        return redirect_to(request, "login")
    deleted = delete_transaction(username, txn_id)
    if deleted:
        sync_upload_after_change(username)
    return JSONResponse({"success": bool(deleted)})


@app.post("/api/alias", name="save_alias")
async def save_alias(request: Request):
    username = _enforce_login(request)
    if not username:
        return redirect_to(request, "login")
    try:
        data = await request.json()
    except Exception:
        data = {}

    raw_description = str(data.get("raw_description", "")).strip()
    display_name = str(data.get("display_name", "")).strip()
    txn_id = data.get("txn_id")
    if not raw_description or not display_name:
        return JSONResponse({"success": False, "error": "Missing alias data"}, status_code=400)

    set_merchant_alias(username, raw_description, display_name)
    if txn_id:
        try:
            update_transaction(username, int(txn_id), {"name": display_name})
        except Exception:
            pass
    sync_upload_after_change(username)
    return JSONResponse({"success": True})


@app.get("/profile", name="profile")
async def profile(request: Request):
    username = _enforce_login(request)
    if not username:
        return redirect_to(request, "login")
    all_dates = get_all_dates_summary(username)
    total_credit = sum(item.get("total_credit", 0) for item in all_dates)
    total_debit = sum(item.get("total_debit", 0) for item in all_dates)

    latest_balance = 0.0
    latest_date = None
    if all_dates:
        latest = all_dates[-1]
        latest_date = latest.get("date", "")
        try:
            day_summary = get_summary_by_date(username, latest_date)
            latest_balance = day_summary.get("balance", total_credit - total_debit)
        except Exception:
            latest_balance = total_credit - total_debit

    banks = get_all_bank_credentials(username)
    profile_img_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static", "img", "profiles")
    os.makedirs(profile_img_folder, exist_ok=True)
    profile_img_file = os.path.join(profile_img_folder, f"{username}.jpg")
    has_profile_img = os.path.exists(profile_img_file)

    from backend.auth import get_auth_db

    conn = get_auth_db()
    cursor = conn.cursor()
    cursor.execute("SELECT created_at FROM users WHERE username = ?", (username,))
    row = cursor.fetchone()
    conn.close()
    member_since = ""
    if row and row["created_at"]:
        try:
            member_since = datetime.fromisoformat(row["created_at"]).strftime("%d %b %Y")
        except Exception:
            member_since = row["created_at"][:10]

    return render_view(
        request,
        "profile.html",
        {
            "username": username,
            "total_credit": total_credit,
            "total_debit": total_debit,
            "latest_balance": latest_balance,
            "latest_date": latest_date,
            "banks": banks,
            "has_profile_img": has_profile_img,
            "member_since": member_since,
            "transaction_count": sum(1 for _ in all_dates),
        },
    )


@app.post("/profile/upload-photo", name="upload_profile_photo")
async def upload_profile_photo(request: Request):
    username = _enforce_login(request)
    if not username:
        return redirect_to(request, "login")
    form = await request.form()
    file = form.get("photo")
    if not getattr(file, "filename", "") or not hasattr(file, "file"):
        flash(request, "No file selected.", "warning")
        return redirect_to(request, "profile")

    allowed = {"jpg", "jpeg", "png", "gif", "webp"}
    ext = file.filename.rsplit(".", 1)[-1].lower() if "." in file.filename else ""
    if ext not in allowed:
        flash(request, "Only image files are allowed (jpg, png, gif, webp).", "danger")
        return redirect_to(request, "profile")

    profile_img_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static", "img", "profiles")
    os.makedirs(profile_img_folder, exist_ok=True)
    save_path = os.path.join(profile_img_folder, f"{username}.jpg")
    with open(save_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    flash(request, "Profile photo updated!", "success")
    return redirect_to(request, "profile")


@app.post("/change-password", name="change_password")
async def change_password(request: Request):
    username = _enforce_login(request)
    if not username:
        return redirect_to(request, "login")
    form = await request.form()
    current_pw = str(form.get("current_password", ""))
    new_pw = str(form.get("new_password", ""))
    confirm_pw = str(form.get("confirm_password", ""))
    referer = request.headers.get("referer")

    if new_pw != confirm_pw:
        flash(request, "New passwords do not match.", "danger")
        return RedirectResponse(url=referer or _route_url(request, "dashboard"), status_code=303)

    if len(new_pw) < 4:
        flash(request, "Password must be at least 4 characters.", "danger")
        return RedirectResponse(url=referer or _route_url(request, "dashboard"), status_code=303)

    from backend.auth import get_auth_db, hash_password, verify_password

    conn = get_auth_db()
    cursor = conn.cursor()
    cursor.execute("SELECT password_hash FROM users WHERE username = ?", (username,))
    row = cursor.fetchone()
    if not row or not verify_password(current_pw, row["password_hash"]):
        conn.close()
        flash(request, "Current password is incorrect.", "danger")
        return RedirectResponse(url=referer or _route_url(request, "dashboard"), status_code=303)

    cursor.execute("UPDATE users SET password_hash = ? WHERE username = ?", (hash_password(new_pw), username))
    conn.commit()
    conn.close()
    flash(request, "Password updated successfully!", "success")
    return redirect_to(request, "dashboard")


@app.get("/analytics", name="analytics")
async def analytics(request: Request):
    username = _enforce_login(request)
    if not username:
        return redirect_to(request, "login")
    return render_view(request, "analytics.html", {"username": username})


@app.get("/api/chart-data", name="api_chart_data")
async def api_chart_data(request: Request):
    username = _enforce_login(request)
    if not username:
        return redirect_to(request, "login")
    return JSONResponse(get_all_dates_summary(username))


@app.get("/api/bank-balances", name="api_bank_balances")
async def api_bank_balances(request: Request):
    username = _enforce_login(request)
    if not username:
        return redirect_to(request, "login")
    rebuild_daily_summary(username)
    return JSONResponse(get_bank_balances_over_time(username))


@app.get("/api/debug-bank", name="debug_bank")
async def debug_bank(request: Request):
    username = _enforce_login(request)
    if not username:
        return redirect_to(request, "login")
    from backend.database import connect_user_db

    conn = connect_user_db(username)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT date, source_bank, balance, debit, credit
        FROM transactions
        ORDER BY date DESC
        LIMIT 30
        """
    )
    txns = [dict(row) for row in cur.fetchall()]
    cur.execute("SELECT * FROM daily_summary ORDER BY date DESC LIMIT 30")
    summary = [dict(row) for row in cur.fetchall()]
    cur.execute(
        "SELECT COUNT(*) as total, SUM(CASE WHEN balance IS NOT NULL THEN 1 ELSE 0 END) as with_balance FROM transactions"
    )
    counts = dict(cur.fetchone())
    conn.close()
    return JSONResponse({"counts": counts, "transactions_sample": txns, "daily_summary_sample": summary})


@app.get("/export/date/{date}/{fmt}", name="export_date_route")
async def export_date_route(request: Request, date: str, fmt: str):
    username = _enforce_login(request)
    if not username:
        return redirect_to(request, "login")
    return _send_and_cleanup(export_day_ledger(username, date, fmt))


@app.get("/export/range/{fmt}", name="export_range_route")
async def export_range_route(request: Request, fmt: str):
    username = _enforce_login(request)
    if not username:
        return redirect_to(request, "login")
    start_date = request.query_params.get("start")
    end_date = request.query_params.get("end")
    if not start_date or not end_date:
        flash(request, "Please provide both start and end dates.", "danger")
        return redirect_to(request, "dashboard")
    return _send_and_cleanup(export_range_ledger(username, start_date, end_date, fmt))


@app.api_route("/get-statement", methods=["GET", "POST"], name="get_statement")
async def get_statement(request: Request):
    username = _enforce_login(request)
    if not username:
        return redirect_to(request, "login")
    all_dates = get_all_dates_summary(username)
    latest_balance, latest_date_str = _get_latest_balance_for_statement(username, all_dates)
    month_options = _build_month_options(all_dates)
    if request.method == "POST":
        return await _handle_statement_post(request, username, latest_balance, latest_date_str, month_options)

    return render_view(
        request,
        _TPL_STATEMENT,
        {
            "username": username,
            "latest_balance": latest_balance,
            "latest_date": latest_date_str,
            "month_options": month_options,
        },
    )


@app.api_route("/statement-passwords", methods=["GET", "POST"], name="statement_passwords")
async def statement_passwords(request: Request):
    username = _enforce_login(request)
    if not username:
        return redirect_to(request, "login")
    if request.method == "POST":
        form = await request.form()
        bank_name = str(form.get("bank_name", "")).strip()
        password = str(form.get("password", "")).strip()
        if not bank_name or not password:
            flash(request, "Please fill in both bank name and password.", "danger")
        else:
            add_bank_credential(username, bank_name, encrypt_bank_pw(password))
            sync_upload_after_change(username)
            flash(request, f"Password for '{bank_name}' saved successfully.", "success")

    credentials = get_all_bank_credentials(username)
    return render_view(request, _TPL_SETTINGS, {"username": username, "credentials": credentials})


@app.api_route("/settings", methods=["GET", "POST"], name="settings")
async def settings(request: Request):
    username = _enforce_login(request)
    if not username:
        return redirect_to(request, "login")
    if request.method == "POST":
        form = await request.form()
        bank_name = str(form.get("bank_name", "")).strip()
        password = str(form.get("password", "")).strip()
        if not bank_name or not password:
            flash(request, "Please fill in both bank name and password.", "danger")
        else:
            add_bank_credential(username, bank_name, encrypt_bank_pw(password))
            sync_upload_after_change(username)
            flash(request, f"Password for '{bank_name}' saved successfully.", "success")
    return redirect_to(request, "statement_passwords")


@app.post("/api/bank-password/{bank_id}", name="delete_bank_pw")
async def delete_bank_pw(request: Request, bank_id: int):
    username = _enforce_login(request)
    if not username:
        return redirect_to(request, "login")
    delete_bank_credential(username, bank_id)
    sync_upload_after_change(username)
    return JSONResponse({"success": True})


@app.api_route("/sync", methods=["GET", "POST"], name="sync")
async def sync(request: Request):
    username = _enforce_login(request)
    if not username:
        return redirect_to(request, "login")
    result = sync_all(username)
    flash(request, result.get("message", "Sync completed."), "success" if result.get("success") else "danger")
    return redirect_to(request, "dashboard")


@app.get("/api/recent-transactions", name="recent_transactions")
async def recent_transactions(request: Request):
    username = _enforce_login(request)
    if not username:
        return redirect_to(request, "login")
    days_raw = request.query_params.get("days", "30")
    try:
        days = max(1, int(days_raw))
    except ValueError:
        days = 30
    return JSONResponse(get_recent_transactions(username, days=days))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="127.0.0.1", port=5000, reload=True)
