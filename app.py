import os
import json
from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    session,
    flash,
    jsonify,
    send_file,
)
from functools import wraps
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from config import ensure_directories, SECRET_KEY, TEMP_FOLDER, AUTH_DB_PATH
from backend.auth import initialize_auth_db, register_user, login_user
from backend.database import (
    create_user_ledger,
    get_bank_balances_over_time,
    get_all_dates_summary,
    get_summary_by_date,
    get_transactions_by_date,
    get_transactions_by_range,
    get_recent_transactions,
    insert_transactions_bulk,
    update_transaction,
    delete_transaction,
    record_statement_source,
    add_bank_credential,
    get_bank_credential,
    get_all_bank_credentials,
    delete_bank_credential,
)
from backend.security import encrypt_password as encrypt_bank_pw, decrypt_password as decrypt_bank_pw
from backend.parser import parse_statement, allowed_file, group_transactions_for_ledger
from backend.extractor import apply_merchant_aliases
from backend.ledger import generate_ledger
from backend.exporter import export_day_ledger, export_range_ledger
from backend.sync_manager import sync_upload_after_change, sync_download_on_login, sync_all

# Template name constants
_TPL_REGISTER = "register.html"
_TPL_UPLOAD = "upload.html"
_TPL_STATEMENT = "statement.html"
_TPL_SETTINGS = "statement_passwords.html"

app = Flask(__name__)
app.secret_key = SECRET_KEY
app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024  # 16MB max upload

# Rate limiter — protects login and upload from brute-force/abuse
limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=[],          # No global limit; apply per-route
    storage_uri="memory://",    # In-memory (switch to redis:// for production)
)

# Initialize on startup
ensure_directories()

# On new device: try to fetch auth.db from Drive so existing users can log in
try:
    from backend.sync_manager import sync_download
    sync_download("auth.db", AUTH_DB_PATH, subfolder=None)
    print("[KC Tracker] auth.db synced from Drive.")
except Exception as e:
    print(f"[KC Tracker] Drive sync skipped: {e}")

initialize_auth_db()


# ---------------------------------------------------------------------------
# Auth decorator
# ---------------------------------------------------------------------------
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if "username" not in session:
            flash("Please log in first.", "warning")
            return redirect(url_for("login"))
        
        # Ensure schema is up-to-date for existing sessions
        if not session.get("schema_checked"):
            try:
                create_user_ledger(session["username"])
                session["schema_checked"] = True
            except Exception as e:
                print(f"Error checking schema: {e}")

        return f(*args, **kwargs)
    return decorated_function


# ---------------------------------------------------------------------------
# Auth routes
# ---------------------------------------------------------------------------
@app.route("/")
def index():
    if "username" in session:
        return redirect(url_for("dashboard"))
    return redirect(url_for("login"))


def _format_display_date(iso_date):
    """Convert ISO date (YYYY-MM-DD) to display format (DD-MM-YYYY)."""
    try:
        if iso_date and len(iso_date) >= 10 and iso_date[4] == "-":
            parts = iso_date.split("-")
            return f"{parts[2]}-{parts[1]}-{parts[0]}"
    except (ValueError, IndexError):
        pass
    return iso_date


@app.route("/login", methods=["GET", "POST"])
@limiter.limit("10 per minute; 50 per hour")
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")

        if not username or not password:
            flash("Please fill in all fields.", "danger")
            return render_template("login.html")

        success, result = login_user(username, password)
        if success:
            session["username"] = username
            create_user_ledger(username)
            sync_download_on_login(username)
            flash("Login successful!", "success")
            return redirect(url_for("dashboard"))
        flash(result, "danger")

    return render_template("login.html")


@app.route("/register", methods=["GET", "POST"])
@limiter.limit("5 per hour")
def register():
    if request.method == "POST":
        error = _validate_registration()
        if error:
            flash(error, "danger")
            return render_template(_TPL_REGISTER)

        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        success, message = register_user(username, password)
        if success:
            create_user_ledger(username)
            flash("Registration successful! Please log in.", "success")
            return redirect(url_for("login"))
        flash(message, "danger")

    return render_template(_TPL_REGISTER)


def _validate_registration():
    """Validate registration form fields. Returns error message or None."""
    username = request.form.get("username", "").strip()
    password = request.form.get("password", "")
    confirm = request.form.get("confirm_password", "")
    if not username or not password:
        return "Please fill in all fields."
    if password != confirm:
        return "Passwords do not match."
    if len(password) < 4:
        return "Password must be at least 4 characters."
    return None


@app.route("/logout")
def logout():
    session.clear()
    flash("You have been logged out.", "info")
    return redirect(url_for("login"))


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------
@app.route("/dashboard")
@login_required
def dashboard():
    return render_template("dashboard.html", username=session["username"])


# ---------------------------------------------------------------------------
# Calendar API - returns events for FullCalendar
# ---------------------------------------------------------------------------
@app.route("/api/events")
@login_required
def api_events():
    username = session["username"]
    summaries = get_all_dates_summary(username)
    events = []
    for s in summaries:
        balance = s["total_credit"] - s["total_debit"]
        color = "#28a745" if balance >= 0 else "#dc3545"
        events.append({
            "start": s["date"],
            "display": "background",
            "backgroundColor": color + "30",
            "extendedProps": {
                "total_debit": s["total_debit"],
                "total_credit": s["total_credit"],
                "balance": balance,
            },
        })
    return jsonify(events)


# ---------------------------------------------------------------------------
# Summary & Detail views
# ---------------------------------------------------------------------------
@app.route("/summary/<date>")
@login_required
def summary(date):
    username = session["username"]
    summary_data = get_summary_by_date(username, date)
    # Display date in DD-MM-YYYY for UI
    display_date = _format_display_date(date)
    return render_template("summary.html", date=display_date, summary=summary_data, username=username)


@app.route("/api/summary/<date>")
@login_required
def api_summary(date):
    username = session["username"]
    summary_data = get_summary_by_date(username, date)
    return jsonify({
        "total_debit":   summary_data.get("total_debit", 0.0),
        "total_credit":  summary_data.get("total_credit", 0.0),
        "balance":       summary_data.get("balance", 0.0),
        "bank_balances": summary_data.get("bank_balances", {}),
    })


@app.route("/ledger/<date>")
@login_required
def ledger_details(date):
    username = session["username"]
    ledger = generate_ledger(username, date)
    display_date = _format_display_date(date)

    # Build prev/next navigation from all available dates
    all_dates = [s["date"] for s in get_all_dates_summary(username)]
    prev_date = next_date = None
    if date in all_dates:
        idx = all_dates.index(date)
        if idx > 0:
            prev_date = all_dates[idx - 1]
        if idx < len(all_dates) - 1:
            next_date = all_dates[idx + 1]

    return render_template(
        "ledger_details.html",
        date=display_date,
        iso_date=date,
        ledger=ledger,
        username=username,
        prev_date=prev_date,
        next_date=next_date,
    )


# ---------------------------------------------------------------------------
# Upload & Preview
# ---------------------------------------------------------------------------
def _render_upload(username, bank_names):
    """Render the upload template with standard context."""
    return render_template(_TPL_UPLOAD, username=username, bank_names=bank_names)


def _get_stmt_password(username, bank_name):
    """Fetch and decrypt the statement password for a bank. Returns (password, error)."""
    if not bank_name:
        return None, None
    encrypted = get_bank_credential(username, bank_name)
    if not encrypted:
        return None, f"No password found for '{bank_name}'. Please add it in Settings."
    try:
        return decrypt_bank_pw(encrypted), None
    except Exception:
        return None, "Failed to decrypt bank password."


@app.route("/upload", methods=["GET", "POST"])
@login_required
@limiter.limit("30 per hour", methods=["POST"])
def upload():
    username = session["username"]
    saved_banks = get_all_bank_credentials(username)
    bank_names = [b["bank_name"] for b in saved_banks]

    if request.method != "POST":
        return _render_upload(username, bank_names)

    file = request.files.get("file")
    if not file or file.filename == "":
        flash("No file selected.", "danger")
        return _render_upload(username, bank_names)

    if not allowed_file(file.filename):
        flash("File type not allowed. Use CSV, Excel, or PDF.", "danger")
        return _render_upload(username, bank_names)

    bank_name = request.form.get("bank_name", "").strip()
    stmt_password, pw_error = _get_stmt_password(username, bank_name)
    if pw_error:
        flash(pw_error, "danger")
        return _render_upload(username, bank_names)

    return _process_upload(file, username, bank_names, stmt_password, bank_name)


def _process_upload(file, username, bank_names, stmt_password, bank_name=""):
    """Save temp file, parse, apply aliases, redirect to preview."""
    temp_path = os.path.join(TEMP_FOLDER, "temp_statement" + os.path.splitext(file.filename)[1])
    file.save(temp_path)
    try:
        result = parse_statement(temp_path, password=stmt_password)
        transactions, detected_bank = result if isinstance(result, tuple) else (result, "")
        # Auto-detected bank name wins; fall back to user-selected name from form
        resolved_bank = (detected_bank or bank_name).strip()

        if not transactions:
            flash("No transactions found in the file.", "warning")
            return _render_upload(username, bank_names)
        transactions = apply_merchant_aliases(transactions, username=username)
        # Store in temp file (session cookies have 4KB limit)
        preview_path = os.path.join(TEMP_FOLDER, f"{username}_preview.json")
        with open(preview_path, "w", encoding="utf-8") as pf:
            json.dump(transactions, pf)
        session["preview_file"] = preview_path
        session["upload_filename"] = file.filename
        session["upload_bank_name"] = resolved_bank
        print(f"[Upload] Bank resolved as: '{resolved_bank}'")
        return redirect(url_for("preview"))
    except ValueError as e:
        flash(str(e), "danger")
        return _render_upload(username, bank_names)
    except Exception as e:
        flash(f"Error parsing file: {str(e)}", "danger")
        return _render_upload(username, bank_names)
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)


@app.route("/preview", methods=["GET", "POST"])
@login_required
def preview():
    if request.method == "POST":
        return _handle_preview_save()

    # GET: show preview
    preview_path = session.get("preview_file", "")
    transactions = []
    if preview_path and os.path.exists(preview_path):
        with open(preview_path, "r", encoding="utf-8") as pf:
            transactions = json.load(pf)
    if not transactions:
        flash("No data to preview. Please upload a file first.", "warning")
        return redirect(url_for("upload"))

    grouped_ledger = group_transactions_for_ledger(transactions)
    return render_template("preview.html", grouped_ledger=grouped_ledger, username=session["username"])


def _handle_preview_save():
    """Handle POST from preview — save transactions to DB."""
    # Always clean up preview temp file, even if something fails below
    preview_path = session.get("preview_file", "")

    def _cleanup_preview():
        if preview_path and os.path.exists(preview_path):
            try:
                os.remove(preview_path)
            except Exception:
                pass

    data_json = request.form.get("transactions_data", "[]")
    print(f"[SAVE] JSON length: {len(data_json)}")
    try:
        transactions = json.loads(data_json)
    except json.JSONDecodeError as e:
        print(f"[SAVE] JSON decode error: {e}")
        _cleanup_preview()
        flash("Invalid data format.", "danger")
        return redirect(url_for("upload"))

    print(f"[SAVE] Parsed {len(transactions)} transactions")
    if transactions:
        print(f"[SAVE] Sample: {transactions[0]}")

    if not transactions:
        _cleanup_preview()
        flash("No transactions to save.", "warning")
        return redirect(url_for("upload"))

    username = session["username"]
    source_bank = session.pop("upload_bank_name", "")
    try:
        insert_transactions_bulk(username, transactions, source_bank=source_bank)
        print(f"[SAVE] Inserted successfully for user '{username}' (bank: {source_bank or 'untagged'})")
    except Exception as e:
        _cleanup_preview()
        print(f"[SAVE] Insert error: {e}")
        import traceback
        traceback.print_exc()

    # Record statement source metadata
    dates = [t.get("date", "") for t in transactions if t.get("date")]
    record_statement_source(
        username,
        file_name=session.pop("upload_filename", "unknown"),
        txn_count=len(transactions),
        start_date=min(dates) if dates else None,
        end_date=max(dates) if dates else None,
    )

    # Clean up preview temp file (guaranteed)
    session.pop("preview_file", "")
    _cleanup_preview()

    sync_upload_after_change(username)
    flash(f"Successfully saved {len(transactions)} transactions.", "success")
    return redirect(url_for("dashboard"))


# ---------------------------------------------------------------------------
# Export routes
# ---------------------------------------------------------------------------
@app.route("/export/date/<date>/<fmt>")
@login_required
def export_date_route(date, fmt):
    username = session["username"]
    filepath = export_day_ledger(username, date, fmt)
    return _send_and_cleanup(filepath)


@app.route("/export/range/<fmt>")
@login_required
def export_range_route(fmt):
    username = session["username"]
    start_date = request.args.get("start")
    end_date = request.args.get("end")
    if not start_date or not end_date:
        flash("Please provide both start and end dates.", "danger")
        return redirect(url_for("dashboard"))
    filepath = export_range_ledger(username, start_date, end_date, fmt)
    return _send_and_cleanup(filepath)


def _send_and_cleanup(filepath):
    """Send file as download, then delete the temp export file."""
    from flask import after_this_request

    @after_this_request
    def remove_file(response):
        try:
            if os.path.exists(filepath):
                os.remove(filepath)
        except Exception:
            pass
        return response

    return send_file(filepath, as_attachment=True)


# ---------------------------------------------------------------------------
# Get Statement route
# ---------------------------------------------------------------------------
def _get_latest_balance_for_statement(username, all_dates):
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

def _resolve_period_dates(period):
    from datetime import date, timedelta
    import calendar as _cal
    today = date.today()
    if period == "this_month":
        return today.replace(day=1).isoformat(), today.isoformat()
    if period == "last_month":
        first = (today.replace(day=1) - timedelta(days=1)).replace(day=1)
        last  = today.replace(day=1) - timedelta(days=1)
        return first.isoformat(), last.isoformat()
    if period == "last_3_months":
        return (today - timedelta(days=90)).isoformat(), today.isoformat()
    if period == "last_6_months":
        return (today - timedelta(days=180)).isoformat(), today.isoformat()
    if period == "this_year":
        return today.replace(month=1, day=1).isoformat(), today.isoformat()
    if period == "last_year":
        return date(today.year - 1, 1, 1).isoformat(), date(today.year - 1, 12, 31).isoformat()
    if period == "fy_current":   # Apr 1 current FY → today
        fy_start = date(today.year if today.month >= 4 else today.year - 1, 4, 1)
        return fy_start.isoformat(), today.isoformat()
    if period == "fy_previous":
        fy_yr = today.year - 1 if today.month >= 4 else today.year - 2
        return date(fy_yr, 4, 1).isoformat(), date(fy_yr + 1, 3, 31).isoformat()
    if period == "all_time":
        return "2000-01-01", today.isoformat()
    if period and period.startswith("month_"):
        parts = period.split("_")
        yr, mo = int(parts[1]), int(parts[2])
        last_day = _cal.monthrange(yr, mo)[1]
        return f"{yr:04d}-{mo:02d}-01", f"{yr:04d}-{mo:02d}-{last_day:02d}"
    return None, None

def _build_month_options(all_dates):
    month_options = []
    seen = set()
    import calendar as _cal2
    for d_row in reversed(all_dates):
        d_str = d_row.get("date", "")
        if len(d_str) >= 7:
            ym = d_str[:7]  # "YYYY-MM"
            if ym not in seen:
                seen.add(ym)
                yr, mo = int(ym[:4]), int(ym[5:7])
                label = f"{_cal2.month_name[mo]} {yr}"
                month_options.append({"value": f"month_{yr}_{mo:02d}", "label": label})
    return month_options

def _handle_statement_post(username, latest_balance, latest_date_str, month_options):
    period    = request.form.get("period", "recent_30").strip()
    start_date = request.form.get("start_date", "").strip()
    end_date   = request.form.get("end_date", "").strip()

    if period == "recent_30":
        transactions = get_recent_transactions(username, days=30)
        if not transactions:
            flash("No transactions found in the last 30 days.", "warning")
            return render_template(_TPL_STATEMENT, username=username,
                                   latest_balance=latest_balance, latest_date=latest_date_str,
                                   month_options=month_options)
        grouped = group_transactions_for_ledger(transactions)
        end_date   = transactions[0]["date"]
        start_date = transactions[-1]["date"]
        return render_template(_TPL_STATEMENT,
                               username=username, grouped=grouped,
                               start_date=start_date, end_date=end_date,
                               period=period, latest_balance=latest_balance,
                               latest_date=latest_date_str, month_options=month_options)

    if period != "custom":
        start_date, end_date = _resolve_period_dates(period)

    if not start_date or not end_date:
        flash("Please select both start and end dates.", "danger")
        return render_template(_TPL_STATEMENT, username=username,
                               latest_balance=latest_balance, latest_date=latest_date_str,
                               month_options=month_options)

    transactions = get_transactions_by_range(username, start_date, end_date)
    if not transactions:
        flash("No transactions found for the selected range.", "warning")
        return render_template(_TPL_STATEMENT, username=username,
                               latest_balance=latest_balance, latest_date=latest_date_str,
                               month_options=month_options)

    grouped = group_transactions_for_ledger(transactions)
    return render_template(
        _TPL_STATEMENT,
        username=username, grouped=grouped,
        start_date=start_date, end_date=end_date,
        period=period, latest_balance=latest_balance,
        latest_date=latest_date_str, month_options=month_options,
    )

@app.route("/get-statement", methods=["GET", "POST"])
@login_required
def get_statement():
    username = session["username"]

    all_dates = get_all_dates_summary(username)
    latest_balance, latest_date_str = _get_latest_balance_for_statement(username, all_dates)
    month_options = _build_month_options(all_dates)

    if request.method == "POST":
        return _handle_statement_post(username, latest_balance, latest_date_str, month_options)

    return render_template(_TPL_STATEMENT, username=username,
                           latest_balance=latest_balance, latest_date=latest_date_str,
                           month_options=month_options)


# ---------------------------------------------------------------------------
# Statement Passwords (legacy route — redirects to settings)
# ---------------------------------------------------------------------------
@app.route("/statement-passwords", methods=["GET", "POST"])
@login_required
def statement_passwords():
    username = session["username"]

    if request.method == "POST":
        bank_name = request.form.get("bank_name", "").strip()
        password = request.form.get("password", "").strip()
        if not bank_name or not password:
            flash("Please fill in both bank name and password.", "danger")
        else:
            encrypted = encrypt_bank_pw(password)
            add_bank_credential(username, bank_name, encrypted)
            sync_upload_after_change(username)
            flash(f"Password for '{bank_name}' saved successfully.", "success")
        return redirect(url_for("statement_passwords"))

    banks = get_all_bank_credentials(username)
    return render_template(_TPL_SETTINGS, username=username, banks=banks)


# ---------------------------------------------------------------------------
# Delete transaction
# ---------------------------------------------------------------------------
@app.route("/api/delete/<int:txn_id>", methods=["POST"])
@login_required
def delete_txn(txn_id):
    username = session["username"]
    delete_transaction(username, txn_id)
    sync_upload_after_change(username)
    return jsonify({"success": True})


@app.route("/api/update/<int:txn_id>", methods=["POST"])
@login_required
def update_txn(txn_id):
    username = session["username"]
    data = request.get_json(silent=True) or {}
    result = update_transaction(username, txn_id, data)
    if result:
        sync_upload_after_change(username)
    return jsonify({"success": result})


# ---------------------------------------------------------------------------
# Settings & Statement Passwords Manager
# ---------------------------------------------------------------------------
@app.route("/settings", methods=["GET"])
@login_required
def settings():
    username = session["username"]
    banks = get_all_bank_credentials(username)
    return render_template(_TPL_SETTINGS, username=username, banks=banks)


@app.route("/settings/update-bank-password", methods=["POST"])
@login_required
def update_bank_password():
    username = session["username"]

    bank_name = request.form.get("bank_name", "").strip()
    password = request.form.get("password", "").strip()

    if not bank_name or not password:
        flash("Please fill in both bank name and password.", "danger")
    else:
        encrypted = encrypt_bank_pw(password)
        add_bank_credential(username, bank_name, encrypted)
        sync_upload_after_change(username)
        flash(f"Password for '{bank_name}' saved successfully.", "success")

    return redirect(url_for("settings"))


@app.route("/api/bank-password/<int:bank_id>", methods=["POST"])
@login_required
def delete_bank_pw(bank_id):
    username = session["username"]
    delete_bank_credential(username, bank_id)
    sync_upload_after_change(username)
    return jsonify({"success": True})


# ---------------------------------------------------------------------------
# Sync
# ---------------------------------------------------------------------------
@app.route("/sync", methods=["GET", "POST"])
@login_required
def sync():
    username = session["username"]
    result = sync_all(username)
    category = "success" if result.get("success") else "danger"
    flash(result.get("message", "Sync completed."), category)
    return redirect(url_for("dashboard"))


# ---------------------------------------------------------------------------
# API Routes (JSON — mobile-ready)
# ---------------------------------------------------------------------------
@app.route("/api/ledger/<date>")
@login_required
def api_ledger(date):
    """Return daily ledger data as JSON."""
    username = session["username"]
    ledger = generate_ledger(username, date)
    return jsonify(ledger)


@app.route("/api/statement")
@login_required
def api_statement():
    """Return transactions for a date range as JSON."""
    username = session["username"]
    start = request.args.get("start", "")
    end = request.args.get("end", "")
    if not start or not end:
        return jsonify({"error": "start and end parameters required"}), 400
    txns = get_transactions_by_range(username, start, end)
    return jsonify(txns)


# ---------------------------------------------------------------------------
# Profile & Account
# ---------------------------------------------------------------------------
@app.route("/profile")
@login_required
def profile():
    username = session["username"]

    # Get total balance from latest date summary
    all_dates = get_all_dates_summary(username)
    total_credit = sum(d.get("total_credit", 0) for d in all_dates)
    total_debit  = sum(d.get("total_debit",  0) for d in all_dates)

    # Latest closing balance — from the most recent date's summary
    latest_balance = 0.0
    latest_date    = None
    if all_dates:
        latest = all_dates[-1]
        latest_date = latest.get("date", "")
        # Try fetching balance from summary_by_date for the most recent day
        try:
            day_summary = get_summary_by_date(username, latest_date)
            latest_balance = day_summary.get("balance", total_credit - total_debit)
        except Exception:
            latest_balance = total_credit - total_debit

    # Bank passwords list
    banks = get_all_bank_credentials(username)

    # Profile picture path
    profile_img_folder = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "static", "img", "profiles"
    )
    os.makedirs(profile_img_folder, exist_ok=True)
    profile_img_file = os.path.join(profile_img_folder, f"{username}.jpg")
    has_profile_img  = os.path.exists(profile_img_file)

    # Get member since from auth db
    from backend.auth import get_auth_db
    conn = get_auth_db()
    cursor = conn.cursor()
    cursor.execute("SELECT created_at FROM users WHERE username = ?", (username,))
    row = cursor.fetchone()
    conn.close()
    member_since = ""
    if row and row["created_at"]:
        try:
            from datetime import datetime
            dt = datetime.fromisoformat(row["created_at"])
            member_since = dt.strftime("%d %b %Y")
        except Exception:
            member_since = row["created_at"][:10]

    return render_template(
        "profile.html",
        username=username,
        total_credit=total_credit,
        total_debit=total_debit,
        latest_balance=latest_balance,
        latest_date=latest_date,
        banks=banks,
        has_profile_img=has_profile_img,
        member_since=member_since,
        transaction_count=sum(1 for _ in all_dates),
    )


@app.route("/profile/upload-photo", methods=["POST"])
@login_required
def upload_profile_photo():
    username = session["username"]
    file = request.files.get("photo")
    if not file or file.filename == "":
        flash("No file selected.", "warning")
        return redirect(url_for("profile"))

    allowed = {"jpg", "jpeg", "png", "gif", "webp"}
    ext = file.filename.rsplit(".", 1)[-1].lower() if "." in file.filename else ""
    if ext not in allowed:
        flash("Only image files are allowed (jpg, png, gif, webp).", "danger")
        return redirect(url_for("profile"))

    profile_img_folder = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "static", "img", "profiles"
    )
    os.makedirs(profile_img_folder, exist_ok=True)
    # Save as jpg regardless of input ext for simplicity
    save_path = os.path.join(profile_img_folder, f"{username}.jpg")
    file.save(save_path)
    flash("Profile photo updated!", "success")
    return redirect(url_for("profile"))


@app.route("/change-password", methods=["POST"])
@login_required
def change_password():
    username = session["username"]
    current_pw  = request.form.get("current_password", "")
    new_pw      = request.form.get("new_password", "")
    confirm_pw  = request.form.get("confirm_password", "")

    if new_pw != confirm_pw:
        flash("New passwords do not match.", "danger")
        return redirect(request.referrer or url_for("dashboard"))

    if len(new_pw) < 4:
        flash("Password must be at least 4 characters.", "danger")
        return redirect(request.referrer or url_for("dashboard"))

    from backend.auth import get_auth_db, verify_password, hash_password
    conn = get_auth_db()
    cursor = conn.cursor()
    cursor.execute("SELECT password_hash FROM users WHERE username = ?", (username,))
    row = cursor.fetchone()

    if not row or not verify_password(current_pw, row["password_hash"]):
        conn.close()
        flash("Current password is incorrect.", "danger")
        return redirect(request.referrer or url_for("dashboard"))

    new_hash = hash_password(new_pw)
    cursor.execute("UPDATE users SET password_hash = ? WHERE username = ?", (new_hash, username))
    conn.commit()
    conn.close()
    flash("Password updated successfully!", "success")
    return redirect(url_for("dashboard"))


# ---------------------------------------------------------------------------
# Charts
# ---------------------------------------------------------------------------
@app.route("/charts")
@login_required
def charts():
    return render_template("charts.html", username=session["username"])


@app.route("/api/chart-data")
@login_required
def api_chart_data():
    data = get_all_dates_summary(session["username"])
    return jsonify(data)


@app.route("/api/bank-balances")
@login_required
def api_bank_balances():
    data = get_bank_balances_over_time(session["username"])
    return jsonify(data)


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    app.run(debug=True, host="127.0.0.1", port=5000)