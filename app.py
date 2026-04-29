import os
import re
import json
import base64
import hashlib
import secrets
import sqlite3
import mimetypes
import threading
from datetime import datetime
from urllib.parse import quote, unquote
from flask import Flask, redirect, request, session, url_for, render_template, send_file, Response
from google_auth_oauthlib.flow import Flow
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from dotenv import load_dotenv
import io
import csv
import fitz  # PyMuPDF

load_dotenv()

app = Flask(__name__)
app.secret_key = os.urandom(24)

# 允許在本機開發使用 HTTP (不需要 HTTPS)
os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

CLIENT_SECRETS_FILE = "credentials.json"
TOKEN_FILE          = "token.json"   # 持久化 OAuth token
DB_FILE             = "wmmg.db"

SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
]

# 實際關鍵字從 .env 載入，不寫死於程式碼中
SEARCH_QUERY = os.environ.get("SEARCH_QUERY", "")
SEARCH_LABEL = "關鍵字"
TRANSACTION_QUERY = os.environ.get("TRANSACTION_QUERY", "")
SECURITIES_QUERY    = os.environ.get("SECURITIES_QUERY", "")
SECURITIES_PDF_PWD  = os.environ.get("SECURITIES_PDF_PASSWORD", "")
TRANSFER_QUERY      = os.environ.get("TRANSFER_QUERY", "")
CHANGHWA_QUERY      = os.environ.get("CHANGHWA_QUERY", "")
DOWNLOAD_DIR = "attachments"

os.makedirs(DOWNLOAD_DIR, exist_ok=True)


# ── 資料庫初始化 ─────────────────────────────────────────────────────────

def _db_connect():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def _init_db():
    with _db_connect() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS messages (
            id          TEXT PRIMARY KEY,
            subject     TEXT,
            sender      TEXT,
            date        TEXT,
            snippet     TEXT,
            attachments TEXT,
            synced_at   TEXT
        );

        CREATE TABLE IF NOT EXISTS transactions (
            rowid       INTEGER PRIMARY KEY AUTOINCREMENT,
            email_id    TEXT,
            card_name   TEXT,
            last4       TEXT,
            card_type   TEXT,
            auth_date   TEXT,
            auth_time   TEXT,
            auth_area   TEXT,
            amount      TEXT,
            merchant    TEXT,
            email_date  TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_tx_email ON transactions(email_id);

        CREATE TABLE IF NOT EXISTS transfers (
            rowid       INTEGER PRIMARY KEY AUTOINCREMENT,
            email_id    TEXT UNIQUE,
            data_json   TEXT,
            email_date  TEXT
        );

        CREATE TABLE IF NOT EXISTS securities (
            rowid       INTEGER PRIMARY KEY AUTOINCREMENT,
            email_id    TEXT,
            subject     TEXT,
            filename    TEXT,
            email_date  TEXT,
            data_json   TEXT,
            UNIQUE(email_id, filename)
        );

        CREATE TABLE IF NOT EXISTS changhwa_deposits (
            rowid        INTEGER PRIMARY KEY AUTOINCREMENT,
            email_id     TEXT UNIQUE,
            txn_datetime TEXT,
            txn_type     TEXT,
            txn_memo     TEXT,
            txn_date     TEXT,
            book_date    TEXT,
            to_account   TEXT,
            from_account TEXT,
            amount       TEXT,
            currency     TEXT,
            email_date   TEXT
        );

        CREATE TABLE IF NOT EXISTS sync_log (
            rowid       INTEGER PRIMARY KEY AUTOINCREMENT,
            synced_at   TEXT,
            status      TEXT,
            message     TEXT
        );
        """)


_init_db()

_sync_lock = threading.Lock()
_is_syncing = False  # 全域同步狀態旗標


def get_credentials():
    """優先從 session 讀取；若無，從 token.json 讀取"""
    creds = None
    if "credentials" in session:
        creds = Credentials(**session["credentials"])
    elif os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        creds = Credentials(**data)
    return creds


def save_credentials(creds):
    data = {
        "token":         creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri":     creds.token_uri,
        "client_id":     creds.client_id,
        "client_secret": creds.client_secret,
        "scopes":        list(creds.scopes) if creds.scopes else [],
    }
    session["credentials"] = data
    with open(TOKEN_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ── 同步核心 ────────────────────────────────────────────────────────────────────────

def _do_sync(creds):
    """抓取 Gmail 資料並 upsert 至 SQLite。回傳 (ok: bool, msg: str)"""
    try:
        service = build("gmail", "v1", credentials=creds)
        with _db_connect() as conn:
            synced_at = datetime.now().isoformat(timespec="seconds")

            # 1. 一般郵件
            results = service.users().messages().list(
                userId="me", q=SEARCH_QUERY, maxResults=50
            ).execute()
            for msg_ref in results.get("messages", []):
                if conn.execute("SELECT 1 FROM messages WHERE id=?", (msg_ref["id"],)).fetchone():
                    continue
                msg = service.users().messages().get(
                    userId="me", id=msg_ref["id"], format="full"
                ).execute()
                headers = {h["name"]: h["value"] for h in msg["payload"].get("headers", [])}
                atts = []
                _collect_attachments(msg["payload"], msg_ref["id"], atts)
                conn.execute(
                    "INSERT OR IGNORE INTO messages(id,subject,sender,date,snippet,attachments,synced_at) VALUES(?,?,?,?,?,?,?)",
                    (msg_ref["id"], headers.get("Subject",""), headers.get("From",""),
                     headers.get("Date",""), msg.get("snippet",""),
                     json.dumps(atts, ensure_ascii=False), synced_at)
                )

            # 2. 交易明細
            results = service.users().messages().list(
                userId="me", q=f'subject:"{TRANSACTION_QUERY}"', maxResults=100
            ).execute()
            for msg_ref in results.get("messages", []):
                if conn.execute("SELECT 1 FROM transactions WHERE email_id=?", (msg_ref["id"],)).fetchone():
                    continue
                msg = service.users().messages().get(
                    userId="me", id=msg_ref["id"], format="full"
                ).execute()
                headers    = {h["name"]: h["value"] for h in msg["payload"].get("headers", [])}
                email_date = headers.get("Date", "")
                rows       = _parse_transactions(_get_body_html(msg["payload"]))
                for r in rows:
                    conn.execute(
                        """INSERT INTO transactions
                           (email_id,card_name,last4,card_type,auth_date,auth_time,auth_area,amount,merchant,email_date)
                           VALUES(?,?,?,?,?,?,?,?,?,?)""",
                        (msg_ref["id"], r["用卡名稱"], r["卡號後4碼"], r["主/附卡"],
                         r["授權日期"], r["授權時間"], r["授權地區"],
                         r["授權金額(約當臺幣)"], r["商店名稱"], email_date)
                    )

            # 3. 轉帳通知
            results = service.users().messages().list(
                userId="me", q=f'subject:"{TRANSFER_QUERY}"', maxResults=200
            ).execute()
            for msg_ref in results.get("messages", []):
                if conn.execute("SELECT 1 FROM transfers WHERE email_id=?", (msg_ref["id"],)).fetchone():
                    continue
                msg = service.users().messages().get(
                    userId="me", id=msg_ref["id"], format="full"
                ).execute()
                headers    = {h["name"]: h["value"] for h in msg["payload"].get("headers", [])}
                email_date = headers.get("Date", "")
                subject    = headers.get("Subject", "")
                rec = _parse_transfer_html(_get_body_html(msg["payload"]), email_date, subject)
                if rec:
                    conn.execute(
                        "INSERT OR IGNORE INTO transfers(email_id,data_json,email_date) VALUES(?,?,?)",
                        (msg_ref["id"], json.dumps(rec, ensure_ascii=False), email_date)
                    )

            # 4. 有價證券對帳單
            results = service.users().messages().list(
                userId="me",
                q=f'subject:"{SECURITIES_QUERY}" has:attachment filename:pdf',
                maxResults=50,
            ).execute()
            for msg_ref in results.get("messages", []):
                msg = service.users().messages().get(
                    userId="me", id=msg_ref["id"], format="full"
                ).execute()
                headers    = {h["name"]: h["value"] for h in msg["payload"].get("headers", [])}
                email_date = headers.get("Date", "")
                subject    = headers.get("Subject", "")
                pdf_atts   = []
                _collect_pdf_attachments(msg["payload"], msg_ref["id"], pdf_atts)
                for att in pdf_atts:
                    if conn.execute(
                        "SELECT 1 FROM securities WHERE email_id=? AND filename=?",
                        (msg_ref["id"], att["filename"])
                    ).fetchone():
                        continue
                    try:
                        pdf_bytes = _fetch_pdf_bytes(service, msg_ref["id"], att["attachment_id"])
                        report    = _parse_securities_pdf(pdf_bytes, email_date)
                        report["subject"]  = subject
                        report["filename"] = att["filename"]
                        conn.execute(
                            "INSERT OR IGNORE INTO securities(email_id,subject,filename,email_date,data_json) VALUES(?,?,?,?,?)",
                            (msg_ref["id"], subject, att["filename"], email_date,
                             json.dumps(report, ensure_ascii=False))
                        )
                    except Exception as e:
                        conn.execute(
                            "INSERT OR IGNORE INTO securities(email_id,subject,filename,email_date,data_json) VALUES(?,?,?,?,?)",
                            (msg_ref["id"], subject, att["filename"], email_date,
                             json.dumps({"error": str(e), "transactions": [], "totals": {}, "notes": {}},
                                        ensure_ascii=False))
                        )

            # 5. 彰化銀行入帳通知
            if CHANGHWA_QUERY:
                results = service.users().messages().list(
                    userId="me", q=f'subject:"{CHANGHWA_QUERY}"', maxResults=200
                ).execute()
                for msg_ref in results.get("messages", []):
                    if conn.execute("SELECT 1 FROM changhwa_deposits WHERE email_id=?", (msg_ref["id"],)).fetchone():
                        continue
                    msg = service.users().messages().get(
                        userId="me", id=msg_ref["id"], format="full"
                    ).execute()
                    headers    = {h["name"]: h["value"] for h in msg["payload"].get("headers", [])}
                    email_date = headers.get("Date", "")
                    rec = _parse_changhwa_html(_get_body_html(msg["payload"]), email_date)
                    if rec:
                        conn.execute(
                            """INSERT OR IGNORE INTO changhwa_deposits
                               (email_id,txn_datetime,txn_type,txn_memo,txn_date,book_date,
                                to_account,from_account,amount,currency,email_date)
                               VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                            (msg_ref["id"], rec["txn_datetime"], rec["txn_type"], rec["txn_memo"],
                             rec["txn_date"], rec["book_date"], rec["to_account"],
                             rec["from_account"], rec["amount"], rec["currency"], email_date)
                        )

            conn.execute(
                "INSERT INTO sync_log(synced_at,status,message) VALUES(?,?,?)",
                (synced_at, "ok", "同步完成")
            )
        return True, f"同步完成 {synced_at}"
    except Exception as e:
        with _db_connect() as conn:
            conn.execute(
                "INSERT INTO sync_log(synced_at,status,message) VALUES(?,?,?)",
                (datetime.now().isoformat(timespec="seconds"), "error", str(e))
            )
        return False, str(e)


@app.route("/")
def index():
    creds = get_credentials()
    authenticated = bool(creds and creds.valid)
    last_sync = None
    with _db_connect() as conn:
        row = conn.execute(
            "SELECT synced_at, status, message FROM sync_log ORDER BY rowid DESC LIMIT 1"
        ).fetchone()
        if row:
            last_sync = dict(row)
    return render_template("index.html", authenticated=authenticated, last_sync=last_sync,
                           is_syncing=_is_syncing)


@app.route("/sync", methods=["POST"])
def sync():
    global _is_syncing
    creds = get_credentials()
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            save_credentials(creds)
        else:
            from flask import jsonify
            return jsonify({"ok": False, "error": "not_authenticated"}), 401
    if not _sync_lock.acquire(blocking=False):
        from flask import jsonify
        return jsonify({"ok": False, "error": "already_syncing"}), 409
    _is_syncing = True
    def _run():
        global _is_syncing
        try:
            _do_sync(creds)
        finally:
            _is_syncing = False
            _sync_lock.release()
    threading.Thread(target=_run, daemon=True).start()
    from flask import jsonify
    return jsonify({"ok": True})


@app.route("/sync/status")
def sync_status():
    from flask import jsonify
    return jsonify({"syncing": _is_syncing})


@app.route("/clear-db", methods=["POST"])
def clear_db():
    creds = get_credentials()
    if not creds or not creds.valid:
        return redirect(url_for("login"))
    with _db_connect() as conn:
        conn.executescript("""
            DELETE FROM messages;
            DELETE FROM transactions;
            DELETE FROM transfers;
            DELETE FROM securities;
            DELETE FROM changhwa_deposits;
            DELETE FROM sync_log;
        """)
    return redirect(url_for("index"))


def _generate_pkce_pair():
    """產生 PKCE code_verifier 與 code_challenge (S256)"""
    code_verifier = secrets.token_urlsafe(64)
    digest = hashlib.sha256(code_verifier.encode("ascii")).digest()
    code_challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
    return code_verifier, code_challenge


@app.route("/login")
def login():
    code_verifier, code_challenge = _generate_pkce_pair()
    session["code_verifier"] = code_verifier

    flow = Flow.from_client_secrets_file(
        CLIENT_SECRETS_FILE,
        scopes=SCOPES,
        redirect_uri=url_for("oauth2callback", _external=True),
    )
    authorization_url, state = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent",
        code_challenge=code_challenge,
        code_challenge_method="S256",
    )
    session["state"] = state
    return redirect(authorization_url)


@app.route("/oauth2callback")
def oauth2callback():
    state = session.get("state")
    code_verifier = session.pop("code_verifier", None)

    flow = Flow.from_client_secrets_file(
        CLIENT_SECRETS_FILE,
        scopes=SCOPES,
        state=state,
        redirect_uri=url_for("oauth2callback", _external=True),
    )
    flow.fetch_token(
        authorization_response=request.url,
        code_verifier=code_verifier,
    )
    creds = flow.credentials
    save_credentials(creds)
    return redirect(url_for("index"))


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("index"))


@app.route("/messages")
def messages():
    creds = get_credentials()
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            save_credentials(creds)
        else:
            return redirect(url_for("login"))

    emails = []
    with _db_connect() as conn:
        rows = conn.execute(
            "SELECT id, subject, sender, date, snippet, attachments FROM messages ORDER BY rowid DESC"
        ).fetchall()
        for row in rows:
            emails.append({
                "id":          row["id"],
                "subject":     row["subject"],
                "sender":      row["sender"],
                "date":        row["date"],
                "snippet":     row["snippet"],
                "attachments": json.loads(row["attachments"] or "[]"),
            })

    return render_template("messages.html", emails=emails, label=SEARCH_LABEL)


def _collect_attachments(payload, message_id, attachments):
    """遞迴收集所有 parts 中的附件"""
    if "parts" in payload:
        for part in payload["parts"]:
            _collect_attachments(part, message_id, attachments)
    else:
        filename = payload.get("filename", "")
        body = payload.get("body", {})
        attachment_id = body.get("attachmentId")
        if filename and attachment_id:
            attachments.append({
                "filename": filename,
                "filename_encoded": quote(filename, safe=""),
                "attachment_id": attachment_id,
                "message_id": message_id,
                "mime_type": payload.get("mimeType", "application/octet-stream"),
            })


@app.route("/download/<message_id>/<attachment_id>")
def download_attachment(message_id, attachment_id):
    creds = get_credentials()
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            save_credentials(creds)
        else:
            return redirect(url_for("login"))

    service = build("gmail", "v1", credentials=creds)

    attachment = service.users().messages().attachments().get(
        userId="me",
        messageId=message_id,
        id=attachment_id,
    ).execute()

    file_data = base64.urlsafe_b64decode(attachment["data"].encode("UTF-8"))

    # 優先從 query string 取得已知的 filename（由 _collect_attachments 傳入）
    raw_name = request.args.get("filename", "")
    filename = unquote(raw_name).strip() if raw_name else ""

    # 若 query string 沒帶（直接輸入 URL），再向 API 查一次
    if not filename:
        msg = service.users().messages().get(
            userId="me", id=message_id, format="full"
        ).execute()
        filename = _find_attachment_filename(msg["payload"], attachment_id) or "attachment"

    # 決定 MIME type（優先 mimetypes 推斷，再用 query string 傳入的）
    mime_type, _ = mimetypes.guess_type(filename)
    if not mime_type:
        mime_type = request.args.get("mime", "application/octet-stream")

    # RFC 5987：Content-Disposition 支援非 ASCII 檔名
    ascii_name = filename.encode("ascii", errors="replace").decode("ascii")
    encoded_name = quote(filename, safe="")
    disposition = f"attachment; filename=\"{ascii_name}\"; filename*=UTF-8''{encoded_name}"

    response = Response(
        file_data,
        mimetype=mime_type,
        headers={"Content-Disposition": disposition},
    )
    return response


def _find_attachment_filename(payload, attachment_id):
    """遞迴搜尋所有節點（含中間節點）以找到對應的檔名"""
    body = payload.get("body", {})
    if body.get("attachmentId") == attachment_id:
        return payload.get("filename") or None
    for part in payload.get("parts", []):
        result = _find_attachment_filename(part, attachment_id)
        if result:
            return result
    return None


# ── 交易明細解析 ────────────────────────────────────────────────

# 從 HTML entity 解碼
_HTML_ENTITIES = {"&nbsp;": " ", "&amp;": "&", "&lt;": "<", "&gt;": ">", "&quot;": '"', "&#39;": "'"}
_ENTITY_RE = re.compile(r"&[a-zA-Z]+;|&#\d+;")

def _decode_entities(s: str) -> str:
    def _replace(m):
        e = m.group(0)
        if e.startswith("&#"):
            return chr(int(e[2:-1]))
        return _HTML_ENTITIES.get(e, e)
    return _ENTITY_RE.sub(_replace, s)


def _td_text(td_html: str) -> str:
    """從單一 <td>...</td> 內容取出純文字並正規化"""
    # 去除所有標籤
    text = re.sub(r"<[^>]+>", "", td_html)
    text = _decode_entities(text)
    # 全形轉半形
    result = []
    for ch in text:
        cp = ord(ch)
        if 0xFF01 <= cp <= 0xFF5E:
            result.append(chr(cp - 0xFEE0))
        elif cp == 0x3000:
            result.append(" ")
        else:
            result.append(ch)
    return "".join(result).strip()


def _get_body_html(payload) -> str:
    """遞迴取出郵件原始 HTML body（優先）或純文字"""
    mime = payload.get("mimeType", "")
    if mime == "text/html":
        data = payload.get("body", {}).get("data", "")
        if data:
            return base64.urlsafe_b64decode(data).decode("utf-8", errors="replace")
    if mime == "text/plain":
        data = payload.get("body", {}).get("data", "")
        if data:
            return base64.urlsafe_b64decode(data).decode("utf-8", errors="replace")
    for prefer in ("text/html", "text/plain"):
        for part in payload.get("parts", []):
            if part.get("mimeType") == prefer:
                html = _get_body_html(part)
                if html:
                    return html
    for part in payload.get("parts", []):
        html = _get_body_html(part)
        if html:
            return html
    return ""


# 抓每個 <tr>...</tr> 區塊（含跨行）
_TR_RE  = re.compile(r"<tr\b[^>]*>(.*?)</tr>", re.S | re.I)
# 抓每個 <td>...</td> 區塊
_TD_RE  = re.compile(r"<td\b[^>]*>(.*?)</td>", re.S | re.I)

# 簡易判斷是否為資料列（非 header），第一欄含卡片關鍵字
_CARD_RE = re.compile(r"VISA|金融卡|信用卡|簽帳卡", re.I)
# 日期格式
_DATE_RE = re.compile(r"\d{4}[/\-]\d{2}[/\-]\d{2}")
# 時間格式
_TIME_RE = re.compile(r"\d{2}:\d{2}")

COLUMNS = ["用卡名稱", "卡號後4碼", "主/附卡", "授權日期", "授權時間", "授權地區", "授權金額(約當臺幣)", "商店名稱"]


def _parse_transactions(html: str) -> list[dict]:
    """直接從 HTML 解析 <tr><td> 結構，逐列比對欄位"""
    rows = []
    for tr_m in _TR_RE.finditer(html):
        tr_content = tr_m.group(1)
        tds = [_td_text(m.group(1)) for m in _TD_RE.finditer(tr_content)]

        # 需要至少 8 欄
        if len(tds) < 8:
            continue

        card_name = tds[0]
        last4     = tds[1]
        card_type = tds[2]
        auth_date = tds[3]
        auth_time = tds[4]
        auth_area = tds[5]
        amount    = tds[6].replace(",", "")
        merchant  = tds[7]

        # 以欄位格式驗證，排除 header 列
        if not _CARD_RE.search(card_name):
            continue
        if not _DATE_RE.fullmatch(auth_date):
            continue
        if not _TIME_RE.fullmatch(auth_time):
            continue
        if not amount.isdigit():
            continue

        rows.append({
            "用卡名稱":           card_name,
            "卡號後4碼":          last4,
            "主/附卡":            card_type,
            "授權日期":           auth_date.replace("-", "/"),
            "授權時間":           auth_time,
            "授權地區":           auth_area,
            "授權金額(約當臺幣)": amount,
            "商店名稱":           merchant,
        })
    return rows


@app.route("/transactions")
def transactions():
    creds = get_credentials()
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            save_credentials(creds)
        else:
            return redirect(url_for("login"))

    all_rows    = []
    email_count = 0
    with _db_connect() as conn:
        rows = conn.execute(
            """SELECT email_id, card_name, last4, card_type, auth_date, auth_time,
                      auth_area, amount, merchant, email_date
               FROM transactions
               ORDER BY auth_date DESC, auth_time DESC"""
        ).fetchall()
        seen_emails = set()
        for row in rows:
            seen_emails.add(row["email_id"])
            all_rows.append({
                "用卡名稱":           row["card_name"],
                "卡號後4碼":          row["last4"],
                "主/附卡":            row["card_type"],
                "授權日期":           row["auth_date"],
                "授權時間":           row["auth_time"],
                "授權地區":           row["auth_area"],
                "授權金額(約當臺幣)": row["amount"],
                "商店名稱":           row["merchant"],
                "_email_date":        row["email_date"],
            })
        email_count = len(seen_emails)

    total = sum(int(r["授權金額(約當臺幣)"]) for r in all_rows if r["授權金額(約當臺幣)"].isdigit())
    return render_template(
        "transactions.html",
        rows=all_rows, columns=COLUMNS, total=total, email_count=email_count,
    )


@app.route("/transactions/export")
def transactions_export():
    creds = get_credentials()
    if not creds or not creds.valid:
        return redirect(url_for("login"))

    with _db_connect() as conn:
        rows = conn.execute(
            "SELECT card_name,last4,card_type,auth_date,auth_time,auth_area,amount,merchant "
            "FROM transactions ORDER BY auth_date DESC, auth_time DESC"
        ).fetchall()

    all_rows = [
        {
            "用卡名稱":           r["card_name"],
            "卡號後4碼":          r["last4"],
            "主/附卡":            r["card_type"],
            "授權日期":           r["auth_date"],
            "授權時間":           r["auth_time"],
            "授權地區":           r["auth_area"],
            "授權金額(約當臺幣)": r["amount"],
            "商店名稱":           r["merchant"],
        }
        for r in rows
    ]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=COLUMNS, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(all_rows)
    return Response(
        "\ufeff" + buf.getvalue(), mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=transactions.csv"},
    )


# ── 台幣轉帳成功通知解析 ──────────────────────────────────────────────

# 抓 <th>...</th> 內文
_TH_RE = re.compile(r"<th\b[^>]*>(.*?)</th>", re.S | re.I)
# 抓單一 <td>...</td> 內文
_TD_SINGLE_RE = re.compile(r"<td\b[^>]*>(.*?)</td>", re.S | re.I)


def _parse_transfer_html(html: str, email_date: str, subject: str) -> dict | None:
    """從轉帳通知郵件 HTML 中提取 key-value 欄位"""
    record = {"_email_date": email_date, "_subject": subject}
    for tr_m in _TR_RE.finditer(html):
        tr_content = tr_m.group(1)
        th_m = _TH_RE.search(tr_content)
        td_m = _TD_SINGLE_RE.search(tr_content)
        if th_m and td_m:
            key = _td_text(th_m.group(1)).rstrip("：: \u3000")
            val = _td_text(td_m.group(1))
            if key:
                record[key] = val
    # 至少需要有交易時間才算有效
    if "交易時間" not in record:
        return None
    # 金額去除「元」與千分位
    for f in ("交易金額", "手續費"):
        if f in record:
            record[f] = record[f].replace("元", "").replace(",", "").strip()
    return record


@app.route("/transfers")
def transfers():
    creds = get_credentials()
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            save_credentials(creds)
        else:
            return redirect(url_for("login"))

    records = []
    with _db_connect() as conn:
        rows = conn.execute(
            "SELECT data_json FROM transfers ORDER BY email_date DESC"
        ).fetchall()
        for row in rows:
            records.append(json.loads(row["data_json"]))

    records.sort(key=lambda r: r.get("交易時間", ""), reverse=True)
    total_out = sum(
        int(r["交易金額"]) for r in records
        if r.get("交易金額", "").lstrip("-+").isdigit()
    )
    return render_template("transfers.html", records=records, total_out=total_out)


@app.route("/transfers/export")
def transfers_export():
    creds = get_credentials()
    if not creds or not creds.valid:
        return redirect(url_for("login"))

    with _db_connect() as conn:
        rows = conn.execute("SELECT data_json FROM transfers ORDER BY email_date DESC").fetchall()
    records = [json.loads(r["data_json"]) for r in rows]
    records.sort(key=lambda r: r.get("交易時間", ""), reverse=True)

    fieldnames = ["交易時間", "交易金額", "手續費",
                  "轉出銀行", "轉出帳號", "轉入銀行", "轉入帳號", "收款戶名",
                  "轉出帳戶存摺摘要", "轉入帳戶存摺摘要", "交易說明"]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(records)
    return Response(
        "\ufeff" + buf.getvalue(), mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=transfers.csv"},
    )


# ── 彰化銀行數位存款帳戶入帳通知解析 ──────────────────────────────

_CHW_SPAN_RE = re.compile(r'<span\b[^>]*Repeater_DetailTemplate2_Label_Line_\d+[^>]*>(.*?)</span>', re.S | re.I)
_CHW_TAG_RE  = re.compile(r'<[^>]+>')


def _chw_text(html_frag: str) -> str:
    """移除內部 HTML 標籤"""
    return _CHW_TAG_RE.sub('', html_frag).strip()


def _parse_changhwa_html(html: str, email_date: str) -> dict | None:
    """解析彰化銀行入帳通知 HTML，回傳結構化 dict 或 None"""
    spans = [_chw_text(m.group(1)) for m in _CHW_SPAN_RE.finditer(html)]
    rec = {
        "txn_datetime": "", "txn_type": "", "txn_memo": "",
        "txn_date": "", "book_date": "",
        "to_account": "", "from_account": "",
        "amount": "", "currency": "",
    }
    for span in spans:
        # Line 1: "2026/03/31 03:41:10 您的數位存款帳戶金額異動"
        m = re.match(r'(\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})\s', span)
        if m:
            rec["txn_datetime"] = m.group(1)
        # Line 3: "交易資訊：轉入交易  (薪水)" or "交易資訊：轉入交易"
        m = re.match(r'交易資訊[：:](.+)', span)
        if m:
            info = m.group(1).strip()
            memo_m = re.search(r'\((.+?)\)', info)
            rec["txn_type"] = re.sub(r'\s*\(.+?\)', '', info).strip()
            rec["txn_memo"] = memo_m.group(1) if memo_m else ""
        # Line 4: "交易日期： 2026/03/31"
        m = re.match(r'交易日期[：:]\s*(\S+)', span)
        if m:
            rec["txn_date"] = m.group(1)
        # Line 5: "記帳日期： 2026/03/31"
        m = re.match(r'記帳日期[：:]\s*(\S+)', span)
        if m:
            rec["book_date"] = m.group(1)
        # Line 6: "轉入帳號：xxxx"
        m = re.match(r'轉入帳號[：:]\s*(\S+)', span)
        if m:
            rec["to_account"] = m.group(1)
        # Line 7: "轉出帳號：xxxx"
        m = re.match(r'轉出帳號[：:]\s*(\S+)', span)
        if m:
            rec["from_account"] = m.group(1)
        # Line 8: "交易金額：TWD         15,994.00"
        m = re.match(r'交易金額[：:]\s*(\S+)\s+([\d,]+\.\d+)', span)
        if m:
            rec["currency"] = m.group(1)
            rec["amount"]   = m.group(2).replace(',', '')
    # 至少需要日期或金額才算有效
    if not rec["txn_date"] and not rec["amount"]:
        return None
    return rec


@app.route("/changhwa")
def changhwa():
    creds = get_credentials()
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            save_credentials(creds)
        else:
            return redirect(url_for("login"))

    records = []
    with _db_connect() as conn:
        rows = conn.execute(
            """SELECT txn_datetime, txn_type, txn_memo, txn_date, book_date,
                      to_account, from_account, amount, currency, email_date
               FROM changhwa_deposits ORDER BY txn_date DESC, txn_datetime DESC"""
        ).fetchall()
        for row in rows:
            records.append(dict(row))
    return render_template("changhwa.html", records=records)


@app.route("/changhwa/export")
def changhwa_export():
    creds = get_credentials()
    if not creds or not creds.valid:
        return redirect(url_for("login"))

    with _db_connect() as conn:
        rows = conn.execute(
            "SELECT txn_datetime,txn_type,txn_memo,txn_date,book_date,"
            "to_account,from_account,amount,currency,email_date "
            "FROM changhwa_deposits ORDER BY txn_date DESC, txn_datetime DESC"
        ).fetchall()

    fieldnames = ["txn_datetime","txn_type","txn_memo","txn_date","book_date",
                  "to_account","from_account","amount","currency","email_date"]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows([dict(r) for r in rows])
    return Response(
        "\ufeff" + buf.getvalue(), mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=changhwa_deposits.csv"},
    )


# ── 有價證券買賣對帳單 PDF 解析 ─────────────────────────────────────

# 民國年日期，例：115/01/08
_ROC_DATE_RE  = re.compile(r"^(\d{3}/\d{2}/\d{2})$")
# 金額（含正負號、千分位）
_AMOUNT_RE    = re.compile(r"^[+\-]?[\d,]+$")
# 單價（小數點數字）
_PRICE_RE     = re.compile(r"^[\d,]+\.\d+$")

SEC_TX_COLS = [
    "交易日期", "交易類別", "證券名稱",
    "單價", "股數", "成交金額",
    "手續費", "證交稅",
    "資自備款/擔保價款", "融資金額/券保證金",
    "資券利息", "券手續費", "代扣稅款",
    "證所稅/健保費", "本公司淨收+/本公司淨付-",
]

SEC_TOTAL_COLS = [
    "總計股數", "總計成交金額", "總計手續費", "總計證交稅",
    "總計資自備款/擔保價款", "總計融資金額/券保證金",
    "總計資券利息", "總計券手續費", "總計代扣稅款",
    "總計證所稅/健保費", "總計本公司淨收+/本公司淨付-",
]


def _clean_num(s: str) -> str:
    """移除千分位逗號，保留正負號"""
    return s.replace(",", "")


def _roc_to_ad(roc: str) -> str:
    """民國年 -> 西元年，例：115/01/08 -> 2026/01/08"""
    try:
        y, m, d = roc.split("/")
        return f"{int(y)+1911}/{m}/{d}"
    except Exception:
        return roc


def _fetch_pdf_bytes(service, message_id: str, attachment_id: str) -> bytes:
    att = service.users().messages().attachments().get(
        userId="me", messageId=message_id, id=attachment_id
    ).execute()
    return base64.urlsafe_b64decode(att["data"].encode("UTF-8"))


def _parse_securities_pdf(pdf_bytes: bytes, email_date: str) -> dict:
    """
    用 PyMuPDF 讀取對帳單 PDF，以 regex 提煉交易明細與統計資訊。
    回傳 dict: { transactions, totals, notes, email_date }
    """
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    if doc.is_encrypted:
        result = doc.authenticate(SECURITIES_PDF_PWD)
        if result == 0:
            doc.close()
            raise ValueError("PDF 密碼錯誤或無法解密，請確認 .env 中的 SECURITIES_PDF_PASSWORD")
    lines = []
    for page in doc:
        text = page.get_text("text")
        for ln in text.splitlines():
            ln = ln.strip()
            if ln:
                lines.append(ln)
    doc.close()

    transactions = []
    totals       = {}
    notes        = {}

    # ── 定位標題行，找出欄位順序 ─────────────────────────────────
    FIELD_ORDER = [
        "交易日期", "交易類別", "證券名稱",
        "單價", "股數", "成交金額",
        "手續費", "證交稅",
        "資自備款",  # 可能含「擔保價款」在同欄
        "融資金額",  # 可能含「券保證金」
        "資券利息", "券手續費", "代扣稅款",
        "證所稅",   # 可能含「健保費」
        "本公司淨", # 淨收/淨付
    ]

    # ── 找所有交易區塊 ────────────────────────────────────────────
    # 結構固定：
    #   line[i]      = 民國日期（交易日期）
    #   line[i+1]    = 交易類別（固定 1 行）
    #   line[i+2...) = 證券名稱（1 行或多行，直到第一個純數字行）
    #   純數字行起   = 單價、股數、成交金額、手續費…共 12 個數值欄位

    def _is_numeric_field(s: str) -> bool:
        """判斷是否為數值欄位（整數/小數/含正負號/含千分位）"""
        return bool(re.fullmatch(r"[+\-]?[\d,]+(\.\d+)?", s.strip()))

    i = 0
    while i < len(lines):
        m = _ROC_DATE_RE.match(lines[i])
        if m:
            date_roc = m.group(1)
            j = i + 1

            # 跳過空行／標題行，取交易類別（固定 1 個值行）
            category = ""
            while j < len(lines) and not category:
                val = lines[j].strip()
                j += 1
                if val and not _is_header_line(val) and not _is_numeric_field(val):
                    category = val

            # 收集證券名稱（多行，直到第一個純數字行）
            name_parts = []
            while j < len(lines):
                val = lines[j].strip()
                if not val or _is_header_line(val):
                    j += 1
                    continue
                if _is_numeric_field(val):
                    break          # 數字行 → 名稱結束，不推進 j
                name_parts.append(val)
                j += 1
            security_name = " ".join(name_parts)

            # 收集 12 個數值欄位：單價、股數、成交金額、手續費、
            # 證交稅、資自備款/擔保、融資/券保證金、資券利息、
            # 券手續費、代扣稅款、證所稅/健保費、本公司淨收付
            num_fields = []
            while j < len(lines) and len(num_fields) < 12:
                val = lines[j].strip()
                j += 1
                if val and _is_numeric_field(val):
                    num_fields.append(_clean_num(val))

            if security_name and len(num_fields) == 12:
                transactions.append({
                    "交易日期":                _roc_to_ad(date_roc),
                    "交易類別":                category,
                    "證券名稱":                security_name,
                    "單價":                   num_fields[0],
                    "股數":                   num_fields[1],
                    "成交金額":                num_fields[2],
                    "手續費":                 num_fields[3],
                    "證交稅":                 num_fields[4],
                    "資自備款/擔保價款":         num_fields[5],
                    "融資金額/券保證金":         num_fields[6],
                    "資券利息":                num_fields[7],
                    "券手續費":                num_fields[8],
                    "代扣稅款":                num_fields[9],
                    "證所稅/健保費":            num_fields[10],
                    "本公司淨收+/本公司淨付-":   num_fields[11],
                })
            i = j
            continue

        # ── 總計區塊 ─────────────────────────────────────────────
        if lines[i].startswith("總計"):
            total_vals = []
            j = i + 1
            while j < len(lines) and len(total_vals) < 11:
                val = lines[j].strip()
                if val and _AMOUNT_RE.match(val.replace(",", "").lstrip("+-")):
                    total_vals.append(_clean_num(val))
                j += 1
            if len(total_vals) == 11:
                for k, col in enumerate(SEC_TOTAL_COLS):
                    totals[col] = total_vals[k]
            i = j
            continue

        # ── 附註（本月 xxx 合計）────────────────────────────────────
        note_m = re.search(r"本月(.+?)合計金額為[\s\n]*([\d,]+)", lines[i])
        if note_m:
            notes[f"本月{note_m.group(1)}合計金額"] = _clean_num(note_m.group(2))

        # ── 客戶訊息（折讓金額）─────────────────────────────────────
        disc_m = re.search(r"一般折讓金額[^:：]*[:：]\$?([\d,]+)", lines[i])
        if disc_m:
            notes["一般折讓金額"] = _clean_num(disc_m.group(1))
        promo_m = re.search(r"促銷折讓金額[^:：]*[:：]\$?([\d,]+)", lines[i])
        if promo_m:
            notes["促銷折讓金額"] = _clean_num(promo_m.group(1))

        # ── 跨行合併附註（下一行是數值）────────────────────────────────
        if i + 1 < len(lines):
            two = lines[i] + lines[i+1]
            note_m2 = re.search(r"本月(.+?)合計金額為[\s]*([\d,]+)", two)
            if note_m2:
                notes[f"本月{note_m2.group(1)}合計金額"] = _clean_num(note_m2.group(2))

        i += 1

    return {
        "transactions": transactions,
        "totals":       totals,
        "notes":        notes,
        "email_date":   email_date,
    }


def _is_header_line(val: str) -> bool:
    """判斷是否為欄位標題列（僅比對已知標題關鍵字，不以純中文判斷）"""
    KNOWN_HEADERS = {
        "交易日期", "交易類別", "證券名稱", "單價", "股數", "成交金額",
        "手續費", "證交稅", "資自備款", "擔保價款", "融資金額", "券保證金",
        "資券利息", "券手續費", "代扣稅款", "證所稅", "健保費",
        "本公司淨收", "本公司淨付", "總計",
    }
    return any(h in val for h in KNOWN_HEADERS) and not _AMOUNT_RE.match(val.replace(",", "").lstrip("+-"))


def _get_service():
    creds = get_credentials()
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            save_credentials(creds)
        else:
            return None, redirect(url_for("login"))
    return build("gmail", "v1", credentials=creds), None


@app.route("/securities")
def securities():
    creds = get_credentials()
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            save_credentials(creds)
        else:
            return redirect(url_for("login"))

    reports = []
    with _db_connect() as conn:
        rows = conn.execute(
            "SELECT subject, filename, email_date, data_json FROM securities ORDER BY email_date DESC"
        ).fetchall()
        for row in rows:
            data = json.loads(row["data_json"])
            data["subject"]    = row["subject"]
            data["filename"]   = row["filename"]
            data["email_date"] = row["email_date"]
            reports.append(data)

    return render_template(
        "securities.html", reports=reports, tx_cols=SEC_TX_COLS, total_cols=SEC_TOTAL_COLS
    )


def _collect_pdf_attachments(payload, message_id, result):
    """遞迴收集 PDF 附檔"""
    if "parts" in payload:
        for part in payload["parts"]:
            _collect_pdf_attachments(part, message_id, result)
    else:
        filename = payload.get("filename", "")
        body     = payload.get("body", {})
        att_id   = body.get("attachmentId")
        mime     = payload.get("mimeType", "")
        if att_id and (mime == "application/pdf" or filename.lower().endswith(".pdf")):
            result.append({"filename": filename, "attachment_id": att_id})


@app.route("/securities/export")
def securities_export():
    creds = get_credentials()
    if not creds or not creds.valid:
        return redirect(url_for("login"))

    all_rows = []
    with _db_connect() as conn:
        rows = conn.execute(
            "SELECT subject, data_json FROM securities ORDER BY email_date DESC"
        ).fetchall()
        for row in rows:
            data = json.loads(row["data_json"])
            for tx in data.get("transactions", []):
                tx["來源郵件"] = row["subject"]
                all_rows.append(tx)

    fieldnames = ["來源郵件"] + SEC_TX_COLS
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(all_rows)
    return Response(
        "\ufeff" + buf.getvalue(), mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=securities.csv"},
    )


@app.route("/.well-known/appspecific/com.chrome.devtools.json")
def chrome_devtools():
    return Response("{}", mimetype="application/json")


if __name__ == "__main__":
    app.run(debug=True, port=5000)
