import os
import re
import base64
import hashlib
import secrets
from flask import Flask, redirect, request, session, url_for, render_template, send_file, Response
from google_auth_oauthlib.flow import Flow
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from dotenv import load_dotenv
import io
import csv

load_dotenv()

app = Flask(__name__)
app.secret_key = os.urandom(24)

# 允許在本機開發使用 HTTP (不需要 HTTPS)
os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

CLIENT_SECRETS_FILE = "credentials.json"
SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
]

# 實際關鍵字從 .env 載入，不寫死於程式碼中
SEARCH_QUERY = os.environ.get("SEARCH_QUERY", "")
SEARCH_LABEL = "關鍵字"
TRANSACTION_QUERY = os.environ.get("TRANSACTION_QUERY", "第一銀行簽帳金融卡消費彙整通知")
DOWNLOAD_DIR = "attachments"

os.makedirs(DOWNLOAD_DIR, exist_ok=True)


def get_credentials():
    creds = None
    if "credentials" in session:
        creds = Credentials(**session["credentials"])
    return creds


def save_credentials(creds):
    session["credentials"] = {
        "token": creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri": creds.token_uri,
        "client_id": creds.client_id,
        "client_secret": creds.client_secret,
        "scopes": creds.scopes,
    }


@app.route("/")
def index():
    creds = get_credentials()
    if not creds or not creds.valid:
        return render_template("index.html", authenticated=False)
    return render_template("index.html", authenticated=True)


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
    return redirect(url_for("messages"))


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

    service = build("gmail", "v1", credentials=creds)

    # 取得符合關鍵字的郵件清單
    results = service.users().messages().list(
        userId="me",
        q=SEARCH_QUERY,
        maxResults=50,
    ).execute()

    messages_list = results.get("messages", [])
    emails = []

    for msg_ref in messages_list:
        msg = service.users().messages().get(
            userId="me",
            id=msg_ref["id"],
            format="full",
        ).execute()

        headers = {h["name"]: h["value"] for h in msg["payload"].get("headers", [])}
        subject = headers.get("Subject", "(無主旨)")
        sender = headers.get("From", "(未知寄件者)")
        date = headers.get("Date", "")
        snippet = msg.get("snippet", "")

        # 收集附件資訊
        attachments = []
        _collect_attachments(msg["payload"], msg_ref["id"], attachments)

        emails.append({
            "id": msg_ref["id"],
            "subject": subject,
            "sender": sender,
            "date": date,
            "snippet": snippet,
            "attachments": attachments,
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

    # 取得檔案名稱
    msg = service.users().messages().get(
        userId="me", id=message_id, format="full"
    ).execute()
    filename = _find_attachment_filename(msg["payload"], attachment_id) or "attachment"

    return send_file(
        io.BytesIO(file_data),
        download_name=filename,
        as_attachment=True,
    )


def _find_attachment_filename(payload, attachment_id):
    if "parts" in payload:
        for part in payload["parts"]:
            result = _find_attachment_filename(part, attachment_id)
            if result:
                return result
    else:
        body = payload.get("body", {})
        if body.get("attachmentId") == attachment_id:
            return payload.get("filename", "attachment")
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

    service = build("gmail", "v1", credentials=creds)

    results = service.users().messages().list(
        userId="me",
        q=f'subject:"{TRANSACTION_QUERY}"',
        maxResults=100,
    ).execute()
    messages_list = results.get("messages", [])

    all_rows = []
    email_count = 0

    for msg_ref in messages_list:
        msg = service.users().messages().get(
            userId="me", id=msg_ref["id"], format="full"
        ).execute()
        headers = {h["name"]: h["value"] for h in msg["payload"].get("headers", [])}
        email_date = headers.get("Date", "")

        html = _get_body_html(msg["payload"])
        rows = _parse_transactions(html)

        if rows:
            email_count += 1
            for r in rows:
                r["_email_date"] = email_date  # 供排序用，不顯示
            all_rows.extend(rows)

    # 依授權日期、授權時間排序（新→舊）
    all_rows.sort(key=lambda r: (r["授權日期"], r["授權時間"]), reverse=True)

    # 計算總金額
    total = sum(int(r["授權金額(約當臺幣)"]) for r in all_rows if r["授權金額(約當臺幣)"].isdigit())

    return render_template(
        "transactions.html",
        rows=all_rows,
        columns=COLUMNS,
        total=total,
        email_count=email_count,
    )


@app.route("/transactions/export")
def transactions_export():
    """匯出 CSV"""
    creds = get_credentials()
    if not creds or not creds.valid:
        return redirect(url_for("login"))

    service = build("gmail", "v1", credentials=creds)
    results = service.users().messages().list(
        userId="me",
        q=f'subject:"{TRANSACTION_QUERY}"',
        maxResults=100,
    ).execute()
    messages_list = results.get("messages", [])

    all_rows = []
    for msg_ref in messages_list:
        msg = service.users().messages().get(
            userId="me", id=msg_ref["id"], format="full"
        ).execute()
        html = _get_body_html(msg["payload"])
        all_rows.extend(_parse_transactions(html))

    all_rows.sort(key=lambda r: (r["授權日期"], r["授權時間"]), reverse=True)

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=COLUMNS, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(all_rows)

    return Response(
        "\ufeff" + buf.getvalue(),   # BOM for Excel UTF-8
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=transactions.csv"},
    )


if __name__ == "__main__":
    app.run(debug=True, port=5000)
