import os
import re
import base64
import hashlib
import secrets
import mimetypes
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
SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
]

# 實際關鍵字從 .env 載入，不寫死於程式碼中
SEARCH_QUERY = os.environ.get("SEARCH_QUERY", "")
SEARCH_LABEL = "關鍵字"
TRANSACTION_QUERY = os.environ.get("TRANSACTION_QUERY", "")
SECURITIES_QUERY    = os.environ.get("SECURITIES_QUERY", "")
SECURITIES_PDF_PWD  = os.environ.get("SECURITIES_PDF_PASSWORD", "")
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
    service, err = _get_service()
    if err:
        return err

    results = service.users().messages().list(
        userId="me",
        q=f'subject:"{SECURITIES_QUERY}" has:attachment filename:pdf',
        maxResults=50,
    ).execute()
    messages_list = results.get("messages", [])

    reports = []  # list of parsed report dicts

    for msg_ref in messages_list:
        msg = service.users().messages().get(
            userId="me", id=msg_ref["id"], format="full"
        ).execute()
        headers    = {h["name"]: h["value"] for h in msg["payload"].get("headers", [])}
        email_date = headers.get("Date", "")
        subject    = headers.get("Subject", "")

        # 找 PDF 附檔
        pdf_attachments = []
        _collect_pdf_attachments(msg["payload"], msg_ref["id"], pdf_attachments)

        for att in pdf_attachments:
            try:
                pdf_bytes = _fetch_pdf_bytes(service, msg_ref["id"], att["attachment_id"])
                report = _parse_securities_pdf(pdf_bytes, email_date)
                report["subject"]  = subject
                report["filename"] = att["filename"]
                reports.append(report)
            except Exception as e:
                reports.append({
                    "subject":      subject,
                    "filename":     att["filename"],
                    "email_date":   email_date,
                    "transactions": [],
                    "totals":       {},
                    "notes":        {},
                    "error":        str(e),
                })

    return render_template(
        "securities.html",
        reports=reports,
        tx_cols=SEC_TX_COLS,
        total_cols=SEC_TOTAL_COLS,
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
    service, err = _get_service()
    if err:
        return err

    results = service.users().messages().list(
        userId="me",
        q=f'subject:"{SECURITIES_QUERY}" has:attachment filename:pdf',
        maxResults=50,
    ).execute()
    messages_list = results.get("messages", [])

    all_rows = []
    for msg_ref in messages_list:
        msg = service.users().messages().get(
            userId="me", id=msg_ref["id"], format="full"
        ).execute()
        headers    = {h["name"]: h["value"] for h in msg["payload"].get("headers", [])}
        email_date = headers.get("Date", "")
        subject    = headers.get("Subject", "")
        pdf_attachments = []
        _collect_pdf_attachments(msg["payload"], msg_ref["id"], pdf_attachments)
        for att in pdf_attachments:
            try:
                pdf_bytes = _fetch_pdf_bytes(service, msg_ref["id"], att["attachment_id"])
                report = _parse_securities_pdf(pdf_bytes, email_date)
                for tx in report["transactions"]:
                    tx["來源郵件"] = subject
                    all_rows.append(tx)
            except Exception:
                pass

    fieldnames = ["來源郵件"] + SEC_TX_COLS
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(all_rows)

    return Response(
        "\ufeff" + buf.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=securities.csv"},
    )


if __name__ == "__main__":
    app.run(debug=True, port=5000)
