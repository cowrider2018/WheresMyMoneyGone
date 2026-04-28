import os
import base64
import hashlib
import secrets
from flask import Flask, redirect, request, session, url_for, render_template, send_file
from google_auth_oauthlib.flow import Flow
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from dotenv import load_dotenv
import io

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


if __name__ == "__main__":
    app.run(debug=True, port=5000)
