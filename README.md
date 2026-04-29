# 郵件帳務管理工具（WMMG）

Flask Web App，透過 Gmail API 自動彙整信用卡消費明細、台幣轉帳通知、彰化銀行入帳通知，以及有價證券買賣對帳單，並提供帳務總覽與 CSV 匯出功能。

---

## 目錄結構

```
WMMG/
├── app.py                  ← 主程式
├── credentials.json        ← Google OAuth 憑證（勿上傳）
├── token.json              ← 自動產生的 OAuth Token（勿上傳）
├── .env                    ← 個人化設定（勿上傳）
├── requirements.txt
├── attachments/            ← 附件暫存目錄（自動建立）
├── wmmg.db                 ← SQLite 資料庫（自動建立,勿上傳）
└── templates/
    ├── index.html
    ├── messages.html
    ├── transactions.html
    ├── transfers.html
    ├── securities.html
    └── changhwa.html
```

---

## 功能一覽

| 功能頁面 | 說明 |
|---|---|
| 首頁帳務概覽 | 彙整各類收支，計算綜合淨損益 |
| 郵件查詢 | 依關鍵字搜尋 Gmail 郵件與附件 |
| 消費明細解析 | 解析信用卡消費通知，列出每筆授權 |
| 台幣轉帳通知彙整 | 彙整轉帳成功通知，計算出帳總額 |
| 彰化銀行入帳通知 | 解析彰化銀行數位存款入帳通知 |
| 有價證券對帳單解析 | 解析 PDF 對帳單，顯示每份的我方損益 |

所有資料儲存於本地 SQLite，每次操作讀取快取，點「更新資料」才透過 Gmail API 增量同步。

---

## 快速開始

### 步驟一：建立 Google Cloud 專案與 OAuth 憑證

1. 前往 [Google Cloud Console](https://console.cloud.google.com/)
2. 建立新專案（或選擇現有專案）
3. 啟用 **Gmail API**：APIs & Services → Library → 搜尋「Gmail API」→ 啟用
4. 建立 OAuth 2.0 憑證：  
   APIs & Services → Credentials → Create Credentials → OAuth client ID
   - Application type：**Web application**
   - Authorized redirect URIs：`http://127.0.0.1:5000/oauth2callback`
5. 下載 JSON 憑證，重命名為 `credentials.json`，放在專案根目錄

> 測試階段請在 OAuth consent screen → Test users 加入您的 Gmail 帳號

---

### 步驟二：建立 `.env` 設定檔

在專案根目錄建立 `.env`，內容範本如下：

```env
# ── 總搜尋條件 ──────────────────────────────────────────────────────
# 此查詢用於「郵件查詢」頁面及首頁，使用 Gmail 搜尋語法
# 建議將下方各通知的主旨全部包含進來
SEARCH_QUERY=("第一銀行簽帳金融卡消費彙整通知" OR "有價證券買賣對帳單" OR "第e行動-台幣轉帳成功通知" OR "【彰化銀行數位存款帳戶】入帳通知")

# ── 消費明細 ────────────────────────────────────────────────────────
# 信用卡 / 金融卡消費彙整通知的郵件主旨（完整比對）
TRANSACTION_QUERY=第一銀行簽帳金融卡消費彙整通知

# ── 台幣轉帳通知 ────────────────────────────────────────────────────
# 台幣轉帳成功通知的郵件主旨
TRANSFER_QUERY=第e行動-台幣轉帳成功通知

# ── 有價證券對帳單 ──────────────────────────────────────────────────
# 對帳單郵件主旨（需含 PDF 附件）
SECURITIES_QUERY=有價證券買賣對帳單

# ── 彰化銀行入帳通知 ────────────────────────────────────────────────
# 彰化銀行數位存款帳戶入帳通知主旨
CHANGHWA_QUERY=【彰化銀行數位存款帳戶】入帳通知

# 對帳單 PDF 的開啟密碼（若無密碼請留空）
SECURITIES_PDF_PASSWORD=
```

> `.env` 已列入 `.gitignore`，不會被提交至版控。

---

### 步驟三：建立 Python 虛擬環境並安裝相依套件

```bash
python -m venv .venv

# Windows
.venv\Scripts\activate

# macOS / Linux
source .venv/bin/activate

pip install -r requirements.txt
```

---

### 步驟四：啟動應用程式

```bash
python app.py
```

開啟瀏覽器前往 [http://127.0.0.1:5000](http://127.0.0.1:5000)，點選「使用 Google 帳號登入」完成授權。

---

## 資料同步說明

- 點首頁「**更新資料**」按鈕觸發增量同步，已存在的郵件不會重複抓取。
- 同步期間按鈕鎖定，完成後自動刷新頁面。
- 點「**清除所有資料**」可清空 SQLite 所有紀錄（不影響 Gmail）。

---

## 帳務損益計算邏輯

| 類型 | 計算方式 |
|---|---|
| 信用卡消費 | 負（支出） |
| 台幣轉帳 | 負（出帳） |
| 彰化銀行入帳 | 正（收入） |
| 有價證券損益 | 「本公司淨付」為我方收入（正）、「本公司淨收」為我方支出（負） |
| **綜合淨損益** | `入帳 + 證券損益 − 消費 − 轉帳` |

---

## 安全注意事項

- `credentials.json`、`token.json`、`.env` 皆不應上傳至公開 repo
- 本工具預設以 `OAUTHLIB_INSECURE_TRANSPORT=1` 允許本機 HTTP 開發，**請勿部署至公開伺服器**
