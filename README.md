# Gmail 關鍵字郵件查詢工具

Flask Web App，使用 Gmail API 依自訂關鍵字搜尋郵件與附件。

---

## 目錄結構

```
WMMG/
├── app.py
├── credentials.json        ← 需自行從 Google Cloud 下載（勿上傳）
├── .env                    ← 關鍵字設定（勿上傳）
├── requirements.txt
├── attachments/            ← 自動建立
└── templates/
    ├── index.html
    └── messages.html
```

---

## 步驟一：建立 Google Cloud 專案與 OAuth 憑證

1. 前往 [Google Cloud Console](https://console.cloud.google.com/)
2. 建立新專案（或選擇現有專案）
3. 啟用 **Gmail API**：  
   APIs & Services → Library → 搜尋「Gmail API」→ 啟用
4. 建立 OAuth 2.0 憑證：  
   APIs & Services → Credentials → Create Credentials → OAuth client ID
   - Application type：**Web application**
   - Authorized redirect URIs 加入：`http://127.0.0.1:5000/oauth2callback`
5. 下載 JSON 憑證，重新命名為 `credentials.json`，放在專案根目錄

> 若帳號為測試階段，請在 OAuth consent screen → Test users 加入您的 Gmail 帳號

---

## 步驟二：設定關鍵字（.env）

複製 `.env` 並填入您的搜尋條件，語法為 Gmail 搜尋語法：

```
SEARCH_QUERY=("關鍵字A" OR "關鍵字B")
```

> `.env` 已列入 `.gitignore`，不會被提交。

---

## 步驟三：安裝相依套件

```bash
pip install -r requirements.txt
```

---

## 步驟四：執行

```bash
python app.py
```

開啟瀏覽器前往 [http://127.0.0.1:5000](http://127.0.0.1:5000)

---

## 功能說明

| 功能 | 說明 |
|------|------|
| Google OAuth 登入 | 使用 OAuth 2.0 授權存取 Gmail |
| 關鍵字搜尋 | 依 `.env` 中設定的關鍵字搜尋郵件，最多顯示 50 封 |
| 附件下載 | 每封郵件的附件可直接點擊下載 |
| 登出 | 清除 Session |
