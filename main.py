"""
Email Digest Bot: Gmail → Excel parsing → compact Telegram digest.
No LLM — just VIN + what was found, compact format.
"""
import imaplib
import email
import os
import json
import urllib.request
from datetime import datetime
from email.header import decode_header
from collections import defaultdict

from dotenv import load_dotenv
import openpyxl

load_dotenv()

GMAIL_USER = os.getenv("GMAIL_USER")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD")
IMAP_SERVER = "imap.gmail.com"
SENDER_FILTER = "no-reply@business.auto.ru"

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

DOWNLOAD_DIR = os.path.join(os.path.dirname(__file__), "downloads")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)


def decode_header_value(value):
    if not value:
        return ""
    parts = decode_header(value)
    result = []
    for part, encoding in parts:
        if isinstance(part, bytes):
            result.append(part.decode(encoding or "utf-8", errors="replace"))
        else:
            result.append(part)
    return " ".join(result)


def extract_url(val):
    """Extract URL from =HYPERLINK(...) formula."""
    if isinstance(val, str) and val.startswith("=HYPERLINK"):
        parts = val.split('"')
        if len(parts) >= 2:
            return parts[1]
    return val if isinstance(val, str) else ""


def extract_offer_id(url):
    """Extract offer ID from auto.ru URL like '1131420679-5346d8a6'."""
    if not url:
        return ""
    # URL: https://auto.ru/cars/used/sale/brand/model/1131420679-5346d8a6/
    parts = url.rstrip("/").split("/")
    if parts:
        last = parts[-1]
        if "-" in last and any(c.isdigit() for c in last):
            return last
    return ""


def make_mobile_link(url):
    """Convert auto.ru URL to m.auto.ru."""
    if not url:
        return ""
    return url.replace("https://auto.ru/", "https://m.auto.ru/")


SALON_SHORT = {
    "июль екб совхозная": "EKT",
    "июль екб металлургов": "EKT",
    "июль екб базовый": "EKT",
    "июль екб": "EKT",
    "июль екб  рынок год": "EKT",
    "июль екб выезд": "EKT",
    "июль члб копейское": "CHL",
    "июль члб": "CHL",
    "июль крд": "KRD",
    "omoda новые": "OMODA",
}


def short_salon(name):
    """Convert salon name to short code."""
    if not name:
        return "???"
    lower = name.lower().strip()
    for key, code in SALON_SHORT.items():
        if key in lower:
            return code
    # Fallback: first 3 chars uppercase
    return name.split()[0][:3].upper() if name else "???"


# ── Gmail ──────────────────────────────────────────────────────────

def fetch_today_emails():
    print(f"[Gmail] Connecting as {GMAIL_USER}...")
    mail = imaplib.IMAP4_SSL(IMAP_SERVER)
    mail.login(GMAIL_USER, GMAIL_APP_PASSWORD)
    mail.select("inbox")

    today = datetime.now().strftime("%d-%b-%Y")
    status, messages = mail.search(
        None, f'(FROM "{SENDER_FILTER}" SINCE {today})'
    )

    if status != "OK" or not messages[0]:
        print("[Gmail] No emails found today")
        mail.logout()
        return []

    email_ids = messages[0].split()
    print(f"[Gmail] Found {len(email_ids)} emails today")

    results = []
    for eid in email_ids:
        status, data = mail.fetch(eid, "(RFC822)")
        if status != "OK":
            continue

        msg = email.message_from_bytes(data[0][1])
        subject = decode_header_value(msg["Subject"])
        files = []

        for part in msg.walk():
            filename = part.get_filename()
            if not filename:
                continue
            decoded_name = decode_header_value(filename)
            if decoded_name.endswith((".xlsx", ".xls", ".csv")):
                safe_name = decoded_name.replace("/", "_").replace("\\", "_")
                filepath = os.path.join(DOWNLOAD_DIR, safe_name)
                with open(filepath, "wb") as f:
                    f.write(part.get_payload(decode=True))
                files.append(filepath)

        if files:
            results.append((subject, files))

    mail.logout()
    return results


# ── Excel → compact format ─────────────────────────────────────────

def col_index(headers, *names):
    """Find column index by name (case-insensitive)."""
    h_lower = {str(h).lower(): i for i, h in enumerate(headers) if h}
    for name in names:
        if name in h_lower:
            return h_lower[name]
    return None


def get(row, idx):
    """Safe get from row by index."""
    if idx is not None and idx < len(row):
        return row[idx]
    return None


def format_not_purchased(filepath):
    """Format 'Не выкупленные' — compact: offer_id / SALON used BRAND MODEL / link."""
    wb = openpyxl.load_workbook(filepath, read_only=True)
    lines = []

    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            continue

        headers = rows[0]
        data = rows[1:]
        if not data:
            continue

        if "совпадения" in sheet_name.lower():
            i_brand = col_index(headers, "марка")
            i_model = col_index(headers, "модель")
            i_salon = col_index(headers, "автосалон")
            i_link = col_index(headers, "ссылка на объявление")

            # Deduplicate by offer link
            seen = set()
            for row in data:
                url = extract_url(get(row, i_link) or "")
                offer_id = extract_offer_id(url)
                if not offer_id or offer_id in seen:
                    continue
                seen.add(offer_id)

                salon = short_salon(get(row, i_salon))
                brand = str(get(row, i_brand) or "").upper()
                model = str(get(row, i_model) or "").upper().replace(" ", "_")
                link = make_mobile_link(url)

                lines.append(f"{offer_id}/")
                lines.append(f"{salon} used {brand} {model}")
                lines.append(link)

            if lines:
                lines.insert(0, f"🔍 Не выкупленные: {len(seen)} авто\n")

    wb.close()
    return "\n".join(lines)


def format_back_on_sale(filepath):
    """Format 'Снова в продаже' — compact: offer_id / SALON used BRAND MODEL / link."""
    wb = openpyxl.load_workbook(filepath, read_only=True)
    lines = []

    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            continue

        headers = rows[0]
        data = rows[1:]
        if not data:
            continue

        if "найденные" in sheet_name.lower():
            i_brand = col_index(headers, "марка")
            i_model = col_index(headers, "модель")
            i_salon = col_index(headers, "автосалон")
            i_link = col_index(headers, "ссылка на объявление")

            seen = set()
            for row in data:
                url = extract_url(get(row, i_link) or "")
                offer_id = extract_offer_id(url)
                if not offer_id or offer_id in seen:
                    continue
                seen.add(offer_id)

                salon = short_salon(get(row, i_salon))
                brand = str(get(row, i_brand) or "").upper()
                model = str(get(row, i_model) or "").upper().replace(" ", "_")
                link = make_mobile_link(url)

                lines.append(f"{offer_id}/")
                lines.append(f"{salon} used {brand} {model}")
                lines.append(link)

            if lines:
                lines.insert(0, f"🔄 Снова в продаже: {len(seen)} авто\n")

    wb.close()
    return "\n".join(lines)


# ── Telegram ───────────────────────────────────────────────────────

def send_telegram(text):
    if not TELEGRAM_CHAT_ID:
        print(text)
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    chunks = []
    while len(text) > 4000:
        split_pos = text.rfind("\n", 0, 4000)
        if split_pos == -1:
            split_pos = 4000
        chunks.append(text[:split_pos])
        text = text[split_pos:].lstrip()
    chunks.append(text)

    for chunk in chunks:
        data = json.dumps({
            "chat_id": TELEGRAM_CHAT_ID,
            "text": chunk,
        }).encode("utf-8")

        req = urllib.request.Request(
            url, data=data,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                result = json.loads(resp.read().decode())
                if not result.get("ok"):
                    print(f"[TG] Error: {result}")
                    return False
        except Exception as e:
            print(f"[TG] Error: {e}")
            return False

    return True


# ── Main ───────────────────────────────────────────────────────────

def run():
    print(f"[{datetime.now().strftime('%H:%M')}] Email Digest Bot")

    emails = fetch_today_emails()
    if not emails:
        print("No reports today.")
        return

    all_parts = [f"📧 Дайджест auto.ru — {datetime.now().strftime('%d.%m.%Y')}\n"]

    for subject, files in emails:
        for filepath in files:
            fname = os.path.basename(filepath).lower()

            if "не_выкупленные" in fname or "не выкупленные" in fname:
                formatted = format_not_purchased(filepath)
            elif "снова_в_продаже" in fname or "снова в продаже" in fname:
                formatted = format_back_on_sale(filepath)
            else:
                formatted = f"📎 {subject} — не распознан формат"

            if formatted:
                all_parts.append(formatted)

    message = "\n\n".join(all_parts)

    success = send_telegram(message)
    print("✅ Sent" if success else "⚠️ Failed")

    # Cleanup
    for _, files in emails:
        for f in files:
            try:
                os.remove(f)
            except OSError:
                pass


if __name__ == "__main__":
    run()
