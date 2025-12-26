import os
import re
import time
import json
import base64
import requests
import pandas as pd
import pytesseract
from io import BytesIO
from PIL import Image
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# Crypto imports
from Crypto.Cipher import AES
from Crypto.Protocol.KDF import PBKDF2
from Crypto.Hash import SHA1, SHA256, SHA512

# ==========================================
# CONFIG
# ==========================================

BASE_URL = "https://www.cinematerial.com"
TV_URL = "https://www.cinematerial.com/tv"

OUTPUT_DIR = r"C:\CineMaterialTV"
CSV_OUTPUT = "cinematerial_tvshows.csv"

pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"

os.makedirs(OUTPUT_DIR, exist_ok=True)

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
})


# ==========================================
# UNIVERSAL COOKIE EDITOR DECRYPTION
# ==========================================

def try_decrypt(ciphertext_b64, password, hash_mod):
    blob = base64.b64decode(ciphertext_b64)

    salt = blob[:16]
    iv = blob[16:28]
    enc = blob[28:]

    key = PBKDF2(password, salt, dkLen=32, count=100000, hmac_hash_module=hash_mod)

    cipher = AES.new(key, AES.MODE_GCM, iv)

    try:
        dec = cipher.decrypt(enc)
        return dec.decode("utf-8", errors="ignore")
    except Exception:
        return None


def decrypt_cookie_editor(ciphertext_b64, password):
    hashes = [SHA256, SHA1, SHA512]

    for h in hashes:
        print(f"[*] Trying PBKDF2 with {h.__name__} ...")
        out = try_decrypt(ciphertext_b64, password, h)
        if out:
            print(f"[+] Successfully decrypted using {h.__name__}")
            return out

    raise Exception("Unable to decrypt cookies.json: No valid hash algorithm worked.")


# ==========================================
# LOAD ENCRYPTED COOKIES
# ==========================================

def load_encrypted_cookies(path="cookies.json"):
    print("[*] Loading encrypted Cookie Editor file...")

    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    ciphertext = raw.get("data")
    if not ciphertext:
        raise Exception("cookies.json missing 'data' field")

    password = input("Enter Cookie Editor password: ").strip()

    decrypted = decrypt_cookie_editor(ciphertext, password)
    cookies = json.loads(decrypted)

    added = 0
    for c in cookies:
        if "cinematerial.com" in c.get("domain", ""):
            session.cookies.set(
                name=c["name"],
                value=c["value"],
                domain=c["domain"],
                path=c.get("path", "/")
            )
            added += 1

    print(f"[+] Imported {added} cookies.")
    return added


# ==========================================
# SCRAPE TV LISTING PAGES
# ==========================================

def get_tv_show_pages():
    results = []
    page = 1

    while True:
        url = f"{TV_URL}?page={page}"
        print(f"[*] Getting page {page} ...")

        r = session.get(url)
        soup = BeautifulSoup(r.text, "html.parser")

        links = soup.select(".media-box-title a")
        if not links:
            print("[*] No more pages.")
            break

        for link in links:
            title = link.text.strip()
            full = urljoin(BASE_URL, link["href"])
            results.append((title, full))

        page += 1
        time.sleep(1)

    print(f"[+] Found {len(results)} shows.")
    return results


# ==========================================
# GET POSTER URLs
# ==========================================

def get_poster_urls(show_url):
    r = session.get(show_url)
    soup = BeautifulSoup(r.text, "html.parser")

    posters = []
    for img in soup.select(".poster img"):
        src = img.get("src")
        if src:
            posters.append(src.replace("t_poster", "l_poster"))

    return posters


# ==========================================
# DOWNLOAD POSTER
# ==========================================

def download_image(url, title):
    filename = f"{title}_{os.path.basename(url)}".replace(" ", "_")
    path = os.path.join(OUTPUT_DIR, filename)

    try:
        img = Image.open(BytesIO(session.get(url).content))
        img.save(path)
        return path
    except:
        return ""


# ==========================================
# OCR CAST + YEAR
# ==========================================

def extract_cast_and_year(path):
    try:
        img = Image.open(path)
        w, h = img.size
        crop = img.crop((0, int(h * 0.8), w, h))

        text = pytesseract.image_to_string(crop)

        cast = re.findall(r"[A-Z][a-z]+ [A-Z][a-z]+", text)
        year_match = re.search(r"(19|20)\d{2}", text)

        return ", ".join(cast[:6]), (year_match.group(0) if year_match else "")
    except:
        return "", ""


# ==========================================
# MAIN
# ==========================================

def main():
    load_encrypted_cookies("cookies.json")

    shows = get_tv_show_pages()

    rows = []

    for title, url in shows:
        print(f"\n[*] Processing: {title}")

        posters = get_poster_urls(url)
        for p in posters:
            print("    Poster:", p)

            local_path = download_image(p, title)
            cast, year = extract_cast_and_year(local_path)

            rows.append({
                "title": title,
                "image_url": p,
                "local_file": local_path,
                "cast_detected": cast,
                "year_detected": year
            })

            time.sleep(0.5)

    pd.DataFrame(rows).to_csv(CSV_OUTPUT, index=False)
    print("\n[+] DONE! CSV saved:", CSV_OUTPUT)


if __name__ == "__main__":
    main()
