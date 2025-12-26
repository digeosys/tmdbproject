import os
import re
import unicodedata
import requests
import pandas as pd
from tqdm import tqdm

# ---------------------------------------------
# CONFIG
# ---------------------------------------------
CSV_PATH = r"c:\albumart\musicpostersfix1.csv"
OUTPUT_DIR = r"c:\albumart\imagesforresizing"

os.makedirs(OUTPUT_DIR, exist_ok=True)

df = pd.read_csv(CSV_PATH, encoding="latin-1")

handle_col = df.columns[0]

# Auto-detect image column
image_col = next(
    col for col in df.columns
    if col.strip().lower() in ["image src", "image_src", "imagesrc"]
)

# ---------------------------------------------
# FILENAME SANITIZER
# ---------------------------------------------
def safe_filename(text):
    text = str(text)

    # Normalize Unicode (é → e)
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()

    # Replace illegal Windows filename chars
    text = re.sub(r'[\\/:*?"<>|]', "-", text)

    # Collapse whitespace/dashes
    text = re.sub(r"\s+", "-", text)
    text = re.sub(r"-{2,}", "-", text)

    return text.strip("-")

# ---------------------------------------------
# DOWNLOAD
# ---------------------------------------------
session = requests.Session()
session.headers.update({"User-Agent": "Mozilla/5.0"})

valid_rows = df[df[image_col].notna() & (df[image_col].astype(str).str.strip() != "")]
print(f"🧪 Found {len(valid_rows)} rows with image URLs")

for _, row in tqdm(valid_rows.iterrows(), total=len(valid_rows), desc="Downloading images"):
    raw_handle = row[handle_col]
    img_url = row[image_col]

    filename = safe_filename(raw_handle) + ".jpg"
    output_path = os.path.join(OUTPUT_DIR, filename)

    try:
        r = session.get(img_url, timeout=30)
        r.raise_for_status()

        with open(output_path, "wb") as f:
            f.write(r.content)

    except Exception as e:
        print(f"❌ Failed: {raw_handle}")
        print(f"   Error: {e}")

print("✅ All possible images downloaded safely.")