import os
import io
import re
import csv
import zipfile
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from PIL import Image
from tqdm import tqdm
from openai import OpenAI

# =====================================================
# CONFIG
# =====================================================
INPUT_ZIP = r"C:\albumart\imagesforresizing\imagesresize.zip"
OUT_DIR = r"C:\albumart\imagesforresizing\output_11x17_ai"
WORK_DIR = r"C:\albumart\imagesforresizing\ai_work"

MODEL = "gpt-image-1"
API_SIZE = (1024, 1536)      # vertical
FINAL_SIZE = (1100, 1700)    # exact 11x17

MAX_WORKERS = 4              # 🔥 speed control (safe: 3–5)
SLEEP_BETWEEN_CALLS = 0.2    # rate-limit friendly

MAPPING_CSV = os.path.join(OUT_DIR, "filename_mapping.csv")
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp"}

# =====================================================
def safe_filename(name):
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    name = re.sub(r'[\\/:*?"<>|]', "-", name)
    name = re.sub(r"\s+", "-", name)
    return re.sub(r"-{2,}", "-", name).strip("-")

# =====================================================
def prepare_canvas(original: Image.Image):
    """
    Creates:
    - transparent canvas (AI paints here)
    - mask (white = paint, black = keep)
    """
    original = original.convert("RGBA")

    canvas = Image.new("RGBA", API_SIZE, (0, 0, 0, 0))
    mask = Image.new("L", API_SIZE, 255)  # white = editable

    # scale original to fit height
    scale = min(API_SIZE[0] / original.width, API_SIZE[1] / original.height)
    fg = original.resize(
        (int(original.width * scale), int(original.height * scale)),
        Image.LANCZOS
    )

    x = (API_SIZE[0] - fg.width) // 2
    y = (API_SIZE[1] - fg.height) // 2

    canvas.paste(fg, (x, y))
    mask.paste(0, (x, y, x + fg.width, y + fg.height))

    return canvas, mask

# =====================================================
def outpaint_one(client, img_path: Path):
    img = Image.open(img_path)
    canvas, mask = prepare_canvas(img)

    buf_img = io.BytesIO()
    buf_mask = io.BytesIO()
    canvas.save(buf_img, "PNG")
    mask.save(buf_mask, "PNG")

    result = client.images.edits(
        model=MODEL,
        image=buf_img.getvalue(),
        mask=buf_mask.getvalue(),
        prompt=(
            "Extend the background naturally to fit a vertical poster. "
            "Do NOT alter existing text, faces, or artwork. "
            "Fill only transparent areas."
        ),
        size=f"{API_SIZE[0]}x{API_SIZE[1]}"
    )

    import base64
    out_bytes = base64.b64decode(result.data[0].b64_json)
    return Image.open(io.BytesIO(out_bytes)).convert("RGB")

# =====================================================
def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(WORK_DIR, exist_ok=True)

    if not os.path.exists(MAPPING_CSV):
        with open(MAPPING_CSV, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(["original", "output"])

    extract_dir = Path(WORK_DIR) / "extracted"
    extract_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(INPUT_ZIP) as z:
        z.extractall(extract_dir)

    images = [p for p in extract_dir.rglob("*") if p.suffix.lower() in IMAGE_EXTS]
    print(f"🧪 Found {len(images)} images")

    client = OpenAI()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {}
        for img_path in images:
            futures[pool.submit(outpaint_one, client, img_path)] = img_path

        for future in tqdm(as_completed(futures), total=len(futures), desc="Outpainting"):
            img_path = futures[future]
            try:
                result_img = future.result()

                safe = safe_filename(img_path.stem) + ".jpg"
                out_path = Path(OUT_DIR) / safe

                result_img.resize(FINAL_SIZE, Image.LANCZOS).save(out_path, "JPEG", quality=95)

                with open(MAPPING_CSV, "a", newline="", encoding="utf-8") as f:
                    csv.writer(f).writerow([img_path.name, safe])

                time.sleep(SLEEP_BETWEEN_CALLS)

            except Exception as e:
                print(f"❌ {img_path.name}: {e}")

    print("✅ COMPLETE")

# =====================================================
if __name__ == "__main__":
    main()