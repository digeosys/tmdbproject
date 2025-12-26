import zipfile
import os
from PIL import Image, ImageFilter

# ------------------------------------------------
# CONFIG
# ------------------------------------------------
ZIP_PATH = r"C:\albumart\imagesforresizing\imagesresize.zip"
OUTPUT_DIR = r"C:\albumart\imagesforresizing\output_11x17"

TARGET_WIDTH = 1100
TARGET_HEIGHT = 1700   # 11 x 17 aspect
TARGET_RATIO = TARGET_WIDTH / TARGET_HEIGHT

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ------------------------------------------------
# HELPERS
# ------------------------------------------------
def is_image(filename):
    return filename.lower().endswith((".jpg", ".jpeg", ".png", ".webp"))

def convert_to_11x17(img: Image.Image) -> Image.Image:
    img = img.convert("RGB")
    w, h = img.size
    img_ratio = w / h

    # Scale foreground to fit height
    scale = TARGET_HEIGHT / h
    fg_w = int(w * scale)
    fg_h = TARGET_HEIGHT
    foreground = img.resize((fg_w, fg_h), Image.LANCZOS)

    # Background (blurred)
    bg = img.resize((TARGET_WIDTH, TARGET_HEIGHT), Image.LANCZOS)
    bg = bg.filter(ImageFilter.GaussianBlur(radius=40))

    # Paste foreground centered
    x_offset = (TARGET_WIDTH - fg_w) // 2
    bg.paste(foreground, (x_offset, 0))

    return bg

# ------------------------------------------------
# PROCESS ZIP
# ------------------------------------------------
with zipfile.ZipFile(ZIP_PATH, "r") as zip_ref:
    image_files = [f for f in zip_ref.namelist() if is_image(f)]

    print(f"🧪 Found {len(image_files)} images in ZIP")

    for name in image_files:
        with zip_ref.open(name) as file:
            img = Image.open(file)

            final_img = convert_to_11x17(img)

            base = os.path.basename(name)
            output_path = os.path.join(OUTPUT_DIR, base.replace(".png", ".jpg"))

            final_img.save(output_path, "JPEG", quality=95)

            print(f"✅ Saved {output_path}")

print("🎯 All images converted to 11x17 poster format.")