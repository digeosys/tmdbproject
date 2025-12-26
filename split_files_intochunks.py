import os
import zipfile

SOURCE_ZIP = r"C:\albumart\imagesforresizing\imagesresize.zip"
OUTPUT_DIR = r"C:\albumart\imagesforresizing\zip_chunks"
MAX_SIZE = 500 * 1024 * 1024  # 500 MB

os.makedirs(OUTPUT_DIR, exist_ok=True)

with zipfile.ZipFile(SOURCE_ZIP, "r") as src:
    files = src.namelist()

    zip_index = 1
    current_size = 0
    current_zip = None

    for filename in files:
        file_bytes = src.read(filename)
        file_size = len(file_bytes)

        # Start a new ZIP if needed
        if current_zip is None or current_size + file_size > MAX_SIZE:
            if current_zip:
                current_zip.close()

            zip_path = os.path.join(
                OUTPUT_DIR, f"images_part_{zip_index}.zip"
            )
            current_zip = zipfile.ZipFile(
                zip_path, "w", zipfile.ZIP_DEFLATED
            )
            print(f"📦 Creating {zip_path}")
            current_size = 0
            zip_index += 1

        current_zip.writestr(filename, file_bytes)
        current_size += file_size

    if current_zip:
        current_zip.close()

print("✅ ZIP successfully split into <500MB chunks.")