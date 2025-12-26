

import requests
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from PIL import Image
from io import BytesIO
import pytesseract
import os

# Tesseract path (Windows)
pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"


POPULAR_URL = "https://api.themoviedb.org/3/movie/popular"
IMAGES_URL = "https://api.themoviedb.org/3/movie/{id}/images"
IMAGE_BASE = "https://image.tmdb.org/t/p/original"

MIN_DATE = datetime(2014, 11, 11)
MAX_DATE = datetime(2025, 11, 11)

MAX_WORKERS = 10
MAX_POPULAR_PAGES = 500

OUTPUT_FOLDER = r"C:\openCVtraining"
OUTPUT_FILE = os.path.join(OUTPUT_FOLDER, "real_movie_posters_with_credit_block.csv")


# --------------------------------------------------
# OCR: Detect if poster has bottom credits
# --------------------------------------------------
def has_bottom_credits(img: Image.Image) -> bool:
    width, height = img.size
    crop_height = int(height * 0.18)
    bottom_area = img.crop((0, height - crop_height, width, height))

    text = pytesseract.image_to_string(bottom_area).lower().strip()

    credit_keywords = [
        "directed", "produced", "executive", "written",
        "screenplay", "production", "starring", "cinematography",
        "music", "soundtrack", "editor", "casting", "photography"
    ]

    return any(word in text for word in credit_keywords)


# --------------------------------------------------
# Filter posters before OCR
# --------------------------------------------------
def filter_candidate_posters(posters):
    banned = ["textless", "clean", "logo", "no-text", "no text"]

    keep = []
    for p in posters:
        if p.get("iso_639_1") != "en":
            continue
        if p.get("width", 0) < 1500:
            continue

        file_path = p.get("file_path", "").lower()
        if any(b in file_path for b in banned):
            continue

        keep.append(p)

    return sorted(
        keep,
        key=lambda p: (p.get("vote_count", 0), p.get("width", 0) * p.get("height", 0)),
        reverse=True
    )


# --------------------------------------------------
# Fetch popular movies in date range
# --------------------------------------------------
def fetch_popular_movies():
    movies = []

    for page in range(1, MAX_POPULAR_PAGES + 1):
        resp = requests.get(
            POPULAR_URL,
            params={"api_key": API_KEY, "language": "en-US", "page": page}
        ).json()

        for m in resp.get("results", []):
            release_date = m.get("release_date")
            if not release_date:
                continue

            try:
                rd = datetime.strptime(release_date, "%Y-%m-%d")
            except:
                continue

            if MIN_DATE <= rd <= MAX_DATE:
                movies.append({
                    "id": m["id"],
                    "title": m["title"],
                    "release_date": release_date,
                    "popularity": m.get("popularity", 0)
                })

        print(f"Fetched page {page}")

        if page >= resp.get("total_pages", page):
            break

    return movies


# --------------------------------------------------
# Fetch ONLY ONE poster URL with credits
# --------------------------------------------------
def fetch_movie_poster(movie):
    movie_id = movie["id"]
    title = movie["title"]

    # Poster metadata
    posters_raw = requests.get(
        IMAGES_URL.format(id=movie_id),
        params={"api_key": API_KEY}
    ).json().get("posters", [])

    posters = filter_candidate_posters(posters_raw)

    for p in posters:
        path = p.get("file_path")
        if not path:
            continue

        image_url = IMAGE_BASE + path

        try:
            img_data = requests.get(image_url).content
            img = Image.open(BytesIO(img_data))

            # Must contain professional credit block
            if has_bottom_credits(img):
                return {
                    "title": title,
                    "release_date": movie["release_date"],
                    "popularity": movie["popularity"],
                    "poster_image_url": image_url,
                    "width": p.get("width"),
                    "height": p.get("height"),
                    "vote_count": p.get("vote_count")
                }
        except:
            continue

    return None


# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main():
    print("Fetching popular movies between 2014–2025…")
    movies = fetch_popular_movies()
    print(f"Movies found: {len(movies)}")

    results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_movie_poster, m): m for m in movies}

        for i, future in enumerate(as_completed(futures), start=1):
            result = future.result()
            if result:
                results.append(result)
                print(f"✔ Poster found for {result['title']}")

            if i % 50 == 0:
                print(f"Processed {i}/{len(movies)} movies…")

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    df = pd.DataFrame(results)
    df.to_csv(OUTPUT_FILE, index=False)

    print(f"\nDone! File saved to:\n{OUTPUT_FILE}\n")


if __name__ == "__main__":
    main()