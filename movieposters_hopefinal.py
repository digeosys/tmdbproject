import requests
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from PIL import Image
from io import BytesIO
import pytesseract
import os

# ---------------------------------------------
# Tesseract path
# ---------------------------------------------
pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"

# ---------------------------------------------
# TMDB API
# ---------------------------------------------


POPULAR_URL = "https://api.themoviedb.org/3/movie/popular"
IMAGES_URL = "https://api.themoviedb.org/3/movie/{id}/images"
CREDITS_URL = "https://api.themoviedb.org/3/movie/{id}/credits"
IMAGE_BASE = "https://image.tmdb.org/t/p/original"

# ---------------------------------------------
# Settings
# ---------------------------------------------
MIN_DATE = datetime(2014, 11, 11)
MAX_DATE = datetime(2025, 11, 11)

MAX_WORKERS = 10
MAX_POPULAR_PAGES = 500

OUTPUT_FOLDER = r"C:\openCVtraining"


# ---------------------------------------------
# Detect bottom billing block using OCR
# ---------------------------------------------
def has_bottom_credits(img: Image.Image) -> bool:
    width, height = img.size
    crop_height = int(height * 0.18)
    bottom = img.crop((0, height - crop_height, width, height))

    text = pytesseract.image_to_string(bottom).lower()

    keywords = [
        "directed", "produced", "executive", "written",
        "screenplay", "production", "starring", "cinematography",
        "music", "editor", "casting", "photography"
    ]

    return any(k in text for k in keywords)


# ---------------------------------------------
# Filter posters BEFORE OCR
# ---------------------------------------------
def filter_candidate_posters(posters):
    banned = ["textless", "clean", "logo", "no-text", "no text"]

    keep = []
    for p in posters:
        if p.get("iso_639_1") != "en":
            continue
        if p.get("width", 0) < 1500:
            continue

        fp = p.get("file_path", "").lower()
        if any(b in fp for b in banned):
            continue

        keep.append(p)

    return sorted(
        keep,
        key=lambda p: (p.get("vote_count", 0), p.get("width", 0) * p.get("height", 0)),
        reverse=True
    )


# ---------------------------------------------
# Fetch TMDB popular movies in date range
# ---------------------------------------------
def fetch_popular_movies():
    movies = []
    for page in range(1, MAX_POPULAR_PAGES + 1):
        resp = requests.get(
            POPULAR_URL,
            params={"api_key": API_KEY, "language": "en-US", "page": page}
        ).json()

        for m in resp.get("results", []):
            rd_str = m.get("release_date")
            if not rd_str:
                continue

            try:
                rd = datetime.strptime(rd_str, "%Y-%m-%d")
            except:
                continue

            if MIN_DATE <= rd <= MAX_DATE:
                movies.append({
                    "id": m["id"],
                    "title": m["title"],
                    "release_date": rd_str,
                    "popularity": m.get("popularity", 0)
                })

        print(f"Fetched popular page {page}")

        if page >= resp.get("total_pages", page):
            break

    return movies


# ---------------------------------------------
# Fetch cast + ONE poster with real credit block
# ---------------------------------------------
def fetch_movie_poster(movie):
    movie_id = movie["id"]
    title = movie["title"]

    # ---- 1. GET TOP-BILLED CAST --------------------------------------
    cast_str = ""
    try:
        credits = requests.get(
            CREDITS_URL.format(id=movie_id),
            params={"api_key": API_KEY}
        ).json()

        cast_list = credits.get("cast", [])
        if cast_list:
            cast_str = " | ".join(c.get("name", "") for c in cast_list[:5])
        else:
            cast_str = "(No Cast Listed)"
            print(f"[WARN] No cast returned for {title}")

    except Exception as e:
        cast_str = "(Cast Fetch Error)"
        print(f"[ERROR] Failed to fetch cast for {title}: {e}")

    # ---- 2. GET POSTERS ------------------------------------------------
    posters_raw = requests.get(
        IMAGES_URL.format(id=movie_id),
        params={"api_key": API_KEY}
    ).json().get("posters", [])

    posters = filter_candidate_posters(posters_raw)

    # ---- 3. TRY EACH POSTER UNTIL ONE HAS A CREDIT BLOCK --------------
    for p in posters:
        path = p.get("file_path")
        if not path:
            continue

        url = IMAGE_BASE + path

        try:
            img_bytes = requests.get(url).content
            img = Image.open(BytesIO(img_bytes))

            if has_bottom_credits(img):
                return {
                    "title": title,
                    "release_date": movie["release_date"],
                    "popularity": movie["popularity"],
                    "poster_image_url": url,
                    "top_billed_cast": cast_str,
                    "width": p.get("width"),
                    "height": p.get("height"),
                    "vote_count": p.get("vote_count")
                }
        except:
            continue

    return None


# ---------------------------------------------
# MAIN
# ---------------------------------------------
def main():
    print("Fetching TMDB popular movies…")
    movies = fetch_popular_movies()
    print(f"Movies found: {len(movies)}")

    results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
        futures = {exe.submit(fetch_movie_poster, m): m for m in movies}

        for i, f in enumerate(as_completed(futures), start=1):
            result = f.result()
            if result:
                results.append(result)
                print(f"✔ Poster found for {result['title']}")

            if i % 50 == 0:
                print(f"Processed {i}/{len(movies)} movies…")

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    # ---- UNIQUE TIMESTAMPED OUTPUT FILE --------------------------------
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    unique_file = os.path.join(OUTPUT_FOLDER, f"FinalPosters_{timestamp}.csv")

    # Force all columns to exist, especially top_billed_cast
    df = pd.DataFrame(results, columns=[
        "title",
        "release_date",
        "popularity",
        "poster_image_url",
        "top_billed_cast",
        "width",
        "height",
        "vote_count"
    ])

    df.to_csv(unique_file, index=False)

    print(f"\nDONE — saved to:\n{unique_file}\n")


# ---------------------------------------------
# EXECUTE
# ---------------------------------------------
if __name__ == "__main__":
    main()