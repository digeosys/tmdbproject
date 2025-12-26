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
# TMDB API (TV VERSION)
# ---------------------------------------------


POPULAR_TV_URL = "https://api.themoviedb.org/3/tv/popular"
IMAGES_URL = "https://api.themoviedb.org/3/tv/{id}/images"
CREDITS_URL = "https://api.themoviedb.org/3/tv/{id}/credits"
IMAGE_BASE = "https://image.tmdb.org/t/p/original"

# ---------------------------------------------
# Settings
# ---------------------------------------------
MAX_WORKERS = 10
MAX_POPULAR_PAGES = 100
OUTPUT_FOLDER = r"C:\openCVtraining"


# ---------------------------------------------
# OCR bottom-credit detection
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
# Filter posters before OCR
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
# Fetch Popular TV Shows
# ---------------------------------------------
def fetch_popular_tv():
    shows = []

    for page in range(1, MAX_POPULAR_PAGES + 1):
        resp = requests.get(
            POPULAR_TV_URL,
            params={"api_key": API_KEY, "language": "en-US", "page": page}
        ).json()

        for s in resp.get("results", []):
            if not s.get("first_air_date"):
                continue

            shows.append({
                "id": s["id"],
                "name": s["name"],
                "first_air_date": s["first_air_date"],
                "popularity": s.get("popularity", 0)
            })

        print(f"Fetched TV popular page {page}")

        if page >= resp.get("total_pages", page):
            break

    return shows


# ---------------------------------------------
# Fetch first 3 posters (with fallback)
# ---------------------------------------------
def fetch_tv_posters(show):
    show_id = show["id"]
    title = show["name"]

    # ---- 1. Cast -------------------------------------
    cast_str = ""
    try:
        credits = requests.get(
            CREDITS_URL.format(id=show_id),
            params={"api_key": API_KEY}
        ).json()

        cast_list = credits.get("cast", [])
        if cast_list:
            cast_str = " | ".join(c.get("name", "") for c in cast_list[:5])
        else:
            cast_str = "(No Cast Listed)"
    except:
        cast_str = "(Cast Fetch Error)"

    # ---- 2. Posters -----------------------------------
    posters_raw = requests.get(
        IMAGES_URL.format(id=show_id),
        params={"api_key": API_KEY}
    ).json().get("posters", [])

    posters = filter_candidate_posters(posters_raw)

    results = []
    credit_found_count = 0

    # ---- 3. FIRST PASS — posters with credits --------
    for p in posters:
        if credit_found_count >= 3:
            break

        url = IMAGE_BASE + p.get("file_path", "")
        try:
            img = Image.open(BytesIO(requests.get(url).content))
            if has_bottom_credits(img):
                credit_found_count += 1
                results.append({
                    "title": title,
                    "first_air_date": show["first_air_date"],
                    "popularity": show["popularity"],
                    "poster_number": credit_found_count,
                    "poster_image_url": url,
                    "top_billed_cast": cast_str,
                    "width": p.get("width"),
                    "height": p.get("height"),
                    "vote_count": p.get("vote_count")
                })
        except:
            continue

    # ---- 4. FALLBACK — no credit posters found -------
    if credit_found_count == 0:
        print(f"[INFO] No OCR-credit posters for {title}. Using top 3 posters…")
        for idx, p in enumerate(posters[:3], start=1):
            url = IMAGE_BASE + p.get("file_path", "")
            results.append({
                "title": title,
                "first_air_date": show["first_air_date"],
                "popularity": show["popularity"],
                "poster_number": idx,
                "poster_image_url": url,
                "top_billed_cast": cast_str,
                "width": p.get("width"),
                "height": p.get("height"),
                "vote_count": p.get("vote_count")
            })

    return results


# ---------------------------------------------
# MAIN
# ---------------------------------------------
def main():
    print("Fetching TMDB popular TV shows…")
    shows = fetch_popular_tv()
    print(f"TV Shows found: {len(shows)}")

    all_results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
        futures = {exe.submit(fetch_tv_posters, s): s for s in shows}

        for i, f in enumerate(as_completed(futures), start=1):
            posters = f.result()
            if posters:
                all_results.extend(posters)
                print(f"✔ Posters found for {posters[0]['title']}")

            if i % 50 == 0:
                print(f"Processed {i}/{len(shows)} shows…")

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    unique_file = os.path.join(OUTPUT_FOLDER, f"TVPosters_{timestamp}.csv")

    df = pd.DataFrame(all_results, columns=[
        "title",
        "first_air_date",
        "popularity",
        "poster_number",
        "poster_image_url",
        "top_billed_cast",
        "width",
        "height",
        "vote_count"
    ])

    df.to_csv(unique_file, index=False)

    print(f"\nDONE — saved to:\n{unique_file}\n")


# ---------------------------------------------
# RUN SCRIPT
# ---------------------------------------------
if __name__ == "__main__":
    main()