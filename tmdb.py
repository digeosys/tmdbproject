import requests
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed


POPULAR_URL = "https://api.themoviedb.org/3/movie/popular"
IMAGES_URL = "https://api.themoviedb.org/3/movie/{id}/images"
CREDITS_URL = "https://api.themoviedb.org/3/movie/{id}/credits"
IMAGE_BASE = "https://image.tmdb.org/t/p/original"

MIN_DATE = datetime(2014, 11, 11)
MAX_DATE = datetime(2025, 11, 11)

MAX_WORKERS = 10
MAX_POPULAR_PAGES = 500


# ------------------------------------------------------
# POSTER FILTERING RULES
# ------------------------------------------------------
def pick_theatrical_posters(posters):
    """
    Final combined rules:
    ✔ English only  (iso_639_1 == “en”)
    ✔ Exclude textless/clean (iso_639_1 None or empty)
    ✔ width >= 1500
    ✔ Prefer official by vote_count (descending)
    ✔ Then sort by largest image size (area)
    ✔ Return top 5
    """

    clean_langs = [None, "", "null"]

    filtered = [
        p for p in posters
        if p.get("width", 0) >= 1500
        and p.get("iso_639_1") == "en"
        and p.get("iso_639_1") not in clean_langs
    ]

    # Sort official posters first:
    # 1) Higher vote_count = more official
    # 2) Larger resolution = theatrical
    filtered_sorted = sorted(
        filtered,
        key=lambda p: (
            p.get("vote_count", 0),
            p.get("width", 0) * p.get("height", 0)
        ),
        reverse=True
    )

    return filtered_sorted[:5]


# ------------------------------------------------------
# FETCH POPULAR MOVIES
# ------------------------------------------------------
def fetch_popular_movies():
    movies = []

    for page in range(1, MAX_POPULAR_PAGES + 1):
        params = {
            "api_key": API_KEY,
            "language": "en-US",
            "page": page
        }

        resp = requests.get(POPULAR_URL, params=params)
        data = resp.json()
        results = data.get("results", [])

        if not results:
            break

        for m in results:
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
                    "title": m.get("title"),
                    "release_date": release_date,
                    "popularity": m.get("popularity")
                })

        print(f"Fetched popular page {page}/{MAX_POPULAR_PAGES}")

        total_pages = data.get("total_pages", page)
        if page >= total_pages:
            break

    return movies


# ------------------------------------------------------
# FETCH POSTERS + CAST
# ------------------------------------------------------
def fetch_movie_assets(movie):
    movie_id = movie["id"]
    title = movie["title"]
    release_date = movie["release_date"]
    popularity = movie["popularity"]

    # Posters
    try:
        img_resp = requests.get(IMAGES_URL.format(id=movie_id),
                                params={"api_key": API_KEY})
        poster_list = img_resp.json().get("posters", [])

        posters = pick_theatrical_posters(poster_list)
    except Exception as e:
        print(f"[WARN] Poster fetch failed for {movie_id}: {e}")
        posters = []

    # Cast
    cast_str = ""
    try:
        cred_resp = requests.get(CREDITS_URL.format(id=movie_id),
                                 params={"api_key": API_KEY})
        cast = cred_resp.json().get("cast", [])[:5]
        cast_str = " | ".join([c["name"] for c in cast])
    except Exception as e:
        print(f"[WARN] Cast fetch failed for {movie_id}: {e}")

    rows = []
    for i, poster in enumerate(posters, start=1):
        path = poster.get("file_path")
        if not path:
            continue

        poster_url = IMAGE_BASE + path

        rows.append({
            "title": f"{title}_{i}",
            "release_date": release_date,
            "popularity": popularity,
            "poster_url": poster_url,
            "width": poster.get("width"),
            "height": poster.get("height"),
            "language": poster.get("iso_639_1"),
            "vote_count": poster.get("vote_count"),
            "top_billed_cast": cast_str
        })

    return rows


# ------------------------------------------------------
# MAIN
# ------------------------------------------------------
def main():
    print("Fetching TMDB Popular Movies 2014–2025…")
    movies = fetch_popular_movies()
    print(f"\nTotal popular movies in date range: {len(movies)}\n")

    all_rows = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_movie_assets, m): m for m in movies}

        for idx, future in enumerate(as_completed(futures), start=1):
            movie = futures[future]
            try:
                all_rows.extend(future.result())
            except Exception as e:
                print(f"[ERROR] Worker failed for {movie['id']}: {e}")

            if idx % 50 == 0:
                print(f"Processed {idx}/{len(movies)} movies…")

    df = pd.DataFrame(all_rows)
    df.to_csv("tmdb_popular_official_english_posters_2014_2025.csv", index=False)

    print("\nDone! Saved as tmdb_popular_official_english_posters_2014_2025.csv")


if __name__ == "__main__":
    main()
