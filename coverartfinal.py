import os
import re
import time
import json
import base64
import random
import requests
import pandas as pd
from urllib.parse import quote
from tqdm import tqdm

# ======================================================
# YOUR SPOTIFY CREDENTIALS
# ======================================================
SPOTIFY_CLIENT_ID = "b29ad53680ad4cd0a5089a0dbcda4d25"
SPOTIFY_CLIENT_SECRET = "ae576cc3802d4743b4caf5275b13e0f1"

# ======================================================
# CONFIG
# ======================================================
TARGET_ARTISTS = 1000
ALBUMS_PER_ARTIST = 5

# How many archive.org items to sample for seeds (bigger = more names, slower)
ARCHIVE_SEED_ITEMS = 20000
ARCHIVE_ROWS_PER_PAGE = 200

# Wikipedia seed pages (simple, high-signal lists; can add more)
WIKI_SEED_PAGES = [
    # Best-selling music artists (lots of global legends)
    "https://en.wikipedia.org/wiki/List_of_best-selling_music_artists",
    # Rolling Stone 100 Greatest Artists (many classics)
    "https://en.wikipedia.org/wiki/Rolling_Stone%27s_100_Greatest_Artists_of_All_Time",
    # Rock and Roll Hall of Fame inductees (bands + solo)
    "https://en.wikipedia.org/wiki/List_of_Rock_and_Roll_Hall_of_Fame_inductees",
]

OUTPUT_DIR = r"C:\OpenCVTraining"
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_FILE = os.path.join(
    OUTPUT_DIR,
    "spotify_top_1000_artists_5_albums_CURATED.xlsx"
)

# Checkpoint (resume-safe)
CHECKPOINT_ARTISTS_JSONL = os.path.join(OUTPUT_DIR, "checkpoint_top_artists.jsonl")
CHECKPOINT_ALBUMS_JSONL = os.path.join(OUTPUT_DIR, "checkpoint_album_rows.jsonl")

HEADERS_COMMON = {"User-Agent": "SpotifyFamousArtistsScraper/1.0 (research@example.com)"}

# ======================================================
# Spotify Auth
# ======================================================
def get_spotify_token() -> str:
    auth = base64.b64encode(
        f"{SPOTIFY_CLIENT_ID}:{SPOTIFY_CLIENT_SECRET}".encode("utf-8")
    ).decode("utf-8")

    r = requests.post(
        "https://accounts.spotify.com/api/token",
        headers={"Authorization": f"Basic {auth}", **HEADERS_COMMON},
        data={"grant_type": "client_credentials"},
        timeout=30
    )
    r.raise_for_status()
    return r.json()["access_token"]

TOKEN = get_spotify_token()
SPOTIFY_HEADERS = {"Authorization": f"Bearer {TOKEN}", **HEADERS_COMMON}

# ======================================================
# Helpers
# ======================================================
def norm_name(s: str) -> str:
    if not s:
        return ""
    s = s.lower().strip()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[’']", "", s)
    s = re.sub(r"[^a-z0-9 &\-]", "", s)
    return s

def clean_artist_seed(s: str) -> str:
    """
    Cleans common junk like "Various Artists", "Unknown", etc.
    """
    if not s:
        return ""
    s = str(s).strip()
    bad = {
        "various artists", "unknown", "unknown artist", "n/a", "none", "various",
        "soundtrack", "original soundtrack", "original cast", "cast", "va"
    }
    if norm_name(s) in bad:
        return ""
    # drop super-long weird strings
    if len(s) > 80:
        return ""
    return s

def jitter_sleep(base: float = 0.25):
    time.sleep(base + random.random() * base)

# ======================================================
# Seed Source A: Archive.org creators from coverartarchive
# ======================================================
def fetch_archive_artist_seeds(target_items: int) -> set[str]:
    """
    Uses archive.org advancedsearch to pull identifiers in the coverartarchive collection,
    then uses the returned 'creator' field directly when available.
    """
    seeds: set[str] = set()
    fetched = 0
    page = 1
    url = "https://archive.org/advancedsearch.php"

    print(f"📦 Pulling Archive.org seeds (up to {target_items} items)...")

    while fetched < target_items:
        params = {
            "q": "collection:coverartarchive",
            "fl[]": ["creator"],
            "rows": ARCHIVE_ROWS_PER_PAGE,
            "page": page,
            "output": "json",
        }
        r = requests.get(url, params=params, headers=HEADERS_COMMON, timeout=30)
        r.raise_for_status()
        docs = r.json().get("response", {}).get("docs", [])
        if not docs:
            break

        for d in docs:
            creator = d.get("creator")
            if isinstance(creator, list):
                # sometimes list
                for c in creator:
                    c = clean_artist_seed(c)
                    if c:
                        seeds.add(c)
            else:
                creator = clean_artist_seed(creator)
                if creator:
                    seeds.add(creator)

        fetched += len(docs)
        page += 1
        time.sleep(0.25)

    print(f"✅ Archive seeds found: {len(seeds)} unique artist names")
    return seeds

# ======================================================
# Seed Source B: Curated lists from Wikipedia (simple parsing)
# ======================================================
def fetch_wikipedia_artist_seeds() -> set[str]:
    """
    Fetches a few Wikipedia list pages and extracts likely artist names from link text.
    This is a lightweight heuristic (not perfect, but good enough for seeding).
    """
    seeds: set[str] = set()

    def extract_link_text(html: str) -> list[str]:
        # basic extraction of <a>text</a> (good enough for list pages)
        texts = re.findall(r'<a [^>]*>([^<]{2,80})</a>', html)
        # clean
        out = []
        for t in texts:
            t = re.sub(r"\s+", " ", t).strip()
            if not t:
                continue
            # skip obvious nav/common terms
            if any(x in t.lower() for x in ["edit", "cite", "help", "portal", "wikipedia", "category"]):
                continue
            # skip years / numbers-only
            if re.fullmatch(r"\d{3,4}", t):
                continue
            out.append(t)
        return out

    print("📚 Pulling curated Wikipedia artist seeds...")

    for page in WIKI_SEED_PAGES:
        try:
            r = requests.get(page, headers=HEADERS_COMMON, timeout=30)
            r.raise_for_status()
            html = r.text
            for t in extract_link_text(html):
                t2 = clean_artist_seed(t)
                if t2:
                    seeds.add(t2)
            time.sleep(0.5)
        except Exception:
            # Best-effort; keep going even if one page fails
            continue

    # Add a few “must include” examples (covers the exact ones you mentioned)
    for must in ["Iron Maiden", "Michael Jackson", "Tool", "Metallica", "Pink Floyd", "Led Zeppelin", "Queen"]:
        seeds.add(must)

    print(f"✅ Wikipedia seeds found: {len(seeds)} candidate names")
    return seeds

# ======================================================
# Spotify: Resolve artist name -> best Spotify artist match
# ======================================================
def spotify_search_artist(name: str) -> dict | None:
    """
    Returns best Spotify artist object for a name, or None.
    Strategy:
      - search top 5 results
      - pick the result with best name similarity, break ties by popularity
    """
    q = name.strip()
    if not q:
        return None

    try:
        r = requests.get(
            "https://api.spotify.com/v1/search",
            headers=SPOTIFY_HEADERS,
            params={"q": q, "type": "artist", "limit": 5},
            timeout=20
        )
        if r.status_code != 200:
            return None

        items = r.json().get("artists", {}).get("items", [])
        if not items:
            return None

        target = norm_name(name)

        def score(a):
            an = norm_name(a.get("name", ""))
            # similarity proxy: exact match gets huge boost, else token overlap
            if an == target:
                sim = 100
            else:
                tset = set(target.split())
                aset = set(an.split())
                sim = int(100 * (len(tset & aset) / max(1, len(tset | aset))))
            pop = a.get("popularity") or 0
            return (sim, pop)

        best = max(items, key=score)
        # require at least some similarity, unless artist is extremely popular
        sim, pop = score(best)
        if sim < 30 and pop < 60:
            return None

        return best
    except Exception:
        return None

# ======================================================
# Spotify: Get top albums (most "famous") for an artist
# ======================================================
def spotify_top_albums_for_artist(artist_name: str, limit: int = 5) -> list[dict]:
    """
    Uses Spotify album search for the artist name.
    Search ranking tends to surface the most famous albums first.
    """
    try:
        r = requests.get(
            "https://api.spotify.com/v1/search",
            headers=SPOTIFY_HEADERS,
            params={
                "q": f'artist:"{artist_name}"',
                "type": "album",
                "limit": 15
            },
            timeout=20
        )
        if r.status_code != 200:
            return []

        albums = r.json().get("albums", {}).get("items", [])
        if not albums:
            return []

        # Deduplicate by album name
        seen = set()
        uniq = []
        for a in albums:
            nm = (a.get("name") or "").strip().lower()
            if not nm or nm in seen:
                continue
            seen.add(nm)
            uniq.append(a)
            if len(uniq) >= limit:
                break

        return uniq
    except Exception:
        return []

# ======================================================
# MAIN PIPELINE
# ======================================================
def main():
    # 1) Build seed pool
    archive_seeds = fetch_archive_artist_seeds(ARCHIVE_SEED_ITEMS)
    wiki_seeds = fetch_wikipedia_artist_seeds()

    all_seeds = list({s for s in (archive_seeds | wiki_seeds) if s})
    random.shuffle(all_seeds)
    print(f"🌱 Total unique seed names: {len(all_seeds)}")

    # 2) Resolve seeds to Spotify artists (dedupe by Spotify ID)
    artists_by_id: dict[str, dict] = {}

    # Resume support for artists
    if os.path.exists(CHECKPOINT_ARTISTS_JSONL):
        try:
            with open(CHECKPOINT_ARTISTS_JSONL, "r", encoding="utf-8") as f:
                for line in f:
                    obj = json.loads(line)
                    sid = obj.get("spotify_id")
                    if sid:
                        artists_by_id[sid] = obj
            print(f"♻️ Resumed {len(artists_by_id)} artists from checkpoint.")
        except Exception:
            pass

    print("🔎 Resolving seed names to Spotify artists (this builds the candidate pool)...")
    with open(CHECKPOINT_ARTISTS_JSONL, "a", encoding="utf-8") as ck:
        for name in tqdm(all_seeds, desc="🎤 Artist lookup"):
            if len(artists_by_id) >= 4000:
                # more than enough candidates to select top 1000
                break

            a = spotify_search_artist(name)
            if not a:
                continue

            sid = a["id"]
            if sid in artists_by_id:
                continue

            artists_by_id[sid] = {
                "spotify_id": sid,
                "name": a.get("name"),
                "popularity": a.get("popularity"),
                "genres": a.get("genres", []),
                "followers": (a.get("followers") or {}).get("total"),
                "seed_used": name
            }
            ck.write(json.dumps(artists_by_id[sid], ensure_ascii=False) + "\n")
            ck.flush()

            # small pacing to avoid search throttles
            jitter_sleep(0.12)

    if not artists_by_id:
        raise RuntimeError("No Spotify artists resolved. Check internet / credentials.")

    # 3) Pick Top 1000 by Spotify popularity
    candidates = list(artists_by_id.values())
    candidates.sort(key=lambda x: (x.get("popularity") or 0, x.get("followers") or 0), reverse=True)
    top_artists = candidates[:TARGET_ARTISTS]

    # quick sanity
    names = [a["name"].lower() for a in top_artists if a.get("name")]
    print(f"✅ Top artists selected: {len(top_artists)}")
    print(f"🔍 Sanity check examples present?")
    for chk in ["iron maiden", "michael jackson", "tool"]:
        print(f"  - {chk}: {'YES' if chk in names else 'NO (seed pool may be too small; increase ARCHIVE_SEED_ITEMS)'}")

    # 4) For each artist, get 5 "most famous" albums + cover URLs (row per album)
    # Resume support for album rows
    album_rows = []
    done_artist_ids = set()

    if os.path.exists(CHECKPOINT_ALBUMS_JSONL):
        try:
            with open(CHECKPOINT_ALBUMS_JSONL, "r", encoding="utf-8") as f:
                for line in f:
                    obj = json.loads(line)
                    album_rows.append(obj)
                    done_artist_ids.add(obj.get("artist_spotify_id"))
            print(f"♻️ Resumed {len(done_artist_ids)} artists' albums from checkpoint.")
        except Exception:
            pass

    with open(CHECKPOINT_ALBUMS_JSONL, "a", encoding="utf-8") as ck2:
        for a in tqdm(top_artists, desc="💿 Top albums"):
            artist_id = a["spotify_id"]
            if artist_id in done_artist_ids:
                continue

            albums = spotify_top_albums_for_artist(a["name"], limit=ALBUMS_PER_ARTIST)

            # If album search is sparse for some artists, fall back to /artists/{id}/albums
            if len(albums) < ALBUMS_PER_ARTIST:
                try:
                    r = requests.get(
                        f"https://api.spotify.com/v1/artists/{artist_id}/albums",
                        headers=SPOTIFY_HEADERS,
                        params={"include_groups": "album", "market": "US", "limit": 50},
                        timeout=20
                    )
                    if r.status_code == 200:
                        items = r.json().get("items", [])
                        # add new ones to reach 5
                        seen = {((x.get("name") or "").lower().strip()) for x in albums}
                        for it in items:
                            nm = (it.get("name") or "").lower().strip()
                            if not nm or nm in seen:
                                continue
                            albums.append(it)
                            seen.add(nm)
                            if len(albums) >= ALBUMS_PER_ARTIST:
                                break
                except Exception:
                    pass

            # Trim to 5
            albums = albums[:ALBUMS_PER_ARTIST]

            # Write album rows
            for alb in albums:
                images = alb.get("images") or []
                image_url = images[0]["url"] if images else None  # highest res available
                row = {
                    "Artist": a.get("name"),
                    "Artist Spotify ID": artist_id,
                    "Artist Popularity": a.get("popularity"),
                    "Artist Genres": ", ".join(a.get("genres") or []),
                    "Album": alb.get("name"),
                    "Album Spotify ID": alb.get("id"),
                    "Release Date": alb.get("release_date"),
                    "Image URL": image_url,
                    "Album URI": alb.get("uri"),
                }
                album_rows.append(row)
                ck2.write(json.dumps(row, ensure_ascii=False) + "\n")
                ck2.flush()

            done_artist_ids.add(artist_id)
            jitter_sleep(0.15)

    # 5) Export Excel (5000-ish rows)
    df = pd.DataFrame(album_rows)

    # Keep only rows for the final selected artists (in case checkpoint contains older runs)
    selected_ids = {a["spotify_id"] for a in top_artists}
    df = df[df["Artist Spotify ID"].isin(selected_ids)].copy()

    # Sort nicely
    df.sort_values(["Artist Popularity", "Artist", "Album"], ascending=[False, True, True], inplace=True)

    # Write
    df.to_excel(OUTPUT_FILE, index=False)

    print("\n✅ DONE")
    print(f"📁 Output Excel:\n{OUTPUT_FILE}")
    print(f"🧷 Artist checkpoint:\n{CHECKPOINT_ARTISTS_JSONL}")
    print(f"🧷 Album checkpoint:\n{CHECKPOINT_ALBUMS_JSONL}")
    print(f"📊 Rows written: {len(df)} (expected ~{TARGET_ARTISTS * ALBUMS_PER_ARTIST})")


if __name__ == "__main__":
    main()
