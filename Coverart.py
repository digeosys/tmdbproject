
pip install requests pandas tqdm openpyxl


import requests
import pandas as pd
import time
from tqdm import tqdm

HEADERS = {
    "User-Agent": "CoverArtScraper/1.0 (your@email.com)"
}

MB_ARTIST_SEARCH = "https://musicbrainz.org/ws/2/artist/"
MB_RELEASE_GROUPS = "https://musicbrainz.org/ws/2/release-group"
COVER_ART = "https://coverartarchive.org/release-group/{}"

artists_data = []

def get_artists(limit=1000):
    artists = []
    offset = 0

    while len(artists) < limit:
        params = {
            "query": "type:group",
            "fmt": "json",
            "limit": 100,
            "offset": offset
        }
        r = requests.get(MB_ARTIST_SEARCH, params=params, headers=HEADERS)
        r.raise_for_status()

        data = r.json()["artists"]
        artists.extend(data)
        offset += 100
        time.sleep(1)

    return artists[:limit]

def get_release_groups(artist_id):
    params = {
        "artist": artist_id,
        "type": "album",
        "fmt": "json",
        "limit": 5
    }
    r = requests.get(MB_RELEASE_GROUPS, params=params, headers=HEADERS)
    r.raise_for_status()
    return r.json().get("release-groups", [])

def get_cover_url(rg_id):
    try:
        r = requests.get(COVER_ART.format(rg_id), headers=HEADERS)
        if r.status_code == 200:
            images = r.json().get("images", [])
            for img in images:
                if img.get("front"):
                    return img["image"]
    except:
        pass
    return None

artists = get_artists(1000)

for artist in tqdm(artists):
    row = {
        "Artist": artist["name"]
    }

    releases = get_release_groups(artist["id"])

    for i, rg in enumerate(releases):
        album_col = "Name of Album" if i == 0 else f"Name of Album{i+1}"
        row[album_col] = rg["title"]
        row[f"Image URL {i+1}"] = get_cover_url(rg["id"])
        time.sleep(0.3)

    artists_data.append(row)

df = pd.DataFrame(artists_data)
df.to_csv("c:\OpenCVTraining\cover_art_archive_top_artists.csv", index=False)