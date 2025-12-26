import os
import re
import random
import html
import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv

# -----------------------------
# Optional: OpenAI text generation
# -----------------------------
USE_OPENAI = True

def clean_handle(s: str) -> str:
    if not isinstance(s, str):
        return ""
    return s.strip()

def normalize_tags(tag_str: str) -> list[str]:
    if not isinstance(tag_str, str) or not tag_str.strip():
        return []
    # Shopify exports tags as "tag1, tag2, tag3"
    return [t.strip().lower() for t in tag_str.split(",") if t.strip()]

def pick_primary_collection(tags: list[str], rules: list[tuple[str, str]]) -> str:
    """
    rules: list of (keyword, collection_handle)
    first match wins
    """
    tag_blob = " ".join(tags)
    for kw, coll in rules:
        if kw in tag_blob:
            return coll
    return "all-posters"  # fallback

def build_related_products(group_df: pd.DataFrame, current_product_gid: str, k: int = 6) -> list[str]:
    """
    group_df must include Product GID for metafield product list
    """
    candidates = group_df["Product GID"].dropna().astype(str).unique().tolist()
    candidates = [c for c in candidates if c and c != current_product_gid]
    random.shuffle(candidates)
    return candidates[:k]

def html_paragraphs(paras: list[str]) -> str:
    return "\n".join([f"<p>{html.escape(p)}</p>" for p in paras])

def html_bullets(items: list[str]) -> str:
    lis = "\n".join([f"<li>{html.escape(i)}</li>" for i in items])
    return f"<ul>\n{lis}\n</ul>"

def make_faq_schema_jsonld() -> str:
    # Keep generic and safe (no "official", no licensing claims)
    return """<script type="application/ld+json">
{
  "@context":"https://schema.org",
  "@type":"FAQPage",
  "mainEntity":[
    {"@type":"Question","name":"Is this an original poster?","acceptedAnswer":{"@type":"Answer","text":"This is a professionally printed reproduction designed for display and collection."}},
    {"@type":"Question","name":"What sizes are available?","acceptedAnswer":{"@type":"Answer","text":"Multiple size options may be available. Please select your preferred size on the product page."}},
    {"@type":"Question","name":"Does it come framed?","acceptedAnswer":{"@type":"Answer","text":"Frames are not included unless explicitly stated in the listing."}},
    {"@type":"Question","name":"How is it shipped?","acceptedAnswer":{"@type":"Answer","text":"Posters are packaged carefully to help protect the print during transit."}}
  ]
}
</script>"""

# -----------------------------
# OpenAI helper (new SDK style)
# -----------------------------
def generate_product_description_openai(title: str, year: str, genre: str, fmt: str, hook: str, tags: list[str]) -> str:
    """
    Returns HTML body with FAQ schema appended.
    """
    load_dotenv()
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key or not USE_OPENAI:
        return generate_product_description_fallback(title, year, genre, fmt, hook, tags)

    # OpenAI SDK
    from openai import OpenAI
    client = OpenAI(api_key=api_key)

    prompt = f"""
Write a unique SEO-optimized product description for a poster product.

Product title: {title}
Year or era: {year}
Genre: {genre}
Format: {fmt}
Unique visual hook: {hook}
Extra tags/context: {", ".join(tags[:12])}

Rules:
- 350–500 words
- No mention of licensing claims
- Do not say “official”
- Confident collectible tone
- Include one cultural or historical reference (generic is fine)
- Avoid repeating phrases
- End with a soft call to action
- Output as HTML using <p> and one <ul> bullet list
"""

    resp = client.responses.create(
        model="gpt-4.1-mini",
        input=prompt.strip()
    )
    text = resp.output_text.strip()

    # Ensure FAQ schema is included (append)
    return text + "\n" + make_faq_schema_jsonld()

def generate_product_description_fallback(title: str, year: str, genre: str, fmt: str, hook: str, tags: list[str]) -> str:
    # Non-AI fallback so the pipeline runs even without API.
    paras = [
        f"This {fmt.lower()} featuring “{title}” is a bold display piece for fans and collectors.",
        f"With a {year or 'classic-era'} vibe and a strong {genre or 'genre'} aesthetic, the artwork stands out thanks to {hook or 'its iconic design'}.",
        "Printed for clarity and detail at display sizes, it’s a great fit for home theaters, music rooms, offices, or any curated wall.",
        "Add it to your collection or gift it to a fellow fan—either way, it’s an easy upgrade to your space."
    ]
    bullets = [
        "Premium high-resolution print",
        "Multiple size options may be available",
        "Ideal for framing or direct wall display",
        "Durable print intended for long-term display",
        "Packaged carefully for transit"
    ]
    body = html_paragraphs(paras) + "\n" + html_bullets(bullets) + "\n" + make_faq_schema_jsonld()
    return body

def generate_collection_copy_openai(collection_name: str, era: str, genre: str, keywords: str) -> str:
    load_dotenv()
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key or not USE_OPENAI:
        return f"<p>Explore {html.escape(collection_name)} spanning {html.escape(era)} with a focus on {html.escape(genre)}. Keywords: {html.escape(keywords)}</p>"

    from openai import OpenAI
    client = OpenAI(api_key=api_key)

    prompt = f"""
Write SEO-optimized collection page content.

Collection name: {collection_name}
Era focus: {era}
Genre: {genre}
Primary keywords: {keywords}

Rules:
- 250–350 words
- Write for collectors and fans
- Do not mention shipping or pricing
- Include 3 subheadings using <h2>
- Output as HTML
"""
    resp = client.responses.create(
        model="gpt-4.1-mini",
        input=prompt.strip()
    )
    return resp.output_text.strip()

def main():
    # -----------------------------
    # INPUT: Your Shopify export CSV
    # -----------------------------
    INPUT_PRODUCTS_CSV = "shopify_products_export.csv"

    # Output files
    OUT_PRODUCTS_UPDATE = "products_update.csv"
    OUT_COLLECTIONS_UPDATE = "collections_update.csv"
    OUT_SKU_MAP = "sku_mapping.csv"

    # -----------------------------
    # Collection mapping rules
    # Put the MOST specific keywords first.
    # -----------------------------
    rules = [
        ("horror", "horror-posters"),
        ("metal", "metal-album-posters"),
        ("rock", "rock-album-posters"),
        ("sci-fi", "sci-fi-posters"),
        ("science fiction", "sci-fi-posters"),
        ("thriller", "thriller-posters"),
        ("action", "action-posters"),
        ("comedy", "comedy-posters"),
        ("drama", "drama-posters"),
    ]

    df = pd.read_csv(INPUT_PRODUCTS_CSV, dtype=str).fillna("")

    # Shopify exports vary. These are common columns:
    # Handle, Title, Tags, Body (HTML), Variant SKU, Product ID (maybe), etc.
    # We'll try to infer.
    required = ["Handle", "Title"]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Missing required column in export: {col}")

    # Product GID: If export has "Product ID", convert to GID. Otherwise leave blank.
    if "Product ID" in df.columns and "Product GID" not in df.columns:
        df["Product GID"] = df["Product ID"].apply(lambda x: f"gid://shopify/Product/{x}" if str(x).strip() else "")
    elif "Product GID" not in df.columns:
        df["Product GID"] = ""

    # Try to get SKU
    sku_col = "Variant SKU" if "Variant SKU" in df.columns else ("SKU" if "SKU" in df.columns else "")
    if not sku_col:
        raise ValueError("Could not find SKU column (Variant SKU or SKU).")

    # Derive genre/year/hook from tags + title (best effort)
    def infer_year(title: str) -> str:
        m = re.search(r"(19\d{2}|20\d{2})", title)
        return m.group(1) if m else ""

    def infer_genre(tags: list[str]) -> str:
        for g in ["horror", "rock", "metal", "sci-fi", "thriller", "action", "comedy", "drama"]:
            if g in tags:
                return g
        return ""

    def infer_hook(title: str, tags: list[str]) -> str:
        # Keep generic; you can improve this by adding a "Unique Hook" column later.
        if "minimal" in tags:
            return "a clean, minimalist layout"
        if "vintage" in tags:
            return "a vintage, retro-inspired look"
        return "bold artwork and strong visual presence"

    # Build mapping
    df["tags_norm"] = df["Tags"].apply(normalize_tags)
    df["primary_collection"] = df["tags_norm"].apply(lambda t: pick_primary_collection(t, rules))
    df["year"] = df["Title"].apply(infer_year)
    df["genre"] = df.apply(lambda r: infer_genre(r["tags_norm"]), axis=1)
    df["format"] = "Poster"

    # Group by collection for related products
    updates = []
    sku_map_rows = []

    # Build a faster lookup by collection
    grouped = {k: v.copy() for k, v in df.groupby("primary_collection")}

    for _, row in tqdm(df.iterrows(), total=len(df), desc="Generating product updates"):
        handle = clean_handle(row["Handle"])
        title = row["Title"].strip()
        sku = row[sku_col].strip()
        coll = row["primary_collection"].strip()
        year = row["year"].strip()
        genre = row["genre"].strip()
        fmt = row["format"]
        hook = infer_hook(title, row["tags_norm"])
        gid = row["Product GID"].strip()

        # Related products: only if we have GIDs
        related = []
        if gid and coll in grouped and "Product GID" in grouped[coll].columns:
            related = build_related_products(grouped[coll], gid, k=6)

        body_html = generate_product_description_openai(
            title=title, year=year, genre=genre, fmt=fmt, hook=hook, tags=row["tags_norm"]
        )

        updates.append({
            "Handle": handle,
            "Body (HTML)": body_html,
            "Metafield: custom.primary_collection": coll,
            # If you don't have Product GIDs, you can instead store handles as text.
            "Metafield: custom.related_products": ",".join(related)
        })

        sku_map_rows.append({
            "SKU": sku,
            "Handle": handle,
            "Title": title,
            "Primary collection": coll
        })

    out_updates = pd.DataFrame(updates)
    out_updates.to_csv(OUT_PRODUCTS_UPDATE, index=False, encoding="utf-8-sig")

    pd.DataFrame(sku_map_rows).to_csv(OUT_SKU_MAP, index=False, encoding="utf-8-sig")

    # Collection copy generation (one row per collection)
    collections = []
    for coll, g in grouped.items():
        # Make a human-friendly collection name from handle (or you can provide your own mapping file)
        coll_name = coll.replace("-", " ").title()
        era = "Classic to Modern"
        genre = coll_name.split()[0] if coll_name else ""
        keywords = f"{coll_name.lower()}, posters, wall art, prints"

        desc_html = generate_collection_copy_openai(coll_name, era, genre, keywords)
        collections.append({
            "Collection handle": coll,
            "Collection title": coll_name,
            "Collection description (HTML)": desc_html
        })

    pd.DataFrame(collections).to_csv(OUT_COLLECTIONS_UPDATE, index=False, encoding="utf-8-sig")

    print("Done.")
    print(f"- {OUT_PRODUCTS_UPDATE}")
    print(f"- {OUT_COLLECTIONS_UPDATE}")
    print(f"- {OUT_SKU_MAP}")

if __name__ == "__main__":
    main()