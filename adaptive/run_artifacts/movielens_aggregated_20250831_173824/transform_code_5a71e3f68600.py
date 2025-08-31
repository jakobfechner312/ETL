import pandas as pd
import re
import unicodedata

output = []
invalid_entries = []
seen_title_year = set()

for _, row in df.iterrows():
    orig = row.to_dict()
    # 1) Preserve MovieLens ID
    movie_id = row["ID_MOVIELENS"]

    # 2) Determine release_year
    year = None
    # a) Prefer explicit column if it exists
    if "release_year" in row and pd.notna(row["release_year"]):
        year = int(row["release_year"])
    else:
        # b) Fallback: extract the last (...) in title, interpret 2‐digit → 1900+
        m = re.search(r"\((\d{2,4})\)\s*$", row["title"])
        if m:
            y = m.group(1)
            year = int(y) if len(y) == 4 else 1900 + int(y)
    if year is None:
        invalid_entries.append({"row": orig, "reason": "no_release_year"})
        continue
    if not (1870 <= year <= 2025):
        invalid_entries.append({"row": orig, "reason": "invalid_year"})
        continue

    # 3) Select rating (must not be NaN)
    rating = row.get("average_rating", pd.NA)
    if pd.isna(rating):
        invalid_entries.append({"row": orig, "reason": "missing_rating"})
        continue

    # 4) Clean title
    t = row["title"]
    # a) remove one trailing (YYYY) then one more trailing (...), if present
    t = re.sub(r"\(\d{4}\)\s*$", "", t)
    t = re.sub(r"\([^)]*\)\s*$", "", t)
    # b) normalize to ASCII lowercase
    t = unicodedata.normalize("NFKD", t).encode("ascii", "ignore").decode("ascii").lower()
    # c) punctuation→space, collapse spaces, strip
    t = re.sub(r"[^\w\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    # d) drop trailing " the"
    if t.endswith(" the"):
        t = t[: -len(" the")].strip()
    # e) dedupe tokens preserving order
    tokens = []
    for w in t.split():
        if w not in tokens:
            tokens.append(w)
    title_clean = " ".join(tokens)
    # f) enforce uniqueness of (title, year)
    key = (title_clean, year)
    if key in seen_title_year:
        invalid_entries.append({"row": orig, "reason": "duplicate_title_year"})
        continue
    seen_title_year.add(key)

    # 5) Parse genres as list[str]
    g = row.get("genres", "")
    if pd.isna(g) or g.strip().lower() in ("", "(no genres listed)"):
        genres_list = []
    else:
        genres_list = [x for x in g.split("|") if x]

    # 6) Append cleaned entry
    output.append({
        "ID": movie_id,
        "title": title_clean,
        "release_year": year,
        "genres": genres_list,
        "rating": float(rating)
    })