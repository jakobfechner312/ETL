import re, unicodedata, string
import pandas as pd

output = []
invalid_entries = []
seen = set()

def clean_title(raw):
    # Normalize to ascii, lowercase
    s = unicodedata.normalize("NFKD", raw).encode("ascii", "ignore").decode().lower()
    # Remove one trailing "(YYYY)"
    s = re.sub(r"\s*\(\d{4}\)\s*$", "", s)
    # Remove one trailing "(...)" general
    s = re.sub(r"\s*\([^)]*\)\s*$", "", s)
    # Replace punctuation with space, collapse whitespace
    trans = str.maketrans(string.punctuation, " " * len(string.punctuation))
    s = s.translate(trans)
    s = re.sub(r"\s+", " ", s).strip()
    # Drop trailing 'the'
    s = re.sub(r"\bthe$", "", s).strip()
    # Dedup tokens keeping order
    tokens, seen_t = [], set()
    for t in s.split():
        if t not in seen_t:
            tokens.append(t)
            seen_t.add(t)
    return " ".join(tokens)

def parse_year(dt):
    if pd.isna(dt):
        return None
    m = re.search(r"(\d{2,4})", str(dt))
    if not m:
        return None
    y = m.group(1)
    if len(y) == 4:
        y = int(y)
    elif len(y) == 2:
        y2 = int(y)
        # prefer 2000–2025 for 0–25, else 1900–1999
        base = 2000 if 0 <= y2 <= 25 else 1900
        y = base + y2
    else:
        return None
    return y

for _, row in df.iterrows():
    orig = row.to_dict()
    ID = row.get("ID_RT")
    # 1) Title
    raw_title = row.get("movie_title") or ""
    title = clean_title(raw_title)
    if not title:
        invalid_entries.append({"row": orig, "reason": "empty title"})
        continue
    # 2) Year
    year = None
    for col in ("original_release_date", "streaming_release_date"):
        year = parse_year(row.get(col))
        if year:
            break
    if year is None or not (1870 <= year <= 2025):
        invalid_entries.append({"row": orig, "reason": "invalid or missing year"})
        continue
    # 3) Rating: prefer tomatometer, fallback to audience
    tom = row.get("tomatometer_rating")
    aud = row.get("audience_rating")
    rating = tom if pd.notna(tom) else aud if pd.notna(aud) else None
    if rating is None:
        invalid_entries.append({"row": orig, "reason": "no rating"})
        continue
    # 4) Genres
    raw_gen = row.get("genres")
    if isinstance(raw_gen, list):
        genres = raw_gen
    elif pd.isna(raw_gen):
        genres = []
    else:
        # split on comma, slash, ampersand
        parts = re.split(r"[,&/]", raw_gen)
        genres = [p.strip() for p in parts if p.strip()]
    # 5) Uniqueness
    key = (title, year)
    if key in seen:
        invalid_entries.append({"row": orig, "reason": "duplicate title+year"})
        continue
    seen.add(key)
    # Append valid
    output.append({
        "ID": ID,
        "title": title,
        "release_year": int(year),
        "genres": genres,
        "rating": float(rating)
    })
# At this point, `output` has the cleaned rows and `invalid_entries` the skipped ones.