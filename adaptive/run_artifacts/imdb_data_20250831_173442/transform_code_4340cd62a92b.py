import pandas as pd
import re
import unicodedata

# Assume df is the DataFrame from context
output = []
invalid_entries = []
seen = set()

def normalize_title(title):
    # ASCII, lowercase
    t = unicodedata.normalize("NFKD", title).encode("ascii", "ignore").decode()
    t = t.lower().strip()
    # Remove trailing "(YYYY)" then any "(...)" 
    t = re.sub(r"\(\s*\d{4}\s*\)$", "", t).strip()
    t = re.sub(r"\([^)]*\)$", "", t).strip()
    # Punctuation → space
    t = re.sub(r"[^\w\s]", " ", t)
    # Collapse spaces, trim
    t = re.sub(r"\s+", " ", t).strip()
    # Drop trailing " the"
    t = re.sub(r"\bthe$", "", t).strip()
    # Dedupe tokens, keep order
    tokens = t.split()
    seen_toks = set()
    uniq = []
    for tok in tokens:
        if tok not in seen_toks:
            uniq.append(tok)
            seen_toks.add(tok)
    return " ".join(uniq)

def parse_year(row):
    # Try release_date
    rd = row.get("release_date", "")
    if pd.notna(rd) and isinstance(rd, str):
        m = re.search(r"(\d{4})$", rd.strip())
        if m:
            y = int(m.group(1))
            return y
    # Fallback to streaming_release_year if present
    sry = row.get("streaming_release_year", pd.NA)
    if pd.notna(sry):
        y = int(sry)
        # two-digit?
        if 0 <= y < 100:
            y += 1900 if y >= 70 else 2000
        return y
    return pd.NA

def select_rating(row):
    # Prefer 'originalRating', else 'averageRating'
    for key in ["originalRating", "averageRating", "rating"]:
        val = row.get(key, pd.NA)
        if pd.notna(val):
            return float(val)
    return pd.NA

for _, row in df.iterrows():
    original = row.to_dict()
    # ID_IMDB
    ID = row["ID_IMDB"]
    # Year
    year = parse_year(original)
    if pd.isna(year) or not (1870 <= year <= 2025):
        invalid_entries.append({"row": original, "reason": "invalid or missing year"})
        continue
    # Rating
    rating = select_rating(original)
    if pd.isna(rating):
        invalid_entries.append({"row": original, "reason": "missing rating"})
        continue
    # Genres
    gens = original.get("genres", "")
    if pd.isna(gens) or gens == "":
        genres = []
    else:
        genres = [g.strip() for g in str(gens).split(",") if g.strip()]
    # Title: prefer originalTitle over primaryTitle
    raw_title = original.get("originalTitle") or original.get("primaryTitle") or ""
    title = normalize_title(raw_title)
    if not title:
        invalid_entries.append({"row": original, "reason": "empty title after cleaning"})
        continue
    # Check duplicates
    key = (title, year)
    if key in seen:
        invalid_entries.append({"row": original, "reason": "duplicate title+year"})
        continue
    seen.add(key)
    # Build cleaned record
    output.append({
        "ID": ID,
        "title": title,
        "release_year": int(year),
        "genres": genres,
        "rating": rating
    })
# After loop, `output` and `invalid_entries` are ready