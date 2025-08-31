import pandas as pd
import re
import unicodedata
import string

# assume your DataFrame is called df
output = []
invalid_entries = []
seen_titles_year = set()

# helper: convert two‐digit year to full year
def normalize_year(y_str):
    y = int(y_str)
    if y < 100:
        # map 00–25 to 2000–2025, 26–99 to 1926–1999
        if y <= 25:
            y += 2000
        else:
            y += 1900
    return y

# punctuation→space translator
punct_map = str.maketrans({p: " " for p in string.punctuation})

for _, row in df.iterrows():
    orig = row.to_dict()
    # 1) ID
    ID = orig.get("ID_METACRITIC")
    # 2) release_year
    date = orig.get("release_date", "")
    parts = date.split("-")
    if len(parts) < 3:
        invalid_entries.append({"row": orig, "reason": "bad release_date"})
        continue
    y_str = parts[-1]
    try:
        year = normalize_year(y_str)
    except:
        invalid_entries.append({"row": orig, "reason": "invalid year part"})
        continue
    if year < 1870 or year > 2025:
        invalid_entries.append({"row": orig, "reason": "year out of range"})
        continue
    # 3) title cleaning
    raw = orig.get("movie_title", "")
    if not isinstance(raw, str) or not raw.strip():
        invalid_entries.append({"row": orig, "reason": "empty title"})
        continue
    t = raw
    # remove trailing (YYYY)
    t = re.sub(r"\(\s*\d{4}\s*\)$", "", t)
    # remove one more trailing (...)
    t = re.sub(r"\(\s*[^)]*\s*\)$", "", t)
    # normalize to ascii
    t = unicodedata.normalize("NFKD", t).encode("ascii", "ignore").decode("ascii")
    # punctuation→space
    t = t.translate(punct_map)
    # collapse spaces, lowercase
    t = re.sub(r"\s+", " ", t).strip().lower()
    # drop trailing "the"
    if t.endswith(" the"):
        t = t[: -len(" the")].rstrip()
    # dedupe tokens
    tokens = t.split()
    seen = set()
    dedup = []
    for tok in tokens:
        if tok not in seen:
            seen.add(tok)
            dedup.append(tok)
    title_clean = " ".join(dedup)
    if not title_clean:
        invalid_entries.append({"row": orig, "reason": "title cleaned empty"})
        continue
    # uniqueness check
    key = (title_clean, year)
    if key in seen_titles_year:
        invalid_entries.append({"row": orig, "reason": "duplicate title/year"})
        continue
    seen_titles_year.add(key)
    # 4) genres
    g = orig.get("genre", "")
    if pd.isna(g) or not isinstance(g, str):
        genres = []
    else:
        genres = [x.strip() for x in g.split(",") if x.strip()]
    # 5) rating
    rating = None
    # prefer metascore if present
    if pd.notna(orig.get("metascore")):
        rating = float(orig["metascore"])
    else:
        us = orig.get("userscore", "")
        try:
            rating = float(us)
        except:
            invalid_entries.append({"row": orig, "reason": "no valid rating"})
            continue
    # assemble
    output.append({
        "ID": ID,
        "title": title_clean,
        "release_year": year,
        "genres": genres,
        "rating": rating
    })
# at the end you have:
#   output       : list of cleaned movie dicts
#   invalid_entries: list of {row:..., reason:...} for skipped rows