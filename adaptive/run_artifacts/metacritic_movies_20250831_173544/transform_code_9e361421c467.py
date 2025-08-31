import re
from unidecode import unidecode
import pandas as pd

# assume df is your DataFrame
output = []
invalid_entries = []
seen_titles = set()

for _, row in df.iterrows():
    original = row.to_dict()
    # Preserve the Metacritic ID as our ID
    ID = row["ID_METACRITIC"]

    # 1) Parse and validate release_year (including two-digit years)
    try:
        # this will interpret 02 as 2002, 10 as 2010, etc.
        dt = pd.to_datetime(row["release_date"], format="%d-%b-%y", dayfirst=True)
        year = dt.year
    except Exception:
        invalid_entries.append({"row": original, "reason": "invalid release_date"})
        continue
    if not (1870 <= year <= 2025):
        invalid_entries.append({"row": original, "reason": f"year {year} out of range"})
        continue

    # 2) Normalize title
    title_raw = str(row["movie_title"])
    title_norm = unidecode(title_raw)                   # ascii
    title_norm = title_norm.lower()                      # lowercase
    # remove one trailing "(YYYY)"
    title_norm = re.sub(r"\(\d{4}\)\s*$", "", title_norm)
    # remove one trailing "(...)" if any
    title_norm = re.sub(r"\([^)]+\)\s*$", "", title_norm)
    # punctuation → space
    title_norm = re.sub(r"[^\w\s]", " ", title_norm)
    # collapse spaces & trim
    title_norm = re.sub(r"\s+", " ", title_norm).strip()
    # drop trailing "the"
    title_norm = re.sub(r"\bthe$", "", title_norm).strip()
    # dedup tokens in order
    tokens = []
    for tk in title_norm.split():
        if tk not in tokens:
            tokens.append(tk)
    title_norm = " ".join(tokens)

    if not title_norm:
        invalid_entries.append({"row": original, "reason": "empty title after cleanup"})
        continue
    if (title_norm, year) in seen_titles:
        invalid_entries.append({"row": original, "reason": "duplicate title/year"})
        continue
    seen_titles.add((title_norm, year))

    # 3) Split genres
    raw_genres = str(row.get("genre", ""))
    genres = [g.strip() for g in raw_genres.split(",") if g.strip()]

    # 4) Determine rating: prefer metascore, fallback to userscore
    metascore = row.get("metascore", pd.NA)
    rating = None
    if pd.notnull(metascore) and metascore != pd.NA and metascore > 0:
        rating = float(metascore) / 10.0
    else:
        try:
            rating = float(row.get("userscore", ""))
        except Exception:
            invalid_entries.append({"row": original, "reason": "no valid rating"})
            continue

    # 5) Assemble cleaned record
    output.append({
        "ID": ID,
        "title": title_norm,
        "release_year": year,
        "genres": genres,
        "rating": rating
    })

# `output` holds the cleaned rows; `invalid_entries` holds any skipped rows