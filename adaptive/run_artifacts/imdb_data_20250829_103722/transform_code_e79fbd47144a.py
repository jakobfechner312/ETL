import pandas as pd
import re
import unicodedata

# Assume df is the input DataFrame
def normalize_title(raw_title):
    # Prefer original title passed in
    # 1) ascii + lowercase
    title = unicodedata.normalize('NFKD', raw_title)
    title = title.encode('ascii', 'ignore').decode('ascii')
    title = title.lower()
    # 2) remove trailing "(YYYY)"
    title = re.sub(r'\(\d{4}\)\s*$', '', title)
    # 3) remove any one trailing "(...)" at end
    title = re.sub(r'\([^)]*\)\s*$', '', title)
    # 4) punctuation → space
    title = re.sub(r'[^\w\s]', ' ', title)
    # 5) collapse spaces + trim
    title = re.sub(r'\s+', ' ', title).strip()
    # 6) drop trailing "the"
    title = re.sub(r'\bthe$', '', title).strip()
    # 7) dedup tokens, keep order
    tokens = title.split()
    seen = set()
    deduped = []
    for t in tokens:
        if t not in seen:
            seen.add(t)
            deduped.append(t)
    return " ".join(deduped)

output = []
invalid_entries = []
seen_titles = set()

for _, row in df.iterrows():
    orig = row.to_dict()
    # 1) ID_IMDB unchanged
    ID = row['ID_IMDB']
    # 2) rating: prefer averageRating
    rating = row.get('averageRating', pd.NA)
    if pd.isna(rating):
        invalid_entries.append({"row": orig, "reason": "missing rating"})
        continue
    # 3) release_year: extract from release_date
    rd = row.get('release_date', None)
    if not isinstance(rd, str) or not rd.strip():
        invalid_entries.append({"row": orig, "reason": "missing release_date"})
        continue
    m = re.search(r'(\d{2,4})\s*$', rd.strip())
    if not m:
        invalid_entries.append({"row": orig, "reason": "cannot parse year"})
        continue
    year = int(m.group(1))
    # interpret two-digit
    if year < 100:
        year += 2000
    # realistic range 1870-2025
    if year < 1870 or year > 2025:
        invalid_entries.append({"row": orig, "reason": f"invalid release_year {year}"})
        continue
    # 4) title: prefer originalTitle
    raw_title = row.get('originalTitle', '') or row.get('primaryTitle', '')
    clean_title = normalize_title(raw_title)
    if not clean_title:
        invalid_entries.append({"row": orig, "reason": "empty title after cleaning"})
        continue
    # unique on (title, year)
    key = (clean_title, year)
    if key in seen_titles:
        invalid_entries.append({"row": orig, "reason": "duplicate title, release_year"})
        continue
    seen_titles.add(key)
    # 5) genres: list of strings
    genres_raw = row.get('genres', '')
    if isinstance(genres_raw, str) and genres_raw.strip() and genres_raw != '\\N':
        genres = [g for g in genres_raw.split(',') if g]
    else:
        genres = []
    # build output record
    rec = {
        "ID": ID,
        "title": clean_title,
        "release_year": year,
        "genres": genres,
        "rating": float(rating)
    }
    output.append(rec)

# 'output' holds cleaned rows; 'invalid_entries' holds skipped rows with reasons