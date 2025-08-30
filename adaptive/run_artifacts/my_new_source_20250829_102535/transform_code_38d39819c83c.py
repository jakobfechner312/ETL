import pandas as pd
import re
import unicodedata

# assume your DataFrame is called df
# df = pd.read_csv(...)

output = []
invalid_entries = []
seen_titles = set()

def clean_title(raw):
    # normalize to ascii, lowercase
    s = unicodedata.normalize('NFKD', raw).encode('ascii', 'ignore').decode('ascii')
    s = s.lower().strip()
    # remove trailing "(YYYY)"
    s = re.sub(r'\(\d{4}\)\s*$', '', s)
    # remove any trailing "(...)" 
    s = re.sub(r'\([^)]*\)\s*$', '', s)
    # punctuation→space
    s = re.sub(r'[^\w\s]', ' ', s)
    # collapse spaces
    s = re.sub(r'\s+', ' ', s).strip()
    # drop trailing " the"
    if s.endswith(' the'):
        s = s[:-4].strip()
    # dedupe tokens
    tokens = s.split()
    seen = set()
    dedup = []
    for t in tokens:
        if t not in seen:
            seen.add(t)
            dedup.append(t)
    return " ".join(dedup)

def interpret_year(y):
    # two-digit → 1900+
    if 0 <= y < 100:
        y += 1900
    return y

for idx, row in df.to_dict('records'):
    # preserve raw row for error reporting
    raw = row.copy()

    # ID_DATA
    id_data = row.get('ID_DATA')

    # extract release_year: try numeric column first
    ry = row.get('release_year') if 'release_year' in row else pd.NA
    if pd.isna(ry):
        # parse from release_date
        rd = row.get('release_date')
        try:
            ry = pd.to_datetime(rd, errors='coerce').year
        except:
            ry = pd.NA
    if pd.isna(ry):
        invalid_entries.append({"row": raw, "reason": "missing release_year"})
        continue
    ry = int(ry)
    ry = interpret_year(ry)
    if not (1870 <= ry <= 2025):
        invalid_entries.append({"row": raw, "reason": f"unrealistic year {ry}"})
        continue

    # rating
    rating = row.get('averageRating')
    if pd.isna(rating):
        invalid_entries.append({"row": raw, "reason": "missing rating"})
        continue
    rating = float(rating)

    # genres
    g = row.get('genres')
    if pd.isna(g) or not g:
        genres = []
    else:
        genres = [x.strip() for x in str(g).split(',') if x.strip()]

    # title: prefer originalTitle
    title_src = row.get('originalTitle') or row.get('primaryTitle') or ''
    title_clean = clean_title(title_src)
    if not title_clean:
        invalid_entries.append({"row": raw, "reason": "empty title after cleaning"})
        continue

    # uniqueness check on (title, year)
    key = (title_clean, ry)
    if key in seen_titles:
        invalid_entries.append({"row": raw, "reason": "duplicate title+year"})
        continue
    seen_titles.add(key)

    # build output row
    output.append({
        "ID": id_data,
        "title": title_clean,
        "release_year": ry,
        "genres": genres,
        "rating": rating
    })

# at end you have:
# output       -> list of cleaned dicts
# invalid_entries -> list of skipped rows with reasons