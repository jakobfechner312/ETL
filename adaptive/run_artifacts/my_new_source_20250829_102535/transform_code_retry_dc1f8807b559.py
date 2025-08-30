import pandas as pd
import re
import unicodedata
import string

# Assuming df is your DataFrame
output = []
invalid_entries = []

def normalize_title(title: str) -> str:
    # 1. to ascii, lowercase
    t = unicodedata.normalize('NFKD', title).encode('ascii', 'ignore').decode('ascii', 'ignore')
    t = t.lower().strip()
    # 2. remove ONE trailing "(YYYY)"
    t = re.sub(r'\(\d{4}\)$', '', t).strip()
    # 3. remove ONE trailing "(...)" any content
    t = re.sub(r'\([^)]*\)$', '', t).strip()
    # 4. punctuation → space
    pattern = '[' + re.escape(string.punctuation) + ']'
    t = re.sub(pattern, ' ', t)
    # 5. collapse spaces, trim
    t = re.sub(r'\s+', ' ', t).strip()
    # 6. drop trailing "the"
    t = re.sub(r'\bthe$', '', t).strip()
    # 7. dedup tokens, keep order
    tokens = t.split()
    seen = set()
    dedup = []
    for tok in tokens:
        if tok not in seen:
            dedup.append(tok)
            seen.add(tok)
    return " ".join(dedup)

def parse_year(col_year, col_date):
    year = None
    # try numeric column
    if pd.notna(col_year):
        try:
            year = int(col_year)
        except:
            pass
    # fallback to date string
    if year is None and pd.notna(col_date):
        # look for last 1–4 digits
        m = re.search(r'(\d{1,4})$', col_date.strip())
        if m:
            try:
                year = int(m.group(1))
            except:
                year = None
    # interpret two-digit
    if isinstance(year, int) and year < 100:
        if year <= 25:
            year += 2000
        else:
            year += 1900
    return year

seen_title_year = set()

for _, row in df.iterrows():
    orig_row = row.to_dict()
    # preserve ID_DATA
    ID = row.get('ID_DATA')
    # rating: prefer averageRating
    rating = row.get('averageRating', pd.NA)
    if pd.isna(rating):
        invalid_entries.append({"row": orig_row, "reason": "missing rating"})
        continue

    # get or extract release_year
    ry = row.get('release_year', pd.NA) if 'release_year' in row else pd.NA
    rd = row.get('release_date', pd.NA)
    year = parse_year(ry, rd)
    # validate year realistic 1870-2025
    if year is None or year < 1870 or year > 2025:
        invalid_entries.append({"row": orig_row, "reason": f"invalid year {year}"})
        continue

    # choose title: prefer originalTitle
    raw_title = row.get('originalTitle') or row.get('primaryTitle') or ''
    cleaned = normalize_title(raw_title)
    if not cleaned:
        invalid_entries.append({"row": orig_row, "reason": "empty title after clean"})
        continue

    # enforce uniqueness of (title, year)
    key = (cleaned, year)
    if key in seen_title_year:
        invalid_entries.append({"row": orig_row, "reason": "duplicate title-year"})
        continue
    seen_title_year.add(key)

    # genres → list[str]
    g = row.get('genres', '')
    if pd.isna(g) or not g:
        genres_list = []
    else:
        # split on comma, strip, drop empties
        genres_list = [x.strip() for x in g.split(',') if x.strip()]

    # build output record
    rec = {
        "ID": ID,
        "title": cleaned,
        "release_year": year,
        "genres": genres_list,
        "rating": float(rating)
    }
    output.append(rec)

# At this point:
# `output` contains cleaned rows
# `invalid_entries` contains skipped rows with reasons