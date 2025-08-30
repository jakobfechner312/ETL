import pandas as pd
import numpy as np
import re
import unicodedata

# Assume df is your original DataFrame
# df = pd.read_csv(...)

output = []
invalid_entries = []
seen_title_year = set()

# Helper: clean title
def clean_title(raw):
    # 1) to ASCII, lowercase
    s = unicodedata.normalize('NFKD', raw).encode('ascii', 'ignore').decode('ascii').lower()
    # 2) remove trailing "(YYYY)"
    s = re.sub(r'\(\d{4}\)\s*$', '', s)
    # 3) remove one trailing "(...)" if any
    s = re.sub(r'\([^)]*\)\s*$', '', s)
    # 4) punctuation -> space (keep a–z, 0–9, space)
    s = re.sub(r'[^a-z0-9\s]', ' ', s)
    # 5) collapse spaces, trim
    s = re.sub(r'\s+', ' ', s).strip()
    # 6) drop trailing "the"
    if s.endswith(' the'):
        s = s[:-4].strip()
    # 7) dedupe tokens, keep order
    tokens = s.split()
    seen = set()
    dedup = []
    for t in tokens:
        if t not in seen:
            dedup.append(t)
            seen.add(t)
    return ' '.join(dedup)

# Helper: parse year with 2- or 4-digit handling
def parse_year(date_str):
    # try extract 4-digit year or 2-digit
    m = re.search(r'(\d{2,4})\s*$', date_str)
    if not m:
        return None
    y = int(m.group(1))
    if y < 100:
        # two-digit: prefer 2000s if <=2025 else 1900s
        cand = 2000 + y
        y = cand if cand <= 2025 else 1900 + y
    return y

# Helper: choose rating (float, no NaN)
def choose_rating(row):
    # prefer metascore -> scale 0–100 to 0–10
    if pd.notna(row['metascore']):
        return row['metascore'] / 10.0
    # fallback to userscore
    try:
        us = float(row['userscore'])
        return us
    except:
        return np.nan

for _, row in df.iterrows():
    orig = row.to_dict()
    # 1) ID_METACRITIC
    movie_id = row['ID_METACRITIC']
    # 2) title
    if not isinstance(row['movie_title'], str) or not row['movie_title'].strip():
        invalid_entries.append({'row': orig, 'reason': 'empty title'})
        continue
    title = clean_title(row['movie_title'])
    if not title:
        invalid_entries.append({'row': orig, 'reason': 'title cleaned to empty'})
        continue
    # 3) release_year
    year = parse_year(row['release_date'])
    if pd.isna(year) or year < 1870 or year > 2025:
        invalid_entries.append({'row': orig, 'reason': f'invalid year {year}'})
        continue
    # 4) uniqueness
    key = (title, year)
    if key in seen_title_year:
        invalid_entries.append({'row': orig, 'reason': 'duplicate title+year'})
        continue
    seen_title_year.add(key)
    # 5) genres
    genres = []
    if isinstance(row['genre'], str) and row['genre'].strip():
        genres = [g.strip() for g in row['genre'].split(',') if g.strip()]
    # 6) rating
    rating = choose_rating(row)
    if pd.isna(rating):
        invalid_entries.append({'row': orig, 'reason': 'no valid rating'})
        continue
    # 7) assemble
    output.append({
        'ID': movie_id,
        'title': title,
        'release_year': year,
        'genres': genres,
        'rating': rating
    })

# At the end you'll have:
#  - output: list of cleaned movie dicts
#  - invalid_entries: list of {'row':..., 'reason':...}