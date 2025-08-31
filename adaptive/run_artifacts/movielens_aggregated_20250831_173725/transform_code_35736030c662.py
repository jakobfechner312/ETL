import pandas as pd
import re
import string
from unidecode import unidecode

output = []
invalid_entries = []
seen = set()

for idx, row in df.iterrows():
    orig = row.to_dict()

    # release_year with two-digit handling
    year = row.get('release_year', pd.NA)
    if pd.isna(year) and 'streaming_release_year' in row:
        year = row['streaming_release_year']
    try:
        year = int(year)
    except:
        invalid_entries.append({'row': orig, 'reason': 'invalid year'})
        continue
    if 0 <= year < 100:
        year = year + (2000 if year <= 25 else 1900)
    if not (1870 <= year <= 2025):
        invalid_entries.append({'row': orig, 'reason': 'year out of range'})
        continue

    # title normalization
    title = unidecode(str(row['title'])).lower()
    title = re.sub(r'\(\d{4}\)\s*$', '', title)
    title = re.sub(r'\([^)]*\)\s*$', '', title)
    trans = str.maketrans(string.punctuation, ' '*len(string.punctuation))
    title = title.translate(trans)
    title = re.sub(r'\s+', ' ', title).strip()
    if title.endswith(' the'):
        title = title[:-4].rstrip()
    toks = title.split()
    cleaned_tokens = []
    for t in toks:
        if t not in cleaned_tokens:
            cleaned_tokens.append(t)
    title = ' '.join(cleaned_tokens)
    if not title:
        invalid_entries.append({'row': orig, 'reason': 'empty title'})
        continue

    # unique (title, year)
    key = (title, year)
    if key in seen:
        invalid_entries.append({'row': orig, 'reason': 'duplicate title-year'})
        continue
    seen.add(key)

    # genres → list
    raw_genres = row.get('genres', '')
    genres = [g for g in str(raw_genres).split('|')
              if g and g.lower() != '(no genres listed)']

    # rating
    rating = row.get('average_rating', pd.NA)
    if pd.isna(rating):
        invalid_entries.append({'row': orig, 'reason': 'missing rating'})
        continue
    try:
        rating = float(rating)
    except:
        invalid_entries.append({'row': orig, 'reason': 'invalid rating'})
        continue

    # append cleaned row
    output.append({
        'ID': int(row['ID_MOVIELENS']),
        'title': title,
        'release_year': year,
        'genres': genres,
        'rating': rating
    })