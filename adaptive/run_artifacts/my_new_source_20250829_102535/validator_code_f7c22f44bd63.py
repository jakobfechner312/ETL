import re
import pandas as pd
import math

valid_output = []
invalid_entries = []
seen_titles = set()
duplicate_count = 0

for entry in output:
    # Validate ID presence
    if 'ID' not in entry:
        invalid_entries.append((entry, 'missing ID'))
        continue

    # Validate and clean title
    title = entry.get('title')
    if not isinstance(title, str) or not title.strip():
        invalid_entries.append((entry, 'empty or missing title'))
        continue
    clean_title = re.sub(r'[^a-z0-9 ]', '', title.lower()).strip()
    if not clean_title:
        invalid_entries.append((entry, 'title cleans to empty'))
        continue

    # Validate release_year
    release_year = entry.get('release_year')
    if release_year is not pd.NA and release_year is not None:
        if not isinstance(release_year, int) or release_year < 1870 or release_year > 2025:
            invalid_entries.append((entry, f'invalid release_year {release_year}'))
            continue

    # Validate rating
    rating = entry.get('rating')
    if rating is None or pd.isna(rating) or (isinstance(rating, float) and math.isnan(rating)):
        invalid_entries.append((entry, 'invalid or missing rating'))
        continue

    # Validate genres if present
    genres = entry.get('genres')
    if genres is not None:
        if not isinstance(genres, list) or any(not isinstance(g, str) for g in genres):
            invalid_entries.append((entry, 'invalid genres'))
            continue

    # Check duplicates based on (clean_title, release_year)
    key = (clean_title, release_year)
    if key in seen_titles:
        duplicate_count += 1
        continue
    seen_titles.add(key)

    # Build cleaned entry
    cleaned = entry.copy()
    cleaned['title'] = clean_title
    valid_output.append(cleaned)

if invalid_entries:
    raise AssertionError(f"Invalid entries found: {invalid_entries}")