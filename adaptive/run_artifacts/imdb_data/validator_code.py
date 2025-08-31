import re
import math
import pandas as pd

valid_output = []
invalid_entries = []
duplicate_count = 0
seen_titles = set()

for entry in output:
    errors = []
    # Validate ID
    if 'ID' not in entry or not isinstance(entry['ID'], int):
        errors.append("ID missing or not int")
    # Validate title
    title = entry.get('title')
    if not isinstance(title, str) or not title.strip():
        errors.append("title missing or empty")
    else:
        # Clean title: lowercase, remove non-alphanumeric/spaces, collapse spaces
        cleaned = re.sub(r'[^a-z0-9\s]', '', title.lower()).strip()
        cleaned = re.sub(r'\s+', ' ', cleaned)
        if title != cleaned:
            errors.append(f"title not cleaned: expected '{cleaned}'")
    # Validate release_year
    year = entry.get('release_year')
    if not pd.isna(year):
        if not isinstance(year, int) or not (1870 <= year <= 2025):
            errors.append(f"release_year out of range or not int: {year}")
    # Validate genres if present
    if 'genres' in entry:
        genres = entry['genres']
        if not isinstance(genres, list) or any(not isinstance(g, str) for g in genres):
            errors.append("genres not a list of strings")
    # Validate rating
    rating = entry.get('rating')
    if rating is None or not isinstance(rating, (int, float)) or (isinstance(rating, float) and math.isnan(rating)):
        errors.append(f"rating missing or NaN: {rating}")
    # Check uniqueness of (title, release_year)
    key = (title, year)
    if key in seen_titles:
        duplicate_count += 1
        errors.append("duplicate title/release_year")
    else:
        seen_titles.add(key)
    # Collect entry
    if errors:
        invalid_entries.append({'entry': entry, 'errors': errors})
    else:
        valid_output.append(entry)

if invalid_entries:
    raise AssertionError(f"Invalid entries found: {invalid_entries}")