import re
import pandas as pd
import math

valid_output = []
invalid_entries = []
duplicate_count = 0
seen = set()

for entry in output:
    errors = []

    # ID check
    if 'ID' not in entry or not isinstance(entry['ID'], int):
        errors.append("ID missing or not int")

    # Title check and cleaning
    title = entry.get('title')
    if not title or not isinstance(title, str) or not title.strip():
        errors.append("title missing or empty")
    else:
        cleaned_title = title.lower()
        cleaned_title = re.sub(r'[^a-z0-9 ]+', '', cleaned_title)
        cleaned_title = re.sub(r'\s+', ' ', cleaned_title).strip()

    # Release year check
    year = entry.get('release_year')
    if year is pd.NA or year is None or (isinstance(year, float) and pd.isna(year)):
        errors.append("release_year missing")
    elif not isinstance(year, int) or not (1870 <= year <= 2025):
        errors.append("release_year invalid")

    # Genres check (optional)
    genres = entry.get('genres', [])
    if not isinstance(genres, list) or not all(isinstance(g, str) for g in genres):
        errors.append("genres invalid")

    # Rating check
    rating = entry.get('rating')
    if not isinstance(rating, (int, float)) or (isinstance(rating, float) and math.isnan(rating)):
        errors.append("rating missing or NaN")

    # If any errors, collect and skip
    if errors:
        invalid_entries.append({"entry": entry, "errors": errors})
        continue

    # Duplicate check on (title, release_year)
    key = (cleaned_title, year)
    if key in seen:
        duplicate_count += 1
        invalid_entries.append({"entry": entry, "errors": ["duplicate title and release_year"]})
        continue
    seen.add(key)

    # Build cleaned entry
    valid_output.append({
        "ID": entry["ID"],
        "title": cleaned_title,
        "release_year": year,
        "genres": genres,
        "rating": float(rating)
    })

# Raise if any invalid entries found
if invalid_entries:
    raise AssertionError(f"Invalid entries found: {invalid_entries}")