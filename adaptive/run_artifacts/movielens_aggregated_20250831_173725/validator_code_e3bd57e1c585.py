import re
import math
import pandas as pd

valid_output = []
invalid_entries = []
seen_keys = set()
duplicate_count = 0

for rec in output:
    errors = []
    rec_id = rec.get('ID')
    if rec_id is None:
        errors.append("Missing ID")
    # Title: must not be empty, cleaned, unique with release_year
    title = rec.get('title')
    if not title or not isinstance(title, str):
        errors.append("Title is empty or not a string")
        clean_title = ""
    else:
        clean_title = re.sub(r'[^a-z0-9 ]', '', title.lower()).strip()
        if not clean_title:
            errors.append("Cleaned title is empty")
    rec['title'] = clean_title
    # Release year: NA or 1870-2025
    ry = rec.get('release_year')
    if pd.isna(ry):
        rec['release_year'] = pd.NA
    elif isinstance(ry, (int, float)):
        ry_int = int(ry)
        if not (1870 <= ry_int <= 2025):
            errors.append(f"Invalid release_year: {ry}")
        else:
            rec['release_year'] = ry_int
    else:
        errors.append(f"release_year must be int or NA, got {type(ry).__name__}")
    # Rating: must never be NaN
    rating = rec.get('rating')
    if rating is None or (isinstance(rating, float) and math.isnan(rating)):
        errors.append("Rating is NaN or missing")
    # Genres: optional list[str]
    genres = rec.get('genres')
    if genres is not None:
        if not isinstance(genres, list) or not all(isinstance(g, str) for g in genres):
            errors.append("Genres must be a list of strings if present")
    # Uniqueness check
    key = (rec['title'], rec.get('release_year'))
    if key in seen_keys:
        duplicate_count += 1
    else:
        seen_keys.add(key)
    # Classify record
    if errors:
        invalid_entries.append({"ID": rec_id, "errors": errors})
    else:
        valid_output.append(rec)

if invalid_entries:
    raise AssertionError(f"Validation failed for entries: {invalid_entries}")