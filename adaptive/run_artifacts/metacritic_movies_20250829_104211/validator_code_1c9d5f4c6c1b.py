import re
import math
import pandas as pd

valid_output = []
invalid_entries = []
duplicate_count = 0
seen = set()

for idx, row in enumerate(output):
    errors = []
    # ID check
    if 'ID' not in row or not isinstance(row['ID'], int):
        errors.append('Invalid ID')
    # title check and clean
    title = row.get('title')
    if not isinstance(title, str) or not title.strip():
        errors.append('Title must be a non-empty string')
    else:
        cleaned = re.sub(r'[^a-zA-Z0-9\s]', '', title).lower().strip()
        if not cleaned:
            errors.append('Title empty after cleaning')
        else:
            row['title'] = cleaned
    # release_year check
    ry = row.get('release_year')
    if ry is not None and ry is not pd.NA:
        if not isinstance(ry, int):
            errors.append('release_year must be int or pd.NA')
        elif ry < 1870 or ry > 2025:
            errors.append('release_year out of realistic bounds')
    # rating check
    rating = row.get('rating')
    if not isinstance(rating, (int, float)) or (isinstance(rating, float) and math.isnan(rating)):
        errors.append('Invalid rating')
    # genres check
    genres = row.get('genres')
    if genres is not None:
        if not isinstance(genres, list) or not all(isinstance(g, str) for g in genres):
            errors.append('Invalid genres')
    # duplicate check
    key = (row.get('title'), row.get('release_year'))
    if key in seen:
        duplicate_count += 1
        continue
    seen.add(key)
    # finalize
    if errors:
        invalid_entries.append({'index': idx, 'ID': row.get('ID'), 'errors': errors})
    else:
        valid_output.append(row)

if invalid_entries:
    raise AssertionError(f"Invalid entries found: {invalid_entries}")