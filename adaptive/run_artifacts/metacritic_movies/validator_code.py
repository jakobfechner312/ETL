import re
import pandas as pd

valid_output = []
invalid_entries = []
duplicate_count = 0
seen = set()

for entry in output:
    errors = []
    new_entry = entry.copy()

    # Check ID
    if 'ID' not in entry or not isinstance(entry['ID'], int):
        errors.append("Invalid or missing ID")

    # Validate and clean title
    title = entry.get('title')
    if not isinstance(title, str) or not title.strip():
        errors.append("Title missing or empty")
    else:
        cleaned = title.lower()
        cleaned = re.sub(r'[^a-z0-9\s]', '', cleaned)
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        if not cleaned:
            errors.append("Title cleaned to empty")
        else:
            new_entry['title'] = cleaned

    # Validate release_year
    ry = entry.get('release_year')
    if pd.isna(ry):
        pass
    elif isinstance(ry, int):
        if ry < 1870 or ry > 2025:
            errors.append(f"release_year {ry} out of range")
    else:
        errors.append("Invalid release_year")

    # Validate genres if present
    if 'genres' in entry:
        genres = entry['genres']
        if not isinstance(genres, list) or not all(isinstance(g, str) for g in genres):
            errors.append("Invalid genres list")

    # Validate rating
    rating = entry.get('rating')
    if not isinstance(rating, (int, float)) or pd.isna(rating):
        errors.append("Invalid or missing rating")

    # Check duplicates (only if previous validations passed)
    key = (new_entry.get('title'), ry)
    if not errors:
        if key in seen:
            duplicate_count += 1
            errors.append("Duplicate title and release_year")
        else:
            seen.add(key)

    # Record entry in appropriate list
    if errors:
        invalid_entries.append({'entry': entry, 'errors': errors})
    else:
        valid_output.append(new_entry)

# Final assertion
assert not invalid_entries, f"Invalid entries found: {invalid_entries}"