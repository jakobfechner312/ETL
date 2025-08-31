import pandas as pd
import re

valid_output = []
invalid_entries = []
seen_titles = set()
duplicate_count = 0

for entry in output:
    errors = []
    # Check ID
    if not isinstance(entry.get("ID"), int):
        errors.append("ID must be int")
    # Check and clean title
    raw_title = entry.get("title", "")
    if not isinstance(raw_title, str) or not raw_title.strip():
        errors.append("title must be non-empty string")
        cleaned_title = ""
    else:
        cleaned_title = re.sub(r"[^a-z0-9\s]", "", raw_title.lower()).strip()
        if not cleaned_title:
            errors.append("cleaned title is empty")
    # Check release_year
    ry = entry.get("release_year")
    if pd.isna(ry):
        pass
    else:
        if not isinstance(ry, int):
            errors.append("release_year must be int or pd.NA")
        elif ry < 1870 or ry > 2025:
            errors.append("release_year out of realistic range")
    # Check rating
    rating = entry.get("rating")
    if rating is None or pd.isna(rating):
        errors.append("rating must be present and not NaN")
    elif not isinstance(rating, (int, float)):
        errors.append("rating must be numeric")
    # Check genres
    genres = entry.get("genres", None)
    if genres is not None:
        if not isinstance(genres, list) or not all(isinstance(g, str) for g in genres):
            errors.append("genres must be list of strings")
    # Validate or collect errors
    if errors:
        invalid_entries.append({"entry": entry, "errors": errors})
        continue
    # Handle duplicates
    dup_key = (cleaned_title, ry)
    if dup_key in seen_titles:
        duplicate_count += 1
        continue
    seen_titles.add(dup_key)
    # Update entry with cleaned title and collect
    entry["title"] = cleaned_title
    valid_output.append(entry)

assert not invalid_entries, f"Invalid entries found: {invalid_entries}"