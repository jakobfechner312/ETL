import re
import math
import pandas as pd

valid_output = []
invalid_entries = []
duplicate_count = 0
seen_titles = set()

for entry in output:
    errors = []
    cleaned_entry = entry.copy()

    # ID check
    if not isinstance(entry.get("ID"), int):
        errors.append("ID must be int")

    # Title check & clean
    title = entry.get("title")
    if not isinstance(title, str) or not title.strip():
        errors.append("title must be non-empty string")
    else:
        cleaned_title = re.sub(r"[^a-z0-9 ]", "", title.lower()).strip()
        if not cleaned_title:
            errors.append("title cleaned to empty")
        cleaned_entry["title"] = cleaned_title

    # Release year check
    ry = entry.get("release_year")
    if ry is pd.NA or ry is None:
        cleaned_entry["release_year"] = pd.NA
    elif isinstance(ry, int):
        if ry < 1870 or ry > 2025:
            errors.append("release_year out of realistic range")
    else:
        errors.append("release_year must be int or pd.NA")

    # Genres check
    genres = entry.get("genres", [])
    if genres is None:
        cleaned_entry["genres"] = []
    elif isinstance(genres, list) and all(isinstance(g, str) for g in genres):
        cleaned_entry["genres"] = genres
    else:
        errors.append("genres must be list of str if present")

    # Rating check
    rating = entry.get("rating")
    if rating is None or not isinstance(rating, (int, float)) or (isinstance(rating, float) and math.isnan(rating)):
        errors.append("rating must be valid float")
    else:
        cleaned_entry["rating"] = float(rating)

    # Duplicate check
    key = (cleaned_entry.get("title"), cleaned_entry.get("release_year"))
    if key in seen_titles:
        duplicate_count += 1
        continue

    if errors:
        invalid_entries.append({"entry": entry, "errors": errors})
    else:
        seen_titles.add(key)
        valid_output.append(cleaned_entry)

if invalid_entries:
    raise AssertionError(f"Invalid entries found: {invalid_entries}")