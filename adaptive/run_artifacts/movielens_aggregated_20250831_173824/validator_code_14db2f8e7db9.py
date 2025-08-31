import re
import pandas as pd
import numpy as np

valid_output = []
invalid_entries = []
seen_titles = set()
duplicate_count = 0

for idx, row in enumerate(output):
    errors = []
    validated = {}

    # ID
    id_val = row.get("ID")
    if not isinstance(id_val, int):
        errors.append("ID must be int")
    else:
        validated["ID"] = id_val

    # title
    title = row.get("title")
    if not isinstance(title, str) or not title.strip():
        errors.append("title must be non-empty str")
    else:
        cleaned = title.lower()
        cleaned = re.sub(r"[^a-z0-9\s]", "", cleaned)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        if not cleaned:
            errors.append("title cleaned to empty")
        else:
            validated["title"] = cleaned

    # release_year
    ry = row.get("release_year")
    if pd.isna(ry):
        validated["release_year"] = pd.NA
    elif isinstance(ry, (int, np.integer)):
        if 1870 <= int(ry) <= 2025:
            validated["release_year"] = int(ry)
        else:
            errors.append("release_year out of realistic range")
    else:
        errors.append("release_year must be int or NA")

    # genres
    genres = row.get("genres", None)
    if genres is None:
        validated["genres"] = []
    elif isinstance(genres, list) and all(isinstance(g, str) for g in genres):
        validated["genres"] = genres
    else:
        errors.append("genres must be list[str] if present")

    # rating
    rating = row.get("rating")
    if rating is None or pd.isna(rating):
        errors.append("rating must not be NaN")
    else:
        try:
            validated["rating"] = float(rating)
        except Exception:
            errors.append("rating must be convertable to float")

    # finalize
    if errors:
        invalid_entries.append({"index": idx, "errors": errors, "row": row})
    else:
        key = (validated["title"], validated["release_year"])
        if key in seen_titles:
            duplicate_count += 1
        else:
            seen_titles.add(key)
            valid_output.append(validated)

assert not invalid_entries, f"Invalid entries found: {invalid_entries}"