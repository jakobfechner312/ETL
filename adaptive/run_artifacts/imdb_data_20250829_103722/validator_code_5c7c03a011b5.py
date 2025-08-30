import re
import math
import pandas as pd
import numpy as np

valid_output = []
invalid_entries = []
seen = set()
duplicate_count = 0

for record in output:
    errors = []
    # Title validation & cleaning
    title = record.get("title")
    if not isinstance(title, str) or not title.strip():
        errors.append("title missing or not a non-empty string")
        cleaned_title = ""
    else:
        cleaned_title = re.sub(r"[^a-z0-9\s]", "", title.lower()).strip()
        if not cleaned_title:
            errors.append("title empty after cleaning")
    # Release year validation
    ry = record.get("release_year")
    if pd.isna(ry) or not isinstance(ry, (int, np.integer)):
        errors.append("release_year missing or not an integer")
    else:
        if not (1870 <= int(ry) <= 2025):
            errors.append(f"release_year {ry} out of realistic range")
    # Rating validation
    rating = record.get("rating")
    if rating is None or not isinstance(rating, (int, float)) or (isinstance(rating, float) and math.isnan(rating)):
        errors.append("rating missing or NaN")
    # Genres validation (optional)
    genres = record.get("genres", None)
    if genres is not None:
        if not isinstance(genres, list) or not all(isinstance(g, str) for g in genres):
            errors.append("genres must be a list of strings if present")
    # Decide valid vs invalid
    if errors:
        invalid_entries.append({"record": record, "errors": errors})
    else:
        key = (cleaned_title, record["release_year"])
        if key in seen:
            duplicate_count += 1
        else:
            seen.add(key)
            # prefer cleaned title for output
            record["title"] = cleaned_title
            valid_output.append(record)

assert not invalid_entries, f"Invalid entries found: {invalid_entries}"