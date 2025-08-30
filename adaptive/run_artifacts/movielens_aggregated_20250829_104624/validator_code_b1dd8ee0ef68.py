import re
import pandas as pd
import math

valid_output = []
invalid_entries = []
seen = set()
duplicate_count = 0

for entry in output:
    reasons = []
    # Check ID
    if not isinstance(entry.get('ID'), int):
        reasons.append("ID is not int")
    # Clean and check title
    title = entry.get('title')
    if not isinstance(title, str) or not title.strip():
        reasons.append("title is empty or not a string")
        cleaned_title = ""
    else:
        cleaned_title = re.sub(r'[^a-z0-9\s]', '', title.lower()).strip()
        if not cleaned_title:
            reasons.append("cleaned title is empty")
    # Check release_year
    ry = entry.get('release_year')
    if pd.isna(ry):
        pass  # pd.NA allowed
    else:
        if not isinstance(ry, int):
            reasons.append("release_year not int or NA")
        elif not (1870 <= ry <= 2025):
            reasons.append("release_year out of realistic range")
    # Check rating
    rating = entry.get('rating')
    if rating is None or (isinstance(rating, float) and math.isnan(rating)):
        reasons.append("rating is NaN or None")
    # Collect valid or invalid
    if reasons:
        invalid_entries.append({'entry': entry, 'reasons': reasons})
    else:
        key = (cleaned_title, ry)
        if key in seen:
            duplicate_count += 1
        else:
            seen.add(key)
            new_entry = entry.copy()
            new_entry['title'] = cleaned_title
            valid_output.append(new_entry)

if invalid_entries:
    raise AssertionError(f"Invalid entries found: {invalid_entries}")