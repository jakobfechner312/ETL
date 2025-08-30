import re
import unicodedata
import pandas as pd

output = []
invalid_entries = []
seen_titles = set()

def clean_title(title: str) -> str:
    # 1) normalize to ascii, lowercase
    normalized = unicodedata.normalize('NFKD', title)
    ascii_title = normalized.encode('ascii', 'ignore').decode('ascii').lower()
    # 2) remove one trailing "(YYYY)"
    ascii_title = re.sub(r'\s*\(\d{4}\)\s*$', '', ascii_title)
    # 3) remove one trailing "(...)" leftover
    ascii_title = re.sub(r'\s*\([^)]*\)\s*$', '', ascii_title)
    # 4) punctuation→space
    ascii_title = re.sub(r'[^\w\s]', ' ', ascii_title)
    # 5) collapse spaces & trim
    ascii_title = re.sub(r'\s+', ' ', ascii_title).strip()
    # 6) drop trailing "the"
    if ascii_title.endswith(' the'):
        ascii_title = ascii_title[:-4].strip()
    # 7) deduplicate tokens, keep order
    tokens = ascii_title.split()
    seen = set()
    deduped = []
    for tok in tokens:
        if tok not in seen:
            seen.add(tok)
            deduped.append(tok)
    return ' '.join(deduped)

def interpret_year(ystr: str) -> int:
    y = int(ystr)
    if len(ystr) == 2:
        # two-digit heuristic: 00-69 → 2000+, 70-99 → 1900+
        y = y + (2000 if y < 70 else 1900)
    return y

for _, row in df.iterrows():
    orig_row = row.to_dict()
    # 1) rating must be present
    rating = row.get('average_rating')
    if pd.isna(rating):
        invalid_entries.append({"row": orig_row, "reason": "missing rating"})
        continue

    # 2) extract year from title if no dedicated column
    title_raw = row.get('title', '')
    m = re.search(r'\((\d{2,4})\)\s*$', title_raw)
    if not m:
        invalid_entries.append({"row": orig_row, "reason": "year not found in title"})
        continue
    try:
        year = interpret_year(m.group(1))
    except:
        invalid_entries.append({"row": orig_row, "reason": "invalid year format"})
        continue
    if not (1870 <= year <= 2025):
        invalid_entries.append({"row": orig_row, "reason": f"year {year} out of range"})
        continue

    # 3) clean the title
    cleaned_title = clean_title(title_raw)
    if not cleaned_title:
        invalid_entries.append({"row": orig_row, "reason": "empty title after cleaning"})
        continue

    # 4) enforce uniqueness on (title, release_year)
    key = (cleaned_title, year)
    if key in seen_titles:
        invalid_entries.append({"row": orig_row, "reason": "duplicate title-year"})
        continue
    seen_titles.add(key)

    # 5) parse genres into list[str]
    genres_raw = row.get('genres', '')
    if pd.isna(genres_raw) or genres_raw.strip() == '(no genres listed)':
        genres_list = []
    else:
        genres_list = [g.strip() for g in genres_raw.split('|') if g.strip()]

    # 6) append cleaned record
    output.append({
        "ID": row["ID_MOVIELENS"],
        "title": cleaned_title,
        "release_year": year,
        "genres": genres_list,
        "rating": float(rating)
    })