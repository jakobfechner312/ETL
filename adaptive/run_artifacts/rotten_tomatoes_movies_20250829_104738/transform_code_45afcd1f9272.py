import re
import unicodedata

output = []
invalid_entries = []
seen_titles = set()

for _, row in df.iterrows():
    orig_row = row.to_dict()

    # 1. ID
    movie_id = row["ID_RT"]

    # 2. Extract release_year from original_release_date or streaming_release_date
    year = None
    for date_field in ["original_release_date", "streaming_release_date"]:
        date_val = row.get(date_field)
        if pd.notna(date_val):
            m = re.match(r"^(\d{2,4})", str(date_val))
            if m:
                y = int(m.group(1))
                if y < 100:
                    # interpret two-digit year
                    y = y + 2000 if y <= 25 else y + 1900
                year = y
                break
    if year is None or year < 1870 or year > 2025:
        invalid_entries.append({"row": orig_row, "reason": "invalid_release_year"})
        continue

    # 3. Normalize title
    raw_title = row.get("movie_title", "")
    # to ASCII, lowercase
    t = unicodedata.normalize("NFKD", raw_title).encode("ascii", "ignore").decode()
    t = t.lower()
    # remove trailing "(YYYY)"
    t = re.sub(r"\(\d{4}\)\s*$", "", t)
    # remove trailing "(...)" if any
    t = re.sub(r"\([^)]*\)\s*$", "", t)
    # punctuation → space, collapse whitespace
    t = re.sub(r"[^a-z0-9\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    # drop trailing 'the'
    t = re.sub(r"\bthe$", "", t).strip()
    # dedupe tokens
    tokens = t.split()
    seen_tok = set()
    clean_tokens = []
    for tok in tokens:
        if tok not in seen_tok:
            seen_tok.add(tok)
            clean_tokens.append(tok)
    title_clean = " ".join(clean_tokens)

    if not title_clean:
        invalid_entries.append({"row": orig_row, "reason": "empty_title"})
        continue

    # 4. Check uniqueness of (title, year)
    key = (title_clean, year)
    if key in seen_titles:
        invalid_entries.append({"row": orig_row, "reason": "duplicate_title_year"})
        continue
    seen_titles.add(key)

    # 5. Select rating: prefer tomatometer_rating, fallback to audience_rating
    rating = row.get("tomatometer_rating")
    if pd.isna(rating):
        rating = row.get("audience_rating")
    if pd.isna(rating):
        invalid_entries.append({"row": orig_row, "reason": "missing_rating"})
        continue
    rating = float(rating)

    # 6. Parse genres into list[str]
    raw_genres = row.get("genres")
    if pd.isna(raw_genres):
        genres_list = []
    else:
        # split on commas or ampersand
        parts = re.split(r"[,&]", raw_genres)
        genres_list = [g.strip() for g in parts if g.strip()]

    # 7. Build output record
    output.append({
        "ID": movie_id,
        "title": title_clean,
        "release_year": year,
        "genres": genres_list,
        "rating": rating
    })