import pandas as pd
import re, unicodedata

output = []
invalid_entries = []
seen = set()

def parse_year(date_str):
    if pd.isna(date_str):
        return None
    # expect formats like "YYYY-MM-DD" or "YY-MM-DD"
    m = re.match(r"^(\d{2,4})", date_str)
    if not m:
        return None
    y = int(m.group(1))
    if y < 100:
        # two-digit → choose century
        if 0 <= y <= 25:
            y += 2000
        else:
            y += 1900
    if 1870 <= y <= 2025:
        return y
    return None

def clean_title(raw):
    # ASCII normalize & lowercase
    s = unicodedata.normalize("NFKD", raw).encode("ascii", "ignore").decode("ascii")
    s = s.lower()
    # remove one trailing "(YYYY)"
    s = re.sub(r"\(\d{4}\)\s*$", "", s)
    # remove one trailing "(...)" any
    s = re.sub(r"\([^)]+\)\s*$", "", s)
    # punctuation to spaces
    s = re.sub(r"[^\w\s]", " ", s)
    # collapse spaces & strip
    s = re.sub(r"\s+", " ", s).strip()
    # drop trailing "the"
    if s.endswith(" the"):
        s = s[: -len(" the")].strip()
    # dedupe tokens
    tokens = s.split()
    seen_tok = set()
    out_tokens = []
    for t in tokens:
        if t not in seen_tok:
            out_tokens.append(t)
            seen_tok.add(t)
    return " ".join(out_tokens)

for row in df.to_dict("records"):
    orig = dict(row)
    ID = row.get("ID_RT")
    # 1) parse release_year
    y = parse_year(row.get("original_release_date"))
    if y is None:
        y = parse_year(row.get("streaming_release_date"))
    if y is None:
        invalid_entries.append({"row": orig, "reason": "invalid or missing release_year"})
        continue
    # 2) clean title
    raw_title = row.get("movie_title", "")
    title = clean_title(raw_title)
    if not title:
        invalid_entries.append({"row": orig, "reason": "empty title after cleaning"})
        continue
    # 3) uniqueness
    key = (title, y)
    if key in seen:
        invalid_entries.append({"row": orig, "reason": "duplicate title+year"})
        continue
    seen.add(key)
    # 4) rating selection
    tom = row.get("tomatometer_rating")
    aud = row.get("audience_rating")
    rating = tom if not pd.isna(tom) else aud
    if pd.isna(rating):
        invalid_entries.append({"row": orig, "reason": "no available rating"})
        continue
    rating = float(rating)
    # 5) genres list
    gens = row.get("genres")
    if pd.isna(gens) or not isinstance(gens, str):
        genres_list = []
    else:
        genres_list = [g.strip() for g in gens.split(",") if g.strip()]
    # assemble
    output.append({
        "ID": ID,
        "title": title,
        "release_year": y,
        "genres": genres_list,
        "rating": rating
    })
# at the end, output and invalid_entries are ready