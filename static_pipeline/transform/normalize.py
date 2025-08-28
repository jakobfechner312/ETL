import re
import unicodedata

def normalize_film_title(title: str) -> str:
	if not isinstance(title, str):
		return ""
	# 1) ASCII + lower
	t = unicodedata.normalize("NFKD", title).encode("ascii", "ignore").decode("utf-8").lower()
	# 2) trailing "(YYYY)" entfernen
	t = re.sub(r"\s*\(\d{4}\)\s*$", "", t)
	# 3) ein weiteres trailing "(...)" entfernen
	t = re.sub(r"\s*\([^)]*\)\s*$", "", t)
	# 4) Interpunktion → Leerzeichen
	t = re.sub(r"[^\w\s]", " ", t)
	# 5) Spaces kollabieren + trimmen
	t = re.sub(r"\s+", " ", t).strip()
	# 6) trailing "the" löschen
	t = re.sub(r"\bthe\s*$", "", t).strip()
	# 7) Duplicate‑Tokens entfernen (Reihenfolge beibehalten)
	seen, toks = set(), []
	for tok in t.split():
		if tok not in seen:
			seen.add(tok); toks.append(tok)
	return " ".join(toks)