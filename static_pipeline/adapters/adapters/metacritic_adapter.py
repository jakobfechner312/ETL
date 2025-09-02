# static_pipeline/adapters/adapters/metacritic_adapter.py
import re
from pathlib import Path
from typing import List

import pandas as pd
from adapters.adapters.base_adapter import BaseAdapter
from transform.normalize import normalize_film_title


class MetacriticAdapter(BaseAdapter):
    """Reiner Pandas-Adapter für Metacritic – mit Validierung & Logging."""

    def extract(self) -> pd.DataFrame: 
        return pd.read_csv(self.config["file_path"], on_bad_lines="skip")

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:  
        df = df.copy()
        src_path = Path(self.config["file_path"])
        # Stabile zeilenbasierte ID
        df["ID_METACRITIC"] = range(1, len(df) + 1)
        invalid_rows: List[dict] = []
        duplicate_rows: List[dict] = []
        seen_keys = set()

        # ---------- Titel normalisieren --------------------------------
        df["title"] = df["movie_title"].astype(str).apply(normalize_film_title)

        # ---------- Datum parsen  --------------------------------------
        def _parse_date(val: str):
            val = str(val).strip()
            # 1) Reguläres dd-MMM-yy (Metacritic-Standard)
            d = pd.to_datetime(val, format="%d-%b-%y", errors="coerce")
            if pd.notna(d):
                return d
            # 2) Month YYYY  (e.g. "January 1994")
            m = pd.to_datetime(val, format="%B %Y", errors="coerce")
            if pd.notna(m):
                return m
            # 3) Reines Jahr "1994"
            if re.fullmatch(r"\d{4}", val):
                return pd.to_datetime(f"{val}-01-01", errors="coerce")
            return pd.NaT

        df["release_date_meta"] = df["release_date"].apply(_parse_date)
        df["year"] = df["release_date_meta"].dt.year.astype("Int64")

        # Korrektur zweistelliger Jahresrollen (>2025 → -100 Jahre)
        df.loc[df["year"] > 2025, "year"] -= 100
        df.loc[df["release_date_meta"].dt.year > 2025, "release_date_meta"] -= pd.DateOffset(
            years=100
        )

        # ---------- Rating-Spalte wählen --------------------------------
        rating_candidates = [
            c for c in df.columns if any(k in c.lower() for k in ("score", "rating"))
        ]

        def _best_rating(row):
            best_val = pd.NA
            best_count = -1
            for col in rating_candidates:
                raw = row[col]
                # Ignoriere Platzhalter wie "tbd"
                if isinstance(raw, str) and raw.lower() == "tbd":
                    continue
                num = pd.to_numeric(raw, errors="coerce")
                if pd.notna(num):
                    # Kritiker-Score liegt 0–100, User-Score meist 0–10
                    if "user" in col.lower() and num <= 10:
                        num *= 10
                    return float(num)
            return best_val

        df["rating_metacritic"] = df.apply(_best_rating, axis=1)

        # ---------- Zeilen iterieren & prüfen ---------------------------
        cleaned_rows = []
        for _, row in df.iterrows():
            reason = None

            title = row["title"]
            if not isinstance(title, str) or not title.strip():
                reason = "empty title"

            year = row["year"]
            if pd.isna(year) or not (1870 <= year <= 2025):
                reason = reason or "invalid year"

            rating = row["rating_metacritic"]
            if pd.isna(rating):
                reason = reason or "missing rating"

            key = (title.lower(), int(year) if pd.notna(year) else None)
            if key in seen_keys:
                duplicate_rows.append({**row.to_dict(), "reason": "duplicate title+year"})
                continue  # Duplikate werden nicht doppelt geprüft
            seen_keys.add(key)

            if reason:
                invalid_rows.append({**row.to_dict(), "reason": reason})
            else:
                cleaned_rows.append(
                    {
                        "ID_METACRITIC": row["ID_METACRITIC"],
                        "title": title,
                        "release_date_meta": row["release_date_meta"],
                        "year": year,
                        "genres": [
                            g.strip()
                            for g in str(row.get("genre", "")).split(",")
                            if g.strip()
                        ],
                        "rating_metacritic": rating,
                    }
                )

        # ---------- Invalid/Duplicate zentral speichern ------------------
        def _convert(rows: list[dict]):
            converted = []
            for d in rows:
                converted.append({
                    "ID_METACRITIC": d.get("ID_METACRITIC"),
                    "title": d.get("title"),
                    "release_date_meta": d.get("release_date_meta"),
                    "year": d.get("year"),
                    "genres": d.get("genres", d.get("genre")),
                    "rating": d.get("rating_metacritic", d.get("rating")),
                    "reason": d.get("reason", "")
                })
            return converted

        self._log_aux_files(
            "MetacriticAdapter",
            _convert(invalid_rows),
            _convert(duplicate_rows),
        )

        # ---------- Ergebnis-DataFrame ----------------------------------
        result_df = pd.DataFrame(cleaned_rows)
        return result_df[["ID_METACRITIC", "title", "release_date_meta", "year", "genres", "rating_metacritic"]]