# static_pipeline/adapters/adapters/imdb_adapter.py
import re
from pathlib import Path
from typing import List
import yaml
import pandas as pd
from adapters.adapters.base_adapter import BaseAdapter
from transform.normalize import normalize_film_title

class ImdbAdapter(BaseAdapter):
    """IMDb-Adapter mit Validierung & Logging (Schema-konform).

    • title            str, bereinigt, (title, year) eindeutig
    • release_date_imdb   datetime64[ns]
    • year             Int64  (1900-2025)
    • genres           list[str]  (optional, leere Liste erlaubt)
    • rating           float 0-10  (IMDb-Skala)
    """

    # ------------------------------------------------------------ #
    # 1) Extract                                                   #
    # ------------------------------------------------------------ #
    def extract(self) -> pd.DataFrame:  # type: ignore[override]
        return pd.read_csv(self.config["file_path"], on_bad_lines="skip")

    # ------------------------------------------------------------ #
    # 2) Transform + Validate                                      #
    # ------------------------------------------------------------ #
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:  # type: ignore[override]
        df = df.copy()
        src_path = Path(self.config["file_path"])

        # Stabile zeilenbasierte ID, die von Anfang an gilt und nicht neu nummeriert wird
        df["ID_IMDB"] = range(1, len(df) + 1)

        invalid_rows: List[dict] = []
        duplicate_rows: List[dict] = []
        seen_keys = set()
        cleaned_rows = []

        # ---------- Titel normalisieren ---------------------------
        df["title"] = df["originalTitle"].astype(str).apply(normalize_film_title)

        # ---------- Datum → release_date_imdb + year --------------
        df["release_date_imdb"] = pd.to_datetime(df.get("release_date"), errors="coerce")
        df["year"] = df["release_date_imdb"].dt.year.astype("Int64")

        # ---------- Fallback-Parsing für "Month YYYY" oder "YYYY" ----------
        def _parse_fallback(val: str):
            val = str(val).strip()
            # "September 2012" etc.
            try:
                dt = pd.to_datetime(val, format="%B %Y", errors="raise")
                return dt
            except Exception:
                pass
            # nur Jahr "2013"
            if val.isdigit() and len(val) == 4:
                return pd.to_datetime(f"{val}-01-01", errors="coerce")
            return pd.NaT

        mask_na_date = df["release_date_imdb"].isna() & df["release_date"].notna()
        if mask_na_date.any():
            df.loc[mask_na_date, "release_date_imdb"] = (
                df.loc[mask_na_date, "release_date"].apply(_parse_fallback)
            )

        # Jahr erneut nachziehen, falls jetzt Datum vorhanden ist
        missing_year = df["year"].isna()
        df.loc[missing_year, "year"] = df.loc[missing_year, "release_date_imdb"].dt.year.astype("Int64")

        # Fallback: separate year-Spalte, falls vorhanden und immer noch NA
        na_mask = df["year"].isna() & df.columns.str.contains("year").any()
        if na_mask.any():
            df.loc[na_mask, "year"] = pd.to_numeric(df.loc[na_mask, "year"], errors="coerce")

        # ---------- Rating (0-10) ----------------------------------
        rating_col = "averageRating" if "averageRating" in df.columns else None

        for _, row in df.iterrows():
            orig = row.to_dict()
            reason = None

            # 1) Title-Check
            title = row["title"]
            if not isinstance(title, str) or not title.strip():
                reason = "empty title"

            # 2) Year-Check
            year = row["year"]
            if pd.isna(year) or not (1870 <= year <= 2025):
                reason = reason or "invalid year"

            # 3) Rating-Check (fehlendes Rating ist erlaubt, wird als NA durchgereicht)
            rating = (
                pd.to_numeric(row.get(rating_col), errors="coerce") if rating_col else pd.NA
            )
            if pd.notna(rating):
                rating = float(rating)          # garantiert Python-Float  → später Float64
            # kein else: fehlendes Rating ist KEIN Invalid-Reason mehr

            # 4) Duplicate-Check
            key = (title.lower(), int(year) if pd.notna(year) else None)
            if key in seen_keys:
                duplicate_rows.append({
                    "ID_IMDB": row["ID_IMDB"],
                    "title": title,
                    "release_date_imdb": row["release_date_imdb"],
                    "year": year,
                    "genres": (
                        [g.strip() for g in re.split(r"[|,]", str(row.get("genres", ""))) if g.strip()]
                        if pd.notna(row.get("genres")) else []
                    ),
                    "rating": (float(rating) if pd.notna(rating) else pd.NA),
                    "reason": "duplicate title+year",
                })
                continue
            seen_keys.add(key)

            # 5) Invalid vs Valid
            genres_raw = row.get("genres", "")
            genres_list = (
                [g.strip() for g in re.split(r"[|,]", str(genres_raw)) if g.strip()]
                if pd.notna(genres_raw)
                else []
            )

            if reason:
                invalid_rows.append({
                    "ID_IMDB": row["ID_IMDB"],
                    "title": title,
                    "release_date_imdb": row["release_date_imdb"],
                    "year": year,
                    "genres": genres_list,
                    "rating": (float(rating) if pd.notna(rating) else pd.NA),
                    "reason": reason,
                })
            else:
                cleaned_rows.append(
                    {
                        "ID_IMDB": row["ID_IMDB"],
                        "title": title,
                        "release_date_imdb": row["release_date_imdb"],
                        "year": year,
                        "genres": genres_list,
                        "rating_imdb": (float(rating) if pd.notna(rating) else pd.NA),
                    }
                )

        # ---------- CSV-Logging (zentraler Pfad) --------------------
        self._log_aux_files("ImdbAdapter", invalid_rows, duplicate_rows)
            
        # ---------- Ergebnis-DataFrame -----------------------------
        result = pd.DataFrame(cleaned_rows)
        return result[["ID_IMDB", "title", "release_date_imdb", "year", "genres", "rating_imdb"]]