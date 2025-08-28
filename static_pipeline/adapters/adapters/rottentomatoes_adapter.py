import pandas as pd
from adapters.adapters.base_adapter import BaseAdapter
from transform.normalize import normalize_film_title  # Importieren
import logging
from pathlib import Path 
from io import StringIO  # NEU


class RottenTomatoesAdapter(BaseAdapter):

    def extract(self) -> pd.DataFrame:
        df = pd.read_csv(self.config['file_path'],
                         engine="python",
                         on_bad_lines="skip")
        logging.info(
            f"RottenTomatoesAdapter: Rohdaten nach CSV-Einlesen (erste 5 Zeilen):\n{df.head()}"
        )
        buf = StringIO()
        df.info(buf=buf)
        logging.info(
            f"RottenTomatoesAdapter: Info zu Rohdaten:\n{buf.getvalue()}")
        return df

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        • Bereinigt Rotten-Tomatoes-Rohdaten
        • akzeptiert fallback auf `streaming_release_date`, falls
        `original_release_date` fehlt oder unparsebar ist
        • speichert verworfene Zeilen in <quelle>_invalid.csv
        """
        from pathlib import Path

        df = df.copy()

        # Stabile zeilenbasierte ID
        df["ID_RT"] = range(1, len(df) + 1)

        # 1) Titel normalisieren ------------------------------------------------
        base_title_series = df["movie_title"].astype(str)
        df["title"] = base_title_series.apply(normalize_film_title)

        # 2) Release-Date (Original + Fallback) ---------------------------------
        df.rename(columns={"original_release_date": "release_date_rt_temp"},
                inplace=True, errors="ignore")

        # 2a  Versuch: original_release_date
        df["release_date_rt"] = pd.to_datetime(df.get("release_date_rt_temp"),
                                            errors="coerce")

        # 2b  Fallback auf streaming_release_date, falls erster Versuch NaT
        needs_fallback = (
            df["release_date_rt"].isna()
            & df.get("streaming_release_date").notna()
        )
        if needs_fallback.any():
            df.loc[needs_fallback, "release_date_rt"] = pd.to_datetime(
                df.loc[needs_fallback, "streaming_release_date"],
                errors="coerce"
            )

        df["year"] = df["release_date_rt"].dt.year.astype("Int64")

        # 3) Genres in Liste umwandeln -----------------------------------------
        df.rename(columns={"genres": "genres_rt_temp"}, inplace=True, errors="ignore")
        df["genres_rt"] = (
            df.get("genres_rt_temp", pd.Series(dtype=str))
            .fillna("")
            .apply(lambda g: [genre.strip() for genre in g.split(",") if genre.strip()]
                    if isinstance(g, str) and g else [])
        )

        # 4) Rating-Spalte wählen (Tomatometer bevorzugt, Fallback Audience) ----
        rating_candidates = ["tomatometer_rating", "audience_rating"]

        def _best_rating(row):
            for col in rating_candidates:
                val = pd.to_numeric(row.get(col), errors="coerce")
                if pd.notna(val):
                    # Beide Ratings sind 0–100 skaliert
                    return float(val)
            return pd.NA

        df["rating_rt_audience"] = (
            df.apply(_best_rating, axis=1)
        ).astype("Float64")

        # 5) Temporäre Spalten loswerden ---------------------------------------
        df.drop(columns=[c for c in ("release_date_rt_temp", "genres_rt_temp")
                        if c in df.columns],
                inplace=True, errors="ignore")

        # 6) Vollständige Validierung & Logging --------------------------------
        invalid_rows, duplicate_rows, seen = [], [], set()
        cleaned_rows = []

        for _, row in df.iterrows():
            orig = row.to_dict()
            reason = None

            # Titel prüfen
            title = row["title"]
            if not isinstance(title, str) or not title.strip():
                reason = reason or "empty title"

            # Jahr prüfen
            year = row["year"]
            if pd.isna(year) or not (1870 <= year <= 2025):
                reason = reason or "invalid year"

            # Rating prüfen
            if pd.isna(row["rating_rt_audience"]):
                reason = reason or "missing rating"

            # Duplikatsprüfung
            key = (title.lower(), int(year) if pd.notna(year) else None)
            if key in seen:
                duplicate_rows.append({**orig, "reason": "duplicate title+year"})
                continue
            seen.add(key)

            if reason:
                invalid_rows.append({**orig, "reason": reason})
            else:
                cleaned_rows.append(row)

        # 7) CSV-Logging in zentralen Ordner (einheitliches Schema) -------------
        def _convert(rows: list[dict]):
            converted = []
            for d in rows:
                converted.append({
                    "title": d.get("title"),
                    "release_date_rt": d.get("release_date_rt"),
                    "year": d.get("year"),
                    "genres_rt": d.get("genres_rt"),
                    "rating_rt_audience": d.get("rating_rt_audience"),
                    "reason": d.get("reason", "")
                })
            return converted

        self._log_aux_files(
            "RottenTomatoesAdapter",
            _convert(invalid_rows),
            _convert(duplicate_rows),
        )

        # 8) Ergebnis-DataFrame -------------------------------------------------
        result = pd.DataFrame(cleaned_rows)
        final_cols = ["ID_RT", "title", "release_date_rt", "year", "genres_rt", "rating_rt_audience"]
        return result[[col for col in final_cols if col in result.columns]]