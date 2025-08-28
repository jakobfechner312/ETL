import pandas as pd
from adapters.adapters.base_adapter import BaseAdapter
from transform.normalize import normalize_film_title
from pathlib import Path  

class MovielensAdapter(BaseAdapter):
    def extract(self):  # type: ignore[override]
        """Lädt die benötigten Movielens-Daten.

        Es werden zwei Betriebsmodi unterstützt:

        1. Aggregierte Datei (empfohlen):
           Ist in der Konfiguration *file_path* **oder** *aggregated_path* gesetzt,
           wird genau eine CSV-Datei eingelesen, die bereits Filme und die
           durchschnittlichen Bewertungen enthält.

        2. Rohdateien: Sind *movies_path* **und** *ratings_path* gesetzt, werden
           diese beiden Dateien geladen und in :pymeth:`transform` zusammengeführt
           wie bisher.
        """

        # --- Modus 1: Eine bereits aggregierte CSV ---
        aggregated_key = next(
            (k for k in ("file_path", "aggregated_path") if k in self.config),
            None,
        )
        if aggregated_key:
            aggregated_df = pd.read_csv(self.config[aggregated_key])
            return aggregated_df  # Einzelnes DataFrame

        # --- Modus 2: Zwei Roh-Dateien (Standard des ursprünglichen Codes) ---
        movies = pd.read_csv(self.config["movies_path"])
        ratings = pd.read_csv(self.config["ratings_path"])
        return movies, ratings

    def transform(self, data) -> pd.DataFrame:  # type: ignore[override]
        """Bereinigt und vereinheitlicht die Movielens-Daten."""

        # Erkennen, ob *data* bereits das finale DataFrame (aggregiert) ist oder
        # ob wir wie bisher zwei DataFrames (Filme + Ratings) erhalten haben.

        if isinstance(data, tuple):
            # --- Legacy-Pfad: Rohdateien ---
            df_movies, df_ratings = data
            df_movies = df_movies.copy()
        else:
            # --- Neuer Pfad: Aggregierte Datei ---
            df_movies = data.copy()
            df_ratings = None  # Wird nicht benötigt

        # Stabile zeilenbasierte ID, die von Anfang an gilt
        df_movies["ID_MOVIELENS"] = range(1, len(df_movies) + 1)

        # Bestehende Logik zur Jahres-Extraktion und Titel-Vorreinigung
        df_movies["year"] = (
            df_movies["title"]
            .astype(str) # Sicherstellen, dass es String ist für .str Methoden
            .str.extract(r"\((\d{4})\)")
            .iloc[:, 0] # Extrahierte Gruppe als Series nehmen
            .astype(float)
            .astype("Int64")
        )
        # ── ungültige Zeilen (fehlendes Jahr) sammeln ──────────
        invalid_df = df_movies[df_movies["year"].isna()].copy()
        invalid_initial: list[dict] = []
        if not invalid_df.empty:
            for _, row in invalid_df.iterrows():
                invalid_initial.append({
                    "title": row["title"],
                    "year": pd.NA,
                    "genres_ml": row.get("genres", ""),
                    "rating_movielens": row.get("average_rating", pd.NA),
                    "reason": "missing year"
                })
        # Fallback: falls Jahr fehlt, versuche release_year/year-Spalten + 2-digit Mapping
        if df_movies["year"].isna().any():
            # release_year übernehmen, falls vorhanden
            if "release_year" in df_movies.columns:
                df_movies.loc[df_movies["year"].isna(), "year"] = pd.to_numeric(
                    df_movies.loc[df_movies["year"].isna(), "release_year"], errors="coerce"
                ).astype("Int64")
            # ggf. vorhandene year-Spalte nochmal numerisch casten
            df_movies["year"] = pd.to_numeric(df_movies["year"], errors="coerce").astype("Float64")
            # 2-digit → 4-digit Mapping
            def map_two_digit(y):
                if pd.isna(y):
                    return pd.NA
                y = int(y)
                if y < 100:
                    y = y + (2000 if y <= 25 else 1900)
                return y
            df_movies["year"] = df_movies["year"].apply(map_two_digit).astype("Int64")
        # entferne verbleibende Zeilen ohne Jahr
        df_movies = df_movies.dropna(subset=["year"])            
        
        # Titel ausschließlich über die zentrale Normalisierung bereinigen
        df_movies["title"] = df_movies["title"].astype(str).apply(normalize_film_title)
        
        # Bestehende Logik für Genres und Ratings
        df_movies.rename(columns={"genres": "genres_ml"}, inplace=True)
        df_movies["genres_ml"] = (
            df_movies["genres_ml"]
            .fillna("")
            .apply(lambda g: [genre.strip() for genre in g.split("|") if genre.strip()] if isinstance(g, str) and g else [])
        )
        
        # --- Bewertungsspalte erstellen ---
        if df_ratings is not None:
            # Alte Logik: Durchschnitt aus Ratings berechnen.
            df_grouped_ratings = (
                df_ratings
                .groupby("movieId")
                .agg(rating_movielens=("rating", "mean"))
                .reset_index()
            )
            df_grouped_ratings["rating_movielens"] = df_grouped_ratings[
                "rating_movielens"
            ].round(2)

            df = pd.merge(df_movies, df_grouped_ratings, on="movieId", how="left")
        else:
            # Neue Logik: Spalte *average_rating* umbenennen.
            if "average_rating" in df_movies.columns:
                df_movies.rename(columns={"average_rating": "rating_movielens"}, inplace=True)
            df = df_movies
        
        # ---------- Vollständige Validierung -----------------------------
        invalid_rows, duplicate_rows, seen = invalid_initial.copy(), [], set()
        cleaned_rows = []

        for _, row in df_movies.iterrows():
            orig = row.to_dict()
            reason = None

            # Titel prüfen
            title = row["title"]
            if not isinstance(title, str) or not title.strip():
                reason = "empty title"

            # Jahr prüfen
            year = row["year"]
            if pd.isna(year) or not (1870 <= year <= 2025):
                reason = reason or "invalid year"

            # Rating prüfen (0–5 oder 0–10 -> schon numerisch?)
            rating = pd.to_numeric(row.get("rating_movielens"), errors="coerce")
            if pd.isna(rating):
                reason = reason or "missing rating"

            # Duplicate-Check
            key = (title.lower(), int(year) if pd.notna(year) else None)
            if key in seen:
                duplicate_rows.append({**orig, "reason": "duplicate title+year"})
                continue
            seen.add(key)

            if reason:
                invalid_rows.append({**orig, "reason": reason})
            else:
                cleaned_rows.append(row)

        # ---------- Invalid/Duplicate in einheitlichem Schema + zentralem Pfad
        def _convert(rows: list[dict]):
            converted = []
            for d in rows:
                converted.append({
                    "ID_MOVIELENS": d.get("ID_MOVIELENS"),
                    "title": d.get("title"),
                    "year": d.get("year"),
                    "genres_ml": d.get("genres_ml", d.get("genres")),
                    "rating_movielens": d.get("rating_movielens"),
                    "reason": d.get("reason", "")
                })
            return converted

        self._log_aux_files(
            "MovielensAdapter",
            _convert(invalid_rows),
            _convert(duplicate_rows),
        )
        print(len(invalid_rows), "invalid  |", len(duplicate_rows), "duplicates")
        result = pd.DataFrame(cleaned_rows)
        if not result.empty and "rating_movielens" in result.columns:
            result["rating_movielens"] = pd.to_numeric(result["rating_movielens"], errors="coerce").astype("Float64")

        final_cols = ["ID_MOVIELENS", "title", "year", "genres_ml", "rating_movielens"]
        return result[[col for col in final_cols if col in result.columns]]