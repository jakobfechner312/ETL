import pandas as pd
from adapters.adapters.base_adapter import BaseAdapter
from transform.normalize import normalize_film_title # Importieren
import logging

class RottenTomatoesAdapter(BaseAdapter):
    def extract(self) -> pd.DataFrame:
            df = pd.read_csv(self.config['file_path'], engine="python", on_bad_lines="skip")
            logging.info(f"RottenTomatoesAdapter: Rohdaten nach CSV-Einlesen (erste 5 Zeilen):\n{df.head()}")
            logging.info(f"RottenTomatoesAdapter: Info zu Rohdaten:\n{df.info()}")
            return df

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # Bestehende Logik zur Auswahl der Titelspalte
        base_title_series = df["movie_title"].astype(str) # Sicherstellen, dass es String ist
        
        # NEU: Anwendung der zentralen Normalisierungsfunktion
        df["title"] = base_title_series.apply(normalize_film_title)

        # Bestehende Logik für andere Spalten
        df.rename(columns={"original_release_date": "release_date_rt_temp", "genres": "genres_rt_temp"}, inplace=True, errors='ignore')

        df["release_date_rt"] = pd.to_datetime(df.get("release_date_rt_temp"), errors="coerce")
        df["year"] = df["release_date_rt"].dt.year.astype("Int64")
        
        df["genres_rt"] = (
            df.get("genres_rt_temp", pd.Series(dtype=str)) # Fallback auf leere Series falls Spalte fehlt
            .fillna("")
            .apply(lambda g: [genre.strip() for genre in g.split(",") if genre.strip()] if isinstance(g, str) and g else [])
        )
        
        cols_to_drop_temp = [col for col in ["release_date_rt_temp", "genres_rt_temp"] if col in df.columns]
        if cols_to_drop_temp:
            df = df.drop(columns=cols_to_drop_temp, errors='ignore')
        
        # Tomatometer Rating - sicherstellen, dass es existiert
        source_column_raw_name = 'audience_rating' # << HIER DIE ANPASSUNG
        target_column_df_name = 'rating_rt_audience'  # Neuer Name der Spalte im DataFrame

        if source_column_raw_name in df.columns:
            logging.info(f"RottenTomatoesAdapter: Spalte '{source_column_raw_name}' gefunden. Erste Werte:\n{df[source_column_raw_name].head()}")
            df[target_column_df_name] = pd.to_numeric(df[source_column_raw_name], errors='coerce')
            logging.info(f"RottenTomatoesAdapter: Spalte '{target_column_df_name}' nach to_numeric. Erste Werte:\n{df[target_column_df_name].head()}")
                
            if "tomatometer_rating" in df.columns and "tomatometer_rating" != source_column_raw_name:
                df = df.drop(columns=["tomatometer_rating"], errors='ignore')
        else:
            logging.warning(f"RottenTomatoesAdapter: Spalte '{source_column_raw_name}' NICHT in CSV gefunden! '{target_column_df_name}' wird mit NA gefüllt.")
            df[target_column_df_name] = pd.Series([pd.NA] * len(df), dtype=float)


        df = df.drop_duplicates(subset=["title", "year"])

        final_cols = ["title", "release_date_rt", "year", "genres_rt", "rating_rt_audience"]
        return df[[col for col in final_cols if col in df.columns]]