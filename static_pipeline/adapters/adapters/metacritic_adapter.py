import pandas as pd
from adapters.adapters.base_adapter import BaseAdapter
from transform.normalize import normalize_film_title # Importieren

class MetacriticAdapter(BaseAdapter):
    def extract(self) -> pd.DataFrame:
        df = pd.read_csv(self.config['file_path'])
        return df

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        
        # Bestehende Logik zur Auswahl der Titelspalte
        base_title_series = df["movie_title"].astype(str) # Sicherstellen, dass es String ist

        # NEU: Anwendung der zentralen Normalisierungsfunktion
        df["title"] = base_title_series.apply(normalize_film_title)
        
        # Bestehende Logik für andere Spalten
        df.rename(columns={"release_date": "release_date_original_temp"}, inplace=True, errors='ignore')
        df["release_date_meta"] = pd.to_datetime(df.get("release_date_original_temp"), format="%d-%b-%y", errors="coerce")
        df["year"] = df["release_date_meta"].dt.year.astype("Int64")
        
        # Jahreskorrektur
        if "year" in df.columns and pd.api.types.is_numeric_dtype(df["year"]):
             df.loc[df["year"] > 2025, "year"] -= 100
        if "release_date_meta" in df.columns and pd.api.types.is_datetime64_any_dtype(df["release_date_meta"]):
             df.loc[df["release_date_meta"].dt.year > 2025, "release_date_meta"] -= pd.DateOffset(years=100)

        df["rating_metacritic"] = pd.to_numeric(df.get("metascore"), errors="coerce")

        if "release_date_original_temp" in df.columns:
            df = df.drop(columns=["release_date_original_temp"], errors='ignore')

        df = df.drop_duplicates(subset=["title", "year"])

        final_cols = ["title", "release_date_meta", "year", "rating_metacritic"]
        return df[[col for col in final_cols if col in df.columns]]