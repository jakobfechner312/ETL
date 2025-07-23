import pandas as pd
from adapters.adapters.base_adapter import BaseAdapter
from transform.normalize import normalize_film_title

class ImdbAdapter(BaseAdapter):
    def extract(self) -> pd.DataFrame:
        df = pd.read_csv(self.config['file_path'])
        return df

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        
        base_title_series = df["originalTitle"].astype(str)
        df["title"] = base_title_series.apply(normalize_film_title)

        df.rename(columns={"release_date": "release_date_original_temp"}, inplace=True, errors='ignore')
        df["release_date_imdb"] = pd.to_datetime(df.get("release_date_original_temp"), errors="coerce")
        df["year"] = df["release_date_imdb"].dt.year.astype("Int64")
        df["rating_imdb"] = pd.to_numeric(df.get("averageRating"), errors="coerce")
        
        if "release_date_original_temp" in df.columns:
            df = df.drop(columns=["release_date_original_temp"], errors='ignore')

        df = df.drop_duplicates(subset=["title", "year"])
        
        final_cols = ["title", "year", "release_date_imdb", "rating_imdb"]
        return df[[col for col in final_cols if col in df.columns]]
