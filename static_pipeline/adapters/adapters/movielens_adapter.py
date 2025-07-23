import pandas as pd
from adapters.adapters.base_adapter import BaseAdapter
from transform.normalize import normalize_film_title

class MovielensAdapter(BaseAdapter):
    def extract(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        # Lädt Filme und Ratings
        movies = pd.read_csv(self.config['movies_path'])
        ratings = pd.read_csv(self.config['ratings_path'])
        return movies, ratings

    def transform(self, data: tuple[pd.DataFrame, pd.DataFrame]) -> pd.DataFrame:
        df_movies, df_ratings = data
        df_movies = df_movies.copy()

        # Bestehende Logik zur Jahres-Extraktion und Titel-Vorreinigung
        df_movies["year"] = (
            df_movies["title"]
            .astype(str) # Sicherstellen, dass es String ist für .str Methoden
            .str.extract(r"\((\d{4})\)")
            .iloc[:, 0] # Extrahierte Gruppe als Series nehmen
            .astype(float)
            .astype("Int64")
        )
        
        base_title_series = (
            df_movies["title"]
            .astype(str) # Sicherstellen, dass es String ist
            .str.replace(r"\s*\(\d{4}\)\s*$", "", regex=True) # Jahr am Ende entfernen, mit optionalen Leerzeichen
            .str.strip()
        )
        
        # NEU: Anwendung der zentralen Normalisierungsfunktion
        df_movies["title"] = base_title_series.apply(normalize_film_title)
        
        # Bestehende Logik für Genres und Ratings
        df_movies.rename(columns={"genres": "genres_ml"}, inplace=True)
        df_movies["genres_ml"] = (
            df_movies["genres_ml"]
            .fillna("")
            .apply(lambda g: [genre.strip() for genre in g.split("|") if genre.strip()] if isinstance(g, str) and g else [])
        )
        
        df_grouped_ratings = (
            df_ratings
            .groupby("movieId")
            .agg(rating_movielens=("rating", "mean"))
            .reset_index()
        )
        df_grouped_ratings["rating_movielens"] = df_grouped_ratings["rating_movielens"].round(2)
        
        df = pd.merge(df_movies, df_grouped_ratings, on="movieId", how="left")
        
        final_cols = ["title", "year", "genres_ml", "rating_movielens"]
        return df[[col for col in final_cols if col in df.columns]]