"""
Fuzzy-Merge mehrerer Bewertungs-Quellen.

• Aggressive Titel-Normalisierung
• ±1-Jahr-Clustering
• Long → Wide-Pivot
• Genres: erste nicht-leere Liste pro Film (ohne weitere Aufbereitung)
• Behalten nur Filme mit ≥ 2 vorhandenen Ratings
"""

from __future__ import annotations

import ast
import re
from typing import List
from pathlib import Path

import pandas as pd

# Akzente → ASCII; Fallback, falls Paket nicht installiert
try:
    from unidecode import unidecode
except ImportError:  # pragma: no cover
    unidecode = lambda s: s  # type: ignore


# ───────────────────────── Hilfsfunktionen ──────────────────────────
def norm_title(title: str) -> str:
    """Titel in robuste, vergleichbare Form bringen."""
    if not isinstance(title, str):
        return ""
    t = unidecode(title).lower()
    t = re.sub(r"\s*\(\d{4}\)$", "", t)           # (YYYY) am Ende
    t = re.sub(r"\s*\([^)]*\)$", "", t)           # sonstige Klammern
    t = re.sub(r"[^\w\s]", " ", t)                # Satzzeichen → Leerzeichen
    t = re.sub(r"\bthe\s*$", "", t)               # trailing "the"
    t = re.sub(r"\s+", " ", t).strip()

    # doppelte Tokens entfernen
    seen, tokens = set(), []
    for tok in t.split():
        if tok not in seen:
            tokens.append(tok)
            seen.add(tok)
    return " ".join(tokens)


def _cluster_years(unique_years: list[int]) -> dict[int, int]:
    """Weist Jahren Cluster-IDs zu, wenn sie maximal ±1 Jahr auseinanderliegen."""
    clusters: list[list[int]] = []
    mapping: dict[int, int] = {}
    for y in sorted(unique_years):
        placed = False
        for cid, members in enumerate(clusters):
            if any(abs(y - m) <= 1 for m in members):
                members.append(y)
                mapping[y] = cid
                placed = True
                break
        if not placed:
            clusters.append([y])
            mapping[y] = len(clusters) - 1
    return mapping


def year_cluster(series: pd.Series) -> pd.Series:
    """Series → Cluster-ID je Jahr."""
    years = [int(y) for y in series.dropna()]
    mapping = _cluster_years(years)
    next_id = (max(mapping.values()) + 1) if mapping else 0
    return series.map(mapping).fillna(next_id).astype(int)


def _first(series: pd.Series):
    """Gibt das erste Element zurück, das keine leere Liste / NA ist."""
    for val in series:
        if isinstance(val, list) and val:   # echte nicht-leere Liste
            return val
        if isinstance(val, str) and val.strip():
            return val
    return []


# ───────────────────────── Hauptfunktion ────────────────────────────
def merge_sources(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    """
    Führt DataFrames aus verschiedenen Quellen in ein gemeinsames Wide-Format
    zusammen, toleriert Titel- und Jahresabweichungen.
    """
    if not dfs:
        return pd.DataFrame()

    # 1) Long-Format aufbauen -------------------------------------------------
    frames: list[pd.DataFrame] = []
    for df in dfs:
        # passende Rating-Spalte identifizieren
        rating_col, source = None, None
        for col in df.columns:
            if col.startswith("rating_"):
                rating_col = col
                source = col.replace("rating_", "")
                break
            if col == "tomatometer_rating":           # Fallback (ältere RT-Spalte)
                rating_col, source = col, "rt_audience"
                break

        if rating_col is None:                       # Quelle ohne Rating
            continue

        tmp = df.copy()
        tmp["rating"] = pd.to_numeric(tmp[rating_col], errors="coerce")
        tmp["source"] = source

        # Genres unverändert übernehmen (erste Spalte, die mit "genres" beginnt)
        genre_col = next((c for c in tmp.columns if c.startswith("genres")), None)
        tmp["genres"] = tmp[genre_col] if genre_col else [[]] * len(tmp)

        # Release-Date sicherstellen
        if "release_date" not in tmp.columns:
            tmp["release_date"] = pd.NaT

        frames.append(
            tmp[["title", "year", "release_date", "genres", "rating", "source"]]
        )

    if not frames:
        return pd.DataFrame()

    long_df = pd.concat(frames, ignore_index=True)

    # 2) Titel normalisieren + Jahr-Cluster ----------------------------------
    long_df["norm_title"] = long_df["title"].apply(norm_title)
    long_df["release_year"] = pd.to_numeric(long_df["year"], errors="coerce").astype(
        "Int64"
    )
    long_df["year_cluster"] = (
        long_df.groupby("norm_title", group_keys=False)["release_year"]
               .apply(year_cluster)
    )

    # 3) Pivot (Long → Wide) --------------------------------------------------
    ratings_wide = (
        long_df.pivot_table(
            index=["norm_title", "year_cluster"],
            columns="source",
            values="rating",
            aggfunc="first",
        )
        .reset_index()
    )

    # 4) Meta-Infos (Titel, Jahr, Release-Date) ------------------------------
    meta = (
        long_df.sort_values(["source", "title"])
               .groupby(["norm_title", "year_cluster"])
               .agg(
                   title=("title", "first"),
                   year=("release_year", "min"),
                   release_date=("release_date", "min"),
               )
               .reset_index()
    )
    wide = ratings_wide.merge(meta, on=["norm_title", "year_cluster"], how="left")

    # 5) Genres: erste nicht-leere Liste (Notebook-Logik) ---------------------
    genres_map = (
        long_df.sort_values("source")
               .groupby(["norm_title", "year_cluster"])["genres"]
               .apply(_first)
               .reset_index()
    )
    wide = wide.merge(genres_map, on=["norm_title", "year_cluster"], how="left")

    # 6) Rating-Spalten umbenennen wie von der Pipeline erwartet -------------
    rename_map = {
        "imdb":        "rating_imdb",
        "movielens":   "rating_movielens",
        "metacritic":  "rating_metacritic",
        "rt_audience": "rating_rt_audience",
    }
    wide = wide.rename(columns=rename_map)

    rating_cols = [
        "rating_imdb",
        "rating_movielens",
        "rating_metacritic",
        "rating_rt_audience",
    ]
    for col in rating_cols:                           # fehlende Spalten anlegen
        if col not in wide.columns:
            wide[col] = pd.NA
    # --- Zwischenspeichern: Wide-Frame vor ≥2-Ratings-Filter -----------
    UNFILTERED_OUT = Path("static_pipeline/data/processed/all_movies_wide_unfiltered.csv")
    UNFILTERED_OUT.parent.mkdir(parents=True, exist_ok=True)
    wide.to_csv(UNFILTERED_OUT, index=False)
    print(f"💾 Ungefiltertes Wide-Ergebnis gespeichert: {UNFILTERED_OUT}")
    
    # 7) Anzahl verfügbarer Ratings + Filter ---------------------------------
    wide["count_ratings"] = wide[rating_cols].notna().sum(axis=1)
    df_final = wide[wide["count_ratings"] >= 2].copy()

    # 8) Sortierung -----------------------------------------------------------
    df_final = (
        df_final.sort_values(["count_ratings", "year"], ascending=[False, False])
                .reset_index(drop=True)
    )

    # 9) Harmonisierung der Zeitspalten --------------------------------------
    #   – 'year'   → 'release_year'
    #   – 'release_date' entfällt komplett
    df_final = (
        df_final
            .rename(columns={"year": "release_year"})
            .drop(columns=["release_date"], errors="ignore")
    )

    # 10) Spaltenauswahl ------------------------------------------------------
    final_columns = [
        "title",
        "release_year",
        "genres",
        "rating_imdb",
        "rating_movielens",
        "rating_metacritic",
        "rating_rt_audience",
        "count_ratings",
    ]
    return df_final[[c for c in final_columns if c in df_final.columns]]