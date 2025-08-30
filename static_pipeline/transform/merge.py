from __future__ import annotations

import json
import re
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd
from transform.normalize import normalize_film_title

# Fallback für unidecode
try:
    from unidecode import unidecode
except ImportError:  # pragma: no cover
    unidecode = lambda s: s  # type: ignore

# Pfade wie bei dir
UNFILTERED_OUT = Path("static_pipeline/data/processed/all_movies_wide_unfiltered.csv")
DUPLICATES_OUT = Path("static_pipeline/data/processed/all_movies_fuzzy_duplicates.csv")

def norm_title(title: str) -> str:
    # exakt dieselbe Normalisierung wie in deinen Adaptern
    return normalize_film_title(title) if isinstance(title, str) else ""

def _cluster_years(unique_years: list[int]) -> dict[int, int]:
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
    years = [int(y) for y in series.dropna()]
    mapping = _cluster_years(years)
    next_id = (max(mapping.values()) + 1) if mapping else 0
    return series.map(mapping).fillna(next_id).astype(int)

def _first(series: pd.Series):
    for val in series:
        if isinstance(val, list) and val:
            return val
        if isinstance(val, str) and val.strip():
            return val
    return []

def _first_valid(series: pd.Series):
    s = series.dropna()
    return s.iloc[0] if not s.empty else pd.NA

def merge_sources(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    """
    Statischer Merge mit:
      • Titel-Normalisierung (zentral)
      • ±1-Jahr-Cluster pro norm_title
      • Long→Wide Aggregation
      • Genres: erste nicht-leere Liste
      • Filter: nur Filme mit ≥2 vorhandenen Ratings
    Rückgabe: EIN DataFrame (wie zuvor), damit main_pipeline.py NICHT bricht.
    Duplikate werden zusätzlich als CSV persistiert (Nebenwirkung), aber NICHT zurückgegeben.
    """
    if not dfs:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    for df in dfs:
        if df is None or df.empty:
            continue

        tmp = df.copy()

        # Rating-Spalte identifizieren
        rating_col, source = None, None
        for col in tmp.columns:
            if col.startswith("rating_"):
                rating_col = col
                source = col.replace("rating_", "")
                break
            if col == "tomatometer_rating":
                rating_col, source = col, "rt_audience"
                break
        if rating_col is None:
            continue

        tmp["source"] = source

        # Genres übernehmen (erste "genres*"-Spalte, sonst leere Liste)
        genre_col = next((c for c in tmp.columns if c.startswith("genres")), None)
        tmp["genres"] = tmp[genre_col] if genre_col else [[]] * len(tmp)

        # release_date absichern
        if "release_date" not in tmp.columns:
            tmp["release_date"] = pd.NaT

        # ID-Spalten (ID_*) mitführen
        id_cols = [c for c in tmp.columns if str(c).startswith("ID_")]

        cols_to_keep = ["title", "year", "release_date", "genres", "source", rating_col] + id_cols
        frames.append(tmp[cols_to_keep])

    if not frames:
        return pd.DataFrame()

    long_df = pd.concat(frames, ignore_index=True)

    # Titel normalisieren + release_year sauber typisieren
    long_df["norm_title"] = long_df["title"].apply(norm_title)
    long_df["release_year"] = pd.to_numeric(long_df["year"], errors="coerce").astype("Int64")

    # Optional: film_sig-Kanalisierung (IDs bündeln) – identisch wie bei dir
    id_cols_sig = [c for c in long_df.columns if str(c).startswith("ID_")]
    if id_cols_sig:
        def _build_film_sig(row: pd.Series) -> str:
            parts = []
            for c in id_cols_sig:
                v = row.get(c, pd.NA)
                if pd.notna(v):
                    parts.append(f"{c}={str(v)}")
            if not parts:
                return ""
            parts.sort()
            return "|".join(parts)

        long_df["film_sig"] = long_df.apply(_build_film_sig, axis=1)

        def _unify_group(g: pd.DataFrame) -> pd.DataFrame:
            # nur echte Signaturen mit >=2 Zeilen vereinheitlichen
            if not isinstance(g.name, str) or g.name == "" or len(g) < 2:
                return g
            counts = g["norm_title"].value_counts(dropna=False)
            max_count = counts.max()
            candidates = counts[counts == max_count].index.tolist()
            best_title = max(candidates, key=lambda s: len(s) if isinstance(s, str) else 0)
            min_year = pd.to_numeric(g["release_year"], errors="coerce").min()
            g = g.copy()
            g["norm_title"] = best_title
            g["release_year"] = pd.Series([min_year] * len(g), index=g.index).astype("Int64")
            return g

        # FutureWarning vermeiden: include_groups=False (wo verfügbar)
        try:
            long_df = long_df.groupby("film_sig", group_keys=False, include_groups=False).apply(_unify_group)
        except TypeError:
            long_df = long_df.groupby("film_sig", group_keys=False).apply(_unify_group)

    # Year-Cluster pro norm_title
    long_df["year_cluster"] = (
        long_df.groupby("norm_title", group_keys=False)["release_year"].apply(year_cluster)
    )

    # Ratings aggregieren (first_valid)
    group_cols = ["norm_title", "year_cluster"]
    rating_cols_present = [c for c in long_df.columns if str(c).startswith("rating_")]
    if rating_cols_present:
        ratings_wide = (
            long_df.groupby(group_cols, as_index=False)[rating_cols_present].agg(_first_valid)
        )
    else:
        ratings_wide = long_df[group_cols].drop_duplicates()

    # Meta (Titel, min Jahr, min release_date)
    meta = (
        long_df.sort_values(["source", "title"])
               .groupby(group_cols)
               .agg(
                   title=("title", "first"),
                   year=("release_year", "min"),
                   release_date=("release_date", "min"),
               )
               .reset_index()
    )
    wide = ratings_wide.merge(meta, on=group_cols, how="left")

    # Genres: erste nicht-leere Liste
    genres_map = (
        long_df.sort_values("source")
               .groupby(group_cols)["genres"]
               .apply(_first)
               .reset_index()
    )
    wide = wide.merge(genres_map, on=group_cols, how="left")

    # IDs je Film (erste gültige je Quelle)
    id_cols_in_long = [c for c in long_df.columns if str(c).startswith("ID_")]
    if id_cols_in_long:
        ids_map = (
            long_df.groupby(group_cols, as_index=False)[id_cols_in_long].agg(_first_valid)
        )
        wide = wide.merge(ids_map, on=group_cols, how="left")

    # Erwartete Rating-Spalten sicherstellen
    for col in ["rating_imdb","rating_movielens","rating_metacritic","rating_rt_audience"]:
        if col not in wide.columns:
            wide[col] = pd.NA

    # Unfiltered-Snapshot schreiben (wie bisher)
    UNFILTERED_OUT.parent.mkdir(parents=True, exist_ok=True)
    wide.to_csv(UNFILTERED_OUT, index=False)
    print(f"💾 Ungefiltertes Wide-Ergebnis gespeichert: {UNFILTERED_OUT}")

    # Count ratings & Filter k ≥ 2
    wide["count_ratings"] = wide[["rating_imdb","rating_movielens","rating_metacritic","rating_rt_audience"]].notna().sum(axis=1)
    df_final = wide[wide["count_ratings"] >= 2].copy()

    # Sortierung (deine Logik)
    df_final = df_final.sort_values(["count_ratings", "year"], ascending=[False, False]).reset_index(drop=True)

    # Spaltenharmonisierung: year → release_year (release_date behalten!)
    df_final = df_final.rename(columns={"year": "release_year"})

    # Duplikate identifizieren & als Datei persistieren (aber NICHT zurückgeben)
    dup_mask = df_final.duplicated(subset=["title", "release_year"], keep=False)
    duplicates = df_final[dup_mask].copy()
    if not duplicates.empty:
        DUPLICATES_OUT.parent.mkdir(parents=True, exist_ok=True)
        duplicates.to_csv(DUPLICATES_OUT, index=False)
        print(f"🟠 Duplikate persistiert: {DUPLICATES_OUT} (Zeilen: {len(duplicates)})")

    # Finale Spaltenauswahl
    id_cols_final = [c for c in df_final.columns if str(c).startswith("ID_")]
    final_columns = [
        *id_cols_final,
        "title",
        "release_year",
        "genres",
        "release_date",
        "rating_imdb",
        "rating_movielens",
        "rating_metacritic",
        "rating_rt_audience",
        "count_ratings",
    ]
    df_final = df_final[[c for c in final_columns if c in df_final.columns]]

    # Rückgabe
    return df_final