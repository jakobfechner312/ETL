import logging
from typing import List, Tuple
from datetime import datetime

import pandas as pd


CURRENT_YEAR: int = datetime.now().year
YEAR_MIN: int = 1888  # erstes Filmjahr (Roundhay Garden Scene)
YEAR_MAX: int = CURRENT_YEAR + 1  # etwas Toleranz

# Spalten, die immer vorhanden sein sollten – kann vom Aufrufer erweitert werden
REQUIRED_BASE_COLS: List[str] = ["title", "year"]


def _detect_rating_columns(df: pd.DataFrame) -> List[str]:
    """Ermittle Rating-Spalten heuristisch (rating_*, *_norm, superscore_*)."""
    rating_like = [
        col
        for col in df.columns
        if col.startswith("rating_") or col.endswith("_norm") or col.startswith("superscore_")
    ]
    return rating_like


def validate_dataframe(
    df: pd.DataFrame,
    *,
    required_cols: List[str] | None = None,
    allow_empty: bool = False,
    df_name: str | None = None,
    log_level: int = logging.WARNING,
) -> Tuple[bool, List[str]]:
    """Führt eine Reihe grundlegender Validierungen durch.

    Args:
        df: Zu prüfendes DataFrame.
        required_cols: Zusätzliche Pflichtspalten neben den Basis-Spalten.
        allow_empty: Falls True, wird ein komplett leeres DF nicht als Fehler gewertet.
        df_name: Optionaler Name für bessere Fehlermeldungen.
        log_level: Logging-Level für gefundene Fehler.

    Returns:
        Tuple (is_valid, errors). is_valid == True wenn keine Fehler gefunden.
    """
    name = df_name or "DataFrame"
    errors: List[str] = []

    # 0) Empty-Check
    if df.empty and not allow_empty:
        errors.append(f"{name} ist leer.")

    # 1) Pflichtspalten
    req_cols = set(REQUIRED_BASE_COLS + (required_cols or []))
    missing = req_cols.difference(df.columns)
    if missing:
        errors.append(f"{name}: fehlende Spalten: {', '.join(sorted(missing))}")

    # 2) year-Range & Typ
    if "year" in df.columns:
        invalid_year_mask = (~df["year"].between(YEAR_MIN, YEAR_MAX)) | df["year"].isna()
        if invalid_year_mask.any():
            n_bad = invalid_year_mask.sum()
            errors.append(f"{name}: {n_bad} Zeilen mit ungültigem Jahr (<{YEAR_MIN} oder >{YEAR_MAX} oder NaN).")
    
    # 3) Rating-Spalten numeric + 0-10 Range
    for col in _detect_rating_columns(df):
        if not pd.api.types.is_numeric_dtype(df[col]):
            errors.append(f"{name}: Spalte {col} ist nicht numerisch (dtype={df[col].dtype}).")
            continue
        bad_mask = (~df[col].between(0, 10)) & (~df[col].isna())
        if bad_mask.any():
            n_bad = bad_mask.sum()
            errors.append(f"{name}: {n_bad} Werte außerhalb 0-10 in {col}.")

    # 4) Duplikate title+year
    if "title" in df.columns and "year" in df.columns:
        dupes = df.duplicated(subset=["title", "year"], keep=False)
        if dupes.any():
            errors.append(
                f"{name}: {dupes.sum()} Zeilen sind doppelt hinsichtlich (title, year)."
            )

    # Logging
    for msg in errors:
        logging.log(log_level, msg)

    return len(errors) == 0, errors


# Praktischer Wrapper ---------------------------------------------------------

def validate_or_raise(
    df: pd.DataFrame,
    **kwargs,
) -> None:
    """Validiert und wirft ValueError, falls Probleme entdeckt werden."""
    ok, errs = validate_dataframe(df, **kwargs)
    if not ok:
        joined = "\n - ".join(errs)
        raise ValueError(f"Validation Fehler:\n - {joined}") 