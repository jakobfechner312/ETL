import logging
from typing import List, Tuple
from datetime import datetime
import pandas as pd
from pathlib import Path

CURRENT_YEAR: int = datetime.now().year
YEAR_MIN: int = 1888
YEAR_MAX: int = CURRENT_YEAR + 1
REQUIRED_BASE_COLS: List[str] = [
    "title"
]  # 'year' wird optional über YEAR_COLUMN_CANDIDATES behandelt
YEAR_COLUMN_CANDIDATES: List[str] = ["year", "release_year"]
RATING_COLUMN_RANGES: dict[str, Tuple[float, float]] = {
    "rating_imdb": (0, 10),
    "rating_movielens": (0, 5),
    "rating_metacritic": (0, 100),
    "rating_rt_audience": (0, 100),
}


def _detect_rating_columns(df: pd.DataFrame) -> List[str]:
    return [
        col for col in df.columns if col.startswith("rating_") or
        col.endswith("_norm") or col.startswith("superscore_")
    ]


def validate_dataframe(
    df: pd.DataFrame,
    *,
    required_cols: List[str] | None = None,
    allow_empty: bool = False,
    df_name: str | None = None,
    log_level: int = logging.WARNING,
    custom_rating_checks: dict[str, Tuple[float, float]] | None = None,
    save_duplicates: bool = False,
    duplicates_output_path: str = "duplicates_found.csv",
    error_report_path: str | None = None,
    save_invalid_rows: bool = False,
    invalid_rows_output_path: str = "invalid_rows_found.csv",
) -> Tuple[bool, List[str]]:
    name = df_name or "DataFrame"
    errors: List[str] = []
    invalid_rows_parts: List[pd.DataFrame] = []

    # 0) Leerer DataFrame
    if df.empty and not allow_empty:
        errors.append(f"{name} ist leer.")

    # 1) Pflichtspalten prüfen
    req_cols = set(REQUIRED_BASE_COLS + (required_cols or []))
    missing = req_cols.difference(df.columns)
    if missing:
        errors.append(f"{name}: fehlende Spalten: {', '.join(sorted(missing))}")

    # 2) Jahr-Spalte ermitteln (year oder release_year)
    year_col = next(
        (col for col in YEAR_COLUMN_CANDIDATES if col in df.columns), None)
    if year_col:
        invalid_year_mask = (
            ~df[year_col].between(YEAR_MIN, YEAR_MAX)) | df[year_col].isna()
        if invalid_year_mask.any():
            n_bad = invalid_year_mask.sum()
            errors.append(
                f"{name}: {n_bad} Zeilen mit ungültigem Jahr (<{YEAR_MIN} oder >{YEAR_MAX} oder NaN) in Spalte '{year_col}'."
            )
            if save_invalid_rows:
                invalid_rows_parts.append(df[invalid_year_mask])
    else:
        errors.append(
            f"{name}: fehlende Jahr-Spalte ('year' oder 'release_year').")

    # 3) Ratingspalten prüfen
    if custom_rating_checks:
        for col, (low, high) in custom_rating_checks.items():
            if col not in df.columns:
                errors.append(f"{name}: erwartete Ratingspalte fehlt: {col}")
                continue
            if not pd.api.types.is_numeric_dtype(df[col]):
                errors.append(
                    f"{name}: Spalte {col} ist nicht numerisch (dtype={df[col].dtype})."
                )
                continue
            bad_mask = (~df[col].between(low, high)) & (~df[col].isna())
            if bad_mask.any():
                n_bad = bad_mask.sum()
                errors.append(
                    f"{name}: {n_bad} Werte außerhalb {low}–{high} in {col}.")
                if save_invalid_rows:
                    invalid_rows_parts.append(df[bad_mask])
    else:
        # Datenset-spezifische Standardranges
        for col in _detect_rating_columns(df):
            if not pd.api.types.is_numeric_dtype(df[col]):
                errors.append(
                    f"{name}: Spalte {col} ist nicht numerisch (dtype={df[col].dtype})."
                )
                continue
            # Range bestimmen: spezielle Vorgabe, Norm/Superscore oder Fallback 0–10
            if col.endswith("_norm") or col.startswith("superscore_"):
                low, high = 0, 10
            elif col.startswith("rating_rt_"):
                low, high = 0, 100
            else:
                low, high = RATING_COLUMN_RANGES.get(col, (0, 10))
            bad_mask = (~df[col].between(low, high)) & (~df[col].isna())
            if bad_mask.any():
                n_bad = bad_mask.sum()
                errors.append(
                    f"{name}: {n_bad} Werte außerhalb {low}–{high} in {col}.")
                if save_invalid_rows:
                    invalid_rows_parts.append(df[bad_mask])

    # 4) Duplikate title+year
    if "title" in df.columns and year_col:
        dupes = df.duplicated(subset=["title", year_col], keep=False)
        if dupes.any():
            errors.append(
                f"{name}: {dupes.sum()} Zeilen sind doppelt hinsichtlich (title, {year_col})."
            )
        if save_duplicates:
            try:
                out_dup_path = Path(duplicates_output_path)
                out_dup_path.parent.mkdir(parents=True, exist_ok=True)
                if dupes.any():
                    df[dupes].to_csv(out_dup_path, index=False)
                else:
                    # Leere CSV mit Header speichern
                    df.head(0).to_csv(out_dup_path, index=False)
                logging.info(
                    f"{name}: Duplikate gespeichert unter {out_dup_path} (Anzahl: {dupes.sum()})"
                )
            except Exception as e:
                errors.append(
                    f"{name}: Fehler beim Speichern der Duplikate: {e}")

    for msg in errors:
        logging.log(log_level, msg)

    # --- Fehlerhafte Zeilen speichern ---
    if save_invalid_rows:
        try:
            if invalid_rows_parts:
                invalid_df = pd.concat(invalid_rows_parts)
                try:
                    invalid_df = invalid_df.drop_duplicates()
                except TypeError:
                    # Manche Spalten enthalten Listen/ungehashbare Objekte -> ohne Duplikat-Entfernung fortfahren
                    invalid_df = invalid_df.reset_index(drop=True)
            else:
                # Leere CSV mit Spaltenkopf erstellen
                invalid_df = df.head(0).copy()
            out_path = Path(invalid_rows_output_path)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            invalid_df.to_csv(out_path, index=False)
            logging.info(
                f"{name}: Fehlerhafte Zeilen gespeichert unter {out_path} (Anzahl: {len(invalid_df)})"
            )
        except Exception as e:
            logging.error(
                f"{name}: Fehler beim Speichern fehlerhafter Zeilen: {e}")

    # --- Fehlerreport speichern ---
    if error_report_path and errors:
        try:
            rep_path = Path(error_report_path)
            rep_path.parent.mkdir(parents=True, exist_ok=True)
            rep_path.write_text("\n".join(errors), encoding="utf-8")
            logging.info(f"{name}: Fehlerreport gespeichert unter {rep_path}")
        except Exception as e:
            logging.error(
                f"{name}: Fehler beim Speichern des Fehlerreports: {e}")

    return len(errors) == 0, errors


def validate_or_raise(
    df: pd.DataFrame,
    **kwargs,
) -> None:
    ok, errs = validate_dataframe(df, **kwargs)
    if not ok:
        joined = "\n - ".join(errs)
        raise ValueError(f"Validation Fehler:\n - {joined}")
