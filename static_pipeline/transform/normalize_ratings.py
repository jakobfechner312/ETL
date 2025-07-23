# kobus_testing/transform/normalize_ratings.py

import pandas as pd
import numpy as np
import logging 
# Importiere die treat_outliers Funktion aus der neuen Datei
from .outlier_treatment import treat_outliers 

def calculate_normalized_ratings_and_superscores(
    df_input: pd.DataFrame, 
    min_ratings_for_superscore: int = 2,
    outlier_treatment_method: str = 'cap', 
    outlier_iqr_factor: float = 1.5,
    outlier_lower_percentile: float = 0.05,
    outlier_upper_percentile: float = 0.95
) -> pd.DataFrame:
    """
    Führt die lineare Normalisierung der Ratings durch, behandelt Ausreißer 
    (optional mit verschiedenen Methoden) und berechnet Superscores.
    
    Args:
        df_input: Input DataFrame.
        min_ratings_for_superscore: Minimale Anzahl benötigter Ratings für Superscore.
        outlier_treatment_method: Methode zur Ausreißerbehandlung ('cap', 'nan', 
                                  'percentile_cap', 'none'). 'cap' ist Standard.
        outlier_iqr_factor: IQR-Faktor für 'cap' und 'nan' Methoden.
        outlier_lower_percentile: Unteres Perzentil für 'percentile_cap'.
        outlier_upper_percentile: Oberes Perzentil für 'percentile_cap'.

    Returns:
        DataFrame mit normalisierten Ratings und Superscores.
    """
    df = df_input.copy() 
    logging.info(f"Normalize_ratings: DataFrame Spalten VOR der Verarbeitung: {df.columns.tolist()}")

    # 1. Lineare Normalisierung der Einzelratings
    df['imdb_norm'] = df.get('rating_imdb', pd.Series(dtype='float64'))
    df['movielens_norm'] = df.get('rating_movielens', pd.Series(dtype='float64')) * 2
    df['metacritic_norm'] = df.get('rating_metacritic', pd.Series(dtype='float64')) / 10
    
    source_rt_series = df.get('rating_rt_audience', pd.Series(dtype='float64'))
    # Sicherstellen, dass die Quellspalte numerisch ist, bevor dividiert wird.
    # .dropna() wird verwendet, um sicherzustellen, dass eine Serie, die nur aus NaNs besteht, nicht fälschlicherweise als numerisch gilt.
    if pd.api.types.is_numeric_dtype(source_rt_series.dropna()):
        df['rt_norm'] = source_rt_series / 10
    else: 
        logging.warning(f"Normalize_ratings: Spalte 'rating_rt_audience' ist nicht durchgehend numerisch (Typ: {source_rt_series.dtype}). Versuche Konvertierung für 'rt_norm'.")
        df['rt_norm'] = pd.to_numeric(source_rt_series, errors='coerce') / 10

    # 2. Behandlung von Ausreißern NACH der Normalisierung auf 0-10
    norm_cols_to_treat = ['imdb_norm', 'movielens_norm', 'metacritic_norm', 'rt_norm']
    if outlier_treatment_method != 'none': # Überprüft, ob Ausreißerbehandlung überhaupt durchgeführt werden soll
        logging.info(f"Normalize_ratings: Starte Ausreißerbehandlung mit Methode '{outlier_treatment_method}'.")
        for col in norm_cols_to_treat:
            if col in df.columns and df[col].notna().any(): # Nur behandeln, wenn Spalte existiert und nicht nur NaNs enthält
                original_series_for_log = df[col].copy() # Für Logging der Änderungen
                
                # Aufruf der importierten Funktion
                df[col] = treat_outliers(
                    df[col], 
                    method=outlier_treatment_method, 
                    iqr_factor=outlier_iqr_factor,
                    lower_percentile=outlier_lower_percentile,
                    upper_percentile=outlier_upper_percentile
                )
                
                # Einfaches Logging, ob sich etwas geändert hat
                if not original_series_for_log.equals(df[col]):
                    logging.info(f"Normalize_ratings: Werte in {col} durch Ausreißerbehandlung verändert.")
                else:
                    logging.debug(f"Normalize_ratings: Keine Änderungen in {col} durch Ausreißerbehandlung.")
            elif col in df.columns: # Fall: Spalte existiert, enthält aber nur NaNs oder ist leer
                 logging.debug(f"Normalize_ratings: Spalte {col} enthält nur NaNs oder ist leer, keine Ausreißerbehandlung.")
            else: # Fall: Spalte existiert nicht im DataFrame
                logging.debug(f"Normalize_ratings: Spalte {col} nicht im DataFrame für Ausreißerbehandlung gefunden.")
    else:
        logging.info("Normalize_ratings: Ausreißerbehandlung übersprungen (method='none').")
    
    existing_norm_cols = [col for col in norm_cols_to_treat if col in df.columns]
    logging.info(f"Normalize_ratings: Vorhandene normalisierte Spalten (existing_norm_cols) für Superscore-Berechnung: {existing_norm_cols}")

    # 3. Zählen der verfügbaren normalisierten Ratings pro Film
    if existing_norm_cols:
        df['num_available_ratings'] = df[existing_norm_cols].notna().sum(axis=1)
    else:
        df['num_available_ratings'] = 0
        logging.warning("Normalize_ratings: Keine der spezifizierten normalisierten Spalten in existing_norm_cols gefunden! 'num_available_ratings' wird auf 0 gesetzt.")

    # 4. DataFrame für die Superscore-Berechnung (nur Filme, die die Bedingung erfüllen)
    df_for_superscores = df[df['num_available_ratings'] >= min_ratings_for_superscore].copy()
    
    potential_new_cols = ['superscore_mean', 'superscore_median'] 
    for p_col in potential_new_cols:
        if p_col not in df.columns:
             df[p_col] = np.nan

    if not df_for_superscores.empty and existing_norm_cols:
        # 5. Superscore-Berechnung (Mittelwert und Median auf 0-10 Skala)
        df_for_superscores.loc[:, 'superscore_mean'] = df_for_superscores[existing_norm_cols].mean(axis=1, skipna=True)
        df_for_superscores.loc[:, 'superscore_median'] = df_for_superscores[existing_norm_cols].median(axis=1, skipna=True)
            
        cols_calculated_in_df_for_superscores = ['superscore_mean', 'superscore_median'] 
        valid_cols_to_update = [c for c in cols_calculated_in_df_for_superscores if c in df_for_superscores.columns]

        if valid_cols_to_update:
            df.update(df_for_superscores[valid_cols_to_update]) 
            logging.info(f"Normalize_ratings: Superscores für {len(df_for_superscores)} Filme aktualisiert.")
        else:
            logging.warning("Normalize_ratings: Keine gültigen Superscore-Spalten zum Aktualisieren im Haupt-DataFrame gefunden.")
            
    else: 
        if df_for_superscores.empty:
            logging.info("Normalize_ratings: DataFrame für Superscore-Berechnung (df_for_superscores) ist leer.")
        if not existing_norm_cols: 
            logging.warning("Normalize_ratings: Keine existierenden normalisierten Spalten (existing_norm_cols) für Superscore-Berechnung vorhanden.")
            
    return df