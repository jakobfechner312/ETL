# kobus_testing/transform/outlier_treatment.py

import pandas as pd
import numpy as np
import logging

def treat_outliers(series: pd.Series, method: str = 'cap', iqr_factor: float = 1.5, lower_percentile: float = 0.05, upper_percentile: float = 0.95) -> pd.Series:
    """
    Behandelt Ausreißer in einer Serie entweder durch Capping (Winsorizing basierend auf IQR)
    oder durch Setzen auf NaN oder durch Perzentil-basiertes Winsorizing.

    Args:
        series: Die zu bereinigende Serie.
        method: Methode zur Ausreißerbehandlung.
                'cap': Cappt Werte an den IQR-Grenzen (Standard).
                'nan': Setzt Ausreißer auf NaN (basierend auf IQR).
                'percentile_cap': Cappt Werte an den angegebenen Perzentilen.
                'none': Überspringt die Ausreißerbehandlung.
        iqr_factor: Faktor für die IQR-Berechnung (relevant für 'cap' und 'nan').
        lower_percentile: Unteres Perzentil für 'percentile_cap'.
        upper_percentile: Oberes Perzentil für 'percentile_cap'.

    Returns:
        Bereinigte Serie.
    """
    series_copy = series.copy()
    
    if method == 'none':
        logging.debug(f"Outlier treatment method is 'none' for series. Series not modified.")
        return series_copy

    valid_series_for_treatment = series_copy.dropna()
    if valid_series_for_treatment.empty or not pd.api.types.is_numeric_dtype(valid_series_for_treatment) or valid_series_for_treatment.nunique() < 2:
        logging.debug(f"Series is empty, not sufficiently numeric, or has less than 2 unique non-NaN values. Skipping outlier treatment for method {method}.")
        return series_copy
        
    Q1 = series_copy.quantile(0.25) 
    Q3 = series_copy.quantile(0.75)
    IQR = Q3 - Q1

    if pd.isna(Q1) or pd.isna(Q3) or (IQR == 0 and (method == 'cap' or method == 'nan')):
        logging.debug(f"IQR is 0 or Q1/Q3 is NaN for method {method}. No outliers to treat based on IQR. Series remains unchanged for series.")
        return series_copy

    lower_bound_iqr = Q1 - iqr_factor * IQR
    upper_bound_iqr = Q3 + iqr_factor * IQR

    if method == 'cap':
        series_copy.loc[series_copy < lower_bound_iqr] = lower_bound_iqr
        series_copy.loc[series_copy > upper_bound_iqr] = upper_bound_iqr
        logging.debug(f"Outliers capped using IQR method. Lower: {lower_bound_iqr}, Upper: {upper_bound_iqr} for series.")
    elif method == 'nan':
        series_copy.loc[(series_copy < lower_bound_iqr) | (series_copy > upper_bound_iqr)] = np.nan
        logging.debug(f"Outliers set to NaN using IQR method. Lower: {lower_bound_iqr}, Upper: {upper_bound_iqr} for series.")
    elif method == 'percentile_cap':
        if not (0 <= lower_percentile < upper_percentile <= 1): 
            logging.warning(f"Invalid percentiles: lower={lower_percentile}, upper={upper_percentile}. Skipping percentile_cap for series.")
            return series_copy
            
        lower_p_val = series_copy.quantile(lower_percentile)
        upper_p_val = series_copy.quantile(upper_percentile)
        
        if pd.isna(lower_p_val) or pd.isna(upper_p_val):
            logging.warning(f"Cannot calculate percentile values (lower_p_val={lower_p_val}, upper_p_val={upper_p_val}). Skipping percentile_cap for series.")
            return series_copy

        series_copy.loc[series_copy < lower_p_val] = lower_p_val
        series_copy.loc[series_copy > upper_p_val] = upper_p_val
        logging.debug(f"Outliers capped using percentile method. Lower P: {lower_p_val}, Upper P: {upper_p_val} for series.")
    elif method not in ['cap', 'nan', 'percentile_cap', 'none']: 
        logging.warning(f"Unknown outlier treatment method: {method}. Series not modified for series.")
    
    return series_copy