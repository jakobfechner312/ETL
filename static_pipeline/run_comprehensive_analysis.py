# kobus_testing/run_comprehensive_analysis.py
import yaml
import logging
from pathlib import Path
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

# --- Globale Stil-Einstellung für Plots ---
plt.style.use('seaborn-v0_8-whitegrid')

# === Funktionen aus rating_analysis_module.py ===

def run_distribution_plots(df: pd.DataFrame, output_dir: Path, analysis_phase: str, plot_configs_override: dict | None = None):
    output_dir.mkdir(parents=True, exist_ok=True)
    logging.info(f"Erstelle Verteilungsplots für Phase '{analysis_phase}' in '{output_dir}'...")

    default_plot_configs = {
        "raw_ratings": {
            "cols": {'rating_imdb': 'IMDB (Original)', 'rating_movielens': 'MovieLens (Original)',
                     'rating_metacritic': 'Metacritic (Original)', 'rating_rt_audience': 'Rotten Tomatoes (Original)'},
            "suptitle": "Verteilung der Original-Ratings (vor Normalisierung)",
            "filename": "dist_01_raw_ratings.png", "kde": False, "xlabel": "Rating"
        },
        "normalized_ratings": {
            "cols": {'imdb_norm': 'IMDB (0-10)', 'movielens_norm': 'MovieLens (0-10)',
                     'metacritic_norm': 'Metacritic (0-10)', 'rt_norm': 'Rotten Tomatoes (0-10)'},
            "suptitle": "Verteilung der linear normalisierten Ratings (0-10)",
            "filename": "dist_02_normalized_ratings.png", "kde": False, "xlabel": "Normalisiertes Rating (0-10)"
        },
        "superscores_0_10": {
            "cols": {'superscore_mean': 'Superscore (Mittelwert, 0-10)', 'superscore_median': 'Superscore (Median, 0-10)'},
            "suptitle": "Verteilung der Superscores (0-10 Skala)",
            "filename": "dist_03_superscores_0_10.png", "kde": True, "xlabel": "Superscore (0-10)"
        }
    }
    plot_configs = plot_configs_override if plot_configs_override else default_plot_configs

    config = plot_configs.get(analysis_phase)
    if not config:
        logging.warning(f"Keine Plot-Konfiguration für Analysephase '{analysis_phase}' gefunden.")
        return

    cols_to_plot = config["cols"]
    valid_cols_to_plot = {k: v for k, v in cols_to_plot.items() if k in df.columns and not df[k].dropna().empty}

    if not valid_cols_to_plot:
        logging.info(f"Keine gültigen Daten für Plots in Phase '{analysis_phase}'.")
        return

    num_plots = len(valid_cols_to_plot)
    if num_plots == 0: return

    ncols_subplot = 2 if num_plots > 1 else 1
    nrows_subplot = (num_plots + ncols_subplot - 1) // ncols_subplot

    fig, axes = plt.subplots(nrows_subplot, ncols_subplot, figsize=(7 * ncols_subplot, 6 * nrows_subplot), squeeze=False)
    axes_flat = axes.flatten()

    plot_idx = 0
    for col, title in valid_cols_to_plot.items():
        ax = axes_flat[plot_idx]
        label_text = title.split('(')[-1].split(',')[0].strip() if '(' in title else title
        sns.histplot(df[col].dropna(), bins=20, ax=ax, kde=config.get("kde", False), label=label_text) # .get für kde
        ax.set_title(title)
        ax.set_xlabel(config.get("xlabel", "Wert")) # .get für xlabel
        if plot_idx % ncols_subplot == 0:
            ax.set_ylabel('Anzahl Filme')
        ax.legend()
        plot_idx += 1

    for i in range(plot_idx, len(axes_flat)):
        fig.delaxes(axes_flat[i])

    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    fig.suptitle(config["suptitle"], fontsize=16)
    file_path = output_dir / config["filename"]
    plt.savefig(file_path)
    plt.close(fig)
    logging.info(f"Plot '{config['filename']}' gespeichert in '{file_path}'.")


def run_correlation_plots(df: pd.DataFrame, output_dir: Path, analysis_phase: str, corr_plot_configs_override: dict | None = None):
    output_dir.mkdir(parents=True, exist_ok=True)
    logging.info(f"Erstelle Korrelationsplots für Phase '{analysis_phase}' in '{output_dir}'...")
    
    default_corr_plot_configs = {
        "raw_ratings": {
            "cols_for_corr": ['rating_imdb', 'rating_movielens', 'rating_metacritic', 'rating_rt_audience'],
            "filename_suffix": "corr_heatmap_01_raw_ratings.png",
            "title_suffix": "Korrelationen der Original-Ratings"
        },
        "normalized_and_superscores": {
            "cols_for_corr": ['imdb_norm', 'movielens_norm', 'metacritic_norm', 'rt_norm', 'superscore_mean', 'superscore_median'],
            "filename_suffix": "corr_heatmap_02_normalized_superscores.png",
            "title_suffix": "Korrelationen (Linear normalisierte Ratings & Superscores 0-10)"
        }
    }
    corr_plot_configs = corr_plot_configs_override if corr_plot_configs_override else default_corr_plot_configs

    config = corr_plot_configs.get(analysis_phase)
    if not config:
        logging.warning(f"Keine Korrelations-Plot-Konfiguration für Analysephase '{analysis_phase}'.")
        return

    cols_for_corr = config["cols_for_corr"]
    valid_cols = [col for col in cols_for_corr if col in df.columns and not df[col].dropna().empty]
    if len(valid_cols) < 2:
        logging.info(f"Nicht genügend Daten für Korrelationsmatrix in Phase '{analysis_phase}'.")
        return

    corr_matrix = df[valid_cols].corr()
    plt.figure(figsize=(max(8, len(valid_cols)), max(6, len(valid_cols)-2)))
    sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', vmin=-1, vmax=1, fmt=".2f")
    plt.title(config["title_suffix"])
    plt.tight_layout()
    file_path = output_dir / config["filename_suffix"]
    plt.savefig(file_path)
    plt.close()
    logging.info(f"Korrelationsplot '{config['filename_suffix']}' gespeichert in '{file_path}'.")

    # Scatterplot für Superscores (falls relevant für die Phase)
    if analysis_phase == "normalized_and_superscores" and \
       'superscore_mean' in valid_cols and 'superscore_median' in valid_cols:
        
        scatter_output_path = output_dir / "scatter_superscore_mean_vs_median.png"
        plt.figure(figsize=(8,6))
        sns.scatterplot(x=df['superscore_mean'], y=df['superscore_median'], alpha=0.3)
        
        min_val_mean_series = df['superscore_mean'].dropna()
        min_val_median_series = df['superscore_median'].dropna()

        if not min_val_mean_series.empty and not min_val_median_series.empty:
            min_val_mean = min_val_mean_series.min()
            min_val_median = min_val_median_series.min()
            max_val_mean = min_val_mean_series.max()
            max_val_median = min_val_median_series.max()
            overall_min = min(min_val_mean, min_val_median)
            overall_max = max(max_val_mean, max_val_median)
            plt.plot([overall_min, overall_max], [overall_min, overall_max], color='red', linestyle='--')
        else:
            logging.warning("Konnte Min/Max für Scatterplot-Linie nicht bestimmen (Superscore-Spalten nach dropna() leer).")
            
        plt.xlabel('Superscore (Mittelwert, 0–10)')
        plt.ylabel('Superscore (Median, 0–10)')
        plt.title('Superscore Mittelwert vs. Median (0–10 Skala)')
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.tight_layout()
        plt.savefig(scatter_output_path)
        plt.close()
        logging.info(f"Scatterplot 'scatter_superscore_mean_vs_median.png' gespeichert in '{scatter_output_path}'.")

# === Funktionen aus merge_analyzer.py (angepasst für Pfad-Normalisierung) ===
# Annahme: normalize_film_title ist entweder hier definiert oder korrekt importiert
# Für dieses Beispiel gehen wir davon aus, dass die ETL main_pipeline.py
# die `normalize_film_title` Funktion im `transform` Modul hat.
# Wenn dieses Skript komplett eigenständig sein soll und `transform.normalize` nicht einfach importierbar ist,
# müsste die Funktion hierher kopiert werden.
try:
    from transform.normalize import normalize_film_title
except ImportError:
    logging.warning("Funktion normalize_film_title nicht gefunden. Detailanalyse in generate_merge_analysis_report könnte fehlschlagen.")
    # Fallback-Dummy-Funktion, damit das Skript nicht direkt crasht
    def normalize_film_title(title: str) -> str:
        return title.lower().strip() if isinstance(title, str) else ""


def generate_merge_analysis_report(
    merged_df: pd.DataFrame,
    report_path: Path,
    original_dfs: dict[str, pd.DataFrame] | None = None
):
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_lines = []
    report_lines.append("======================================")
    report_lines.append("     Umfassender Merge-Analyse-Bericht     ")
    report_lines.append("======================================")
    report_lines.append(f"Datum der Analyse: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    NO_YEAR_STR_KEY = "###NO_YEAR###"

    report_lines.append("--- Allgemeine Statistiken (Finaler Merge) ---")
    report_lines.append(f"Gesamtzahl der einzigartigen Filme im Ergebnis: {len(merged_df)}")
    
    rating_columns_present = [col for col in ['rating_imdb', 'rating_movielens', 'rating_rt_audience', 'rating_rt_audience', 
                                            'imdb_norm', 'movielens_norm', 'metacritic_norm', 'rt_norm', 
                                            'superscore_mean', 'superscore_median'] if col in merged_df.columns]
    if rating_columns_present:
        report_lines.append("\nRating-Spalten im gemergeten DataFrame vorhanden:")
        for col in rating_columns_present:
             report_lines.append(f"  - {col}: {merged_df[col].notna().sum()} nicht-fehlende Werte")

    if "count_ratings" in merged_df.columns: # Bezieht sich auf die Anzahl der Original-Ratings vor Normalisierung
        report_lines.append("\nVerteilung nach Anzahl der Original-Rating-Quellen ('count_ratings'):")
        rating_counts = merged_df["count_ratings"].value_counts().sort_index()
        for count, num_movies in rating_counts.items():
            report_lines.append(f"  - Filme mit {count} Original-Rating-Quelle(n): {num_movies}")
    
    if "num_available_ratings" in merged_df.columns: # Bezieht sich auf normalisierte Ratings für Superscore
        report_lines.append("\nVerteilung nach Anzahl der verfügbaren normalisierten Ratings für Superscore ('num_available_ratings'):")
        available_counts = merged_df["num_available_ratings"].value_counts().sort_index()
        for count, num_movies in available_counts.items():
            report_lines.append(f"  - Filme mit {count} verfügbaren norm. Rating(s) für Superscore: {num_movies}")

    if "genres" in merged_df.columns:
        num_with_genres = merged_df["genres"].apply(lambda x: bool(x) if isinstance(x, list) else False).sum()
        report_lines.append(f"\nAnzahl Filme mit mindestens einem Genre: {num_with_genres} (von {len(merged_df)})")
    
    if "release_date" in merged_df.columns:
        num_with_release_date = merged_df["release_date"].notna().sum()
        report_lines.append(f"Anzahl Filme mit Release Date: {num_with_release_date} (von {len(merged_df)})")
    
    report_lines.append("\n--- Details zu allen Spalten im finalen DataFrame ---")
    report_lines.append(f"Spalten: {', '.join(merged_df.columns.tolist())}")
    for col in merged_df.columns:
        non_na_count = merged_df[col].notna().sum()
        dtype = str(merged_df[col].dtype)
        report_lines.append(f"  - Spalte '{col}' (Typ: {dtype}): {non_na_count} nicht-fehlende Werte (von {len(merged_df)})")

    if original_dfs:
        report_lines.append("\n\n--- Analyse der Ursprungsquellen (Vergleich mit gemergten Daten) ---")
        
        # Erstelle normalisierte Schlüssel für den gemergten DataFrame (title, year)
        merged_keys_set = set()
        if "title" in merged_df.columns:
            key_title_merged = merged_df["title"].astype(str).apply(normalize_film_title)
            key_year_merged = merged_df["year"].astype(str).fillna(NO_YEAR_STR_KEY) if "year" in merged_df.columns else pd.Series([NO_YEAR_STR_KEY] * len(merged_df), index=merged_df.index)
            merged_keys_set = set(zip(key_title_merged, key_year_merged))
        else:
            report_lines.append("WARNUNG: 'title' Spalte nicht im gemergten DataFrame für Detailvergleich der Quellen.")


        for source_name, source_df_orig in original_dfs.items():
            report_lines.append(f"\nQuelle: {source_name}")
            if source_df_orig is None or source_df_orig.empty:
                report_lines.append("  - Keine Daten für diese Quelle vorhanden oder DataFrame ist leer.")
                continue
            
            # source_df_orig ist das DataFrame, wie es vom Adapter kam (und gespeichert wurde)
            report_lines.append(f"  - Ursprüngliche Anzahl an Einträgen (nach Adapter-Transformation): {len(source_df_orig)}")
            
            source_keys_set = set()
            if "title" in source_df_orig.columns:
                 # Titel in source_df_orig sollte bereits normalisiert sein durch den Adapter
                source_key_title = source_df_orig["title"].astype(str) # .apply(normalize_film_title) nicht nötig wenn Adapter das schon tut
                source_key_year = source_df_orig["year"].astype(str).fillna(NO_YEAR_STR_KEY) if "year" in source_df_orig.columns else pd.Series([NO_YEAR_STR_KEY] * len(source_df_orig), index=source_df_orig.index)
                source_keys_set = set(zip(source_key_title, source_key_year))
                
                unique_entries_in_source = len(source_keys_set)
                titles_found_in_merge_count = len(source_keys_set.intersection(merged_keys_set))
                
                report_lines.append(f"  - Anzahl einzigartiger Schlüssel (normalisierter Titel, Jahr) in Quelle: {unique_entries_in_source}")
                report_lines.append(f"  - Davon im finalen Merge gefunden (basierend auf Schlüssel): {titles_found_in_merge_count}")
                if unique_entries_in_source > 0:
                    percentage_found = (titles_found_in_merge_count / unique_entries_in_source) * 100
                    report_lines.append(f"  - Anteil im Merge: {percentage_found:.2f}%")

                lost_source_keys = source_keys_set - merged_keys_set
                if lost_source_keys:
                    report_lines.append(f"  - Filme aus '{source_name}' NICHT im finalen Merge gefunden (max. 10 Beispiele):")
                    # Iteriere über das Original-DataFrame, um die nicht gefundenen Titel anzuzeigen
                    count = 0
                    for idx, row in source_df_orig.iterrows():
                        # Verwende die gleichen Schlüssel für den Check
                        check_title = row["title"] # Bereits normalisiert angenommen
                        check_year = str(row["year"]) if "year" in row and pd.notna(row["year"]) else NO_YEAR_STR_KEY
                        if (check_title, check_year) in lost_source_keys:
                            report_lines.append(f"    - '{row['title']}' (Jahr: {row.get('year', 'N/A')})") # Zeige den Titel aus der Quelldatei
                            count +=1
                            if count >=10: break                
            else:
                report_lines.append("  - Spalte 'title' nicht in dieser Quelldatei für Detailanalyse gefunden.")
    else:
        report_lines.append("\nKeine originalen Adapter-DataFrames für Detailvergleich der Quellen bereitgestellt.")

    try:
        with open(report_path, "w", encoding="utf-8") as f:
            for line in report_lines:
                f.write(line + "\n")
        logging.info(f"Merge-Analysebericht gespeichert unter: {report_path}")
    except Exception as e:
        logging.error(f"Fehler beim Speichern des Merge-Analyseberichts: {e}")


# === Funktionen inspiriert von RatingAnalyzer aus rating_analysis.py ===

def get_rating_statistics(df: pd.DataFrame, rating_cols_map: dict) -> pd.DataFrame:
    """
    Berechnet deskriptive Statistiken für angegebene Rating-Spalten.
    Args:
        df: DataFrame, das die Rating-Spalten enthält.
        rating_cols_map: Dictionary {'Spaltenname_im_df': 'Anzeigename_im_Bericht'}
    Returns:
        DataFrame mit Statistiken.
    """
    stats = {}
    for col_name, display_name in rating_cols_map.items():
        if col_name in df.columns:
            series = df[col_name].dropna()
            if not series.empty:
                stats[display_name] = {
                    'mean': series.mean(),
                    'std': series.std(),
                    'min': series.min(),
                    'max': series.max(),
                    'median': series.median(), # Hinzugefügt
                    'count': series.count()
                }
            else:
                 stats[display_name] = {k: np.nan for k in ['mean', 'std', 'min', 'max', 'median', 'count']}
        else:
            logging.warning(f"Statistik-Spalte '{col_name}' nicht im DataFrame gefunden.")
    return pd.DataFrame(stats).T.round(2)


def run_rating_scatter_plots(df: pd.DataFrame, output_dir: Path, rating_cols: list):
    """
    Erstellt paarweise Scatter-Plots für die angegebenen Rating-Spalten.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    logging.info(f"Erstelle paarweise Rating-Scatter-Plots in '{output_dir}'...")

    # Filter auf tatsächlich vorhandene Spalten
    valid_rating_cols = [col for col in rating_cols if col in df.columns]
    if len(valid_rating_cols) < 2:
        logging.warning("Nicht genügend valide Rating-Spalten für Scatter-Plots vorhanden.")
        return

    num_cols = len(valid_rating_cols)
    # Erzeuge Paare von Spalten
    pairs = []
    for i in range(num_cols):
        for j in range(i + 1, num_cols):
            pairs.append((valid_rating_cols[i], valid_rating_cols[j]))

    if not pairs:
        logging.info("Keine Paare für Scatter-Plots gefunden.")
        return

    # Dynamische Anpassung der Subplot-Größe
    # Ziel: Ca. 2-3 Plots pro Reihe
    plots_per_row = 3
    num_rows_scatter = (len(pairs) + plots_per_row - 1) // plots_per_row
    
    fig, axes = plt.subplots(num_rows_scatter, plots_per_row, figsize=(5 * plots_per_row, 5 * num_rows_scatter), squeeze=False)
    axes_flat = axes.flatten()

    for idx, (col1, col2) in enumerate(pairs):
        if idx < len(axes_flat): # Sicherstellen, dass wir nicht außerhalb der Achsen plotten
            ax = axes_flat[idx]
            if col1 in df.columns and col2 in df.columns:
                sns.scatterplot(data=df, x=col1, y=col2, ax=ax, alpha=0.3)
                ax.set_title(f'{col1} vs {col2}', fontsize=10)
                ax.tick_params(axis='x', rotation=30)
                ax.tick_params(axis='y', rotation=0)
            else:
                logging.warning(f"Eine oder beide Spalten für Scatterplot nicht gefunden: {col1}, {col2}")
                ax.text(0.5, 0.5, "Daten nicht verfügbar", ha='center', va='center')


    # Verbleibende leere Subplots entfernen
    for i in range(len(pairs), len(axes_flat)):
        fig.delaxes(axes_flat[i])

    fig.suptitle('Paarweise Scatter Plots der Rating-Quellen', fontsize=16, y=1.0) # y anpassen für Platz
    plt.tight_layout(rect=[0, 0, 1, 0.96]) # rect anpassen
    file_path = output_dir / 'rating_scatter_plots_all_pairs.png'
    plt.savefig(file_path)
    plt.close(fig)
    logging.info(f"Rating Scatter Plots gespeichert in '{file_path}'.")


# === Haupt-Analyseklasse und Ausführung ===

class ComprehensiveMovieAnalyzer:
    def __init__(self, config_path_str: str = 'config.yaml'):
        self.config_path = Path(config_path_str)
        if not self.config_path.exists():
            # Versuch, im übergeordneten Verzeichnis zu suchen, falls es sich um ein typisches Projektlayout handelt
            alt_config_path = Path(__file__).resolve().parent.parent / config_path_str
            if alt_config_path.exists():
                self.config_path = alt_config_path
            else:
                raise FileNotFoundError(f"Konfigurationsdatei nicht gefunden: {config_path_str} oder {alt_config_path}")

        with open(self.config_path, 'r') as f:
            self.cfg = yaml.safe_load(f)

        log_level_str = self.cfg.get('logging', {}).get('level', 'INFO').upper()
        logging.basicConfig(level=getattr(logging, log_level_str, logging.INFO), 
                            format='%(asctime)s - %(levelname)s - %(message)s')
        
        self.output_cfg = self.cfg.get('output', {})
        self.analysis_cfg = self.output_cfg.get('analysis', {}) # Spezifische Analyse-Output-Pfade

    def _resolve_path(self, path_str: str | Path) -> Path:
        """ Löst einen Pfad relativ zum Konfigurationsdatei-Verzeichnis auf, wenn er relativ ist. """
        path_obj = Path(path_str)
        if path_obj.is_absolute():
            return path_obj
        return (self.config_path.parent / path_obj).resolve()

    def load_data(self) -> tuple[pd.DataFrame | None, pd.DataFrame | None, dict[str, pd.DataFrame]]:
        merged_df_raw = None
        df_final_processed = None
        dfs_collection_loaded = {}

        # 1. Lade rohen Merge-DataFrame (output.csv_path aus der ETL)
        raw_merged_path_str = self.output_cfg.get("csv_path")
        if raw_merged_path_str:
            raw_merged_path = self._resolve_path(raw_merged_path_str)
            try:
                merged_df_raw = pd.read_csv(raw_merged_path)
                logging.info(f"Roher Merge-DataFrame geladen von: {raw_merged_path} ({len(merged_df_raw)} Zeilen)")
            except FileNotFoundError:
                logging.error(f"Roher Merge-DataFrame NICHT gefunden: {raw_merged_path}")
            except Exception as e:
                logging.error(f"Fehler beim Laden des rohen Merge-DataFrames von {raw_merged_path}: {e}")
        else:
            logging.warning("Kein 'output.csv_path' in Config für rohen Merge-DataFrame.")

        # 2. Lade final verarbeiteten DataFrame (final_filtered_superscore.csv)
        # Pfad wird abgeleitet vom output.csv_path oder Standard-Pfad
        final_filtered_filename = "final_filtered_superscore.csv" # Annahme aus ETL
        if raw_merged_path_str: # Basis-Pfad vom rohen Merge nehmen
            base_processed_dir = self._resolve_path(raw_merged_path_str).parent
        else: # Fallback, falls output.csv_path nicht da ist
            base_processed_dir = self._resolve_path(self.output_cfg.get("default_processed_data_path", "data/processed"))
        
        path_final_filtered = base_processed_dir / final_filtered_filename
        try:
            df_final_processed = pd.read_csv(path_final_filtered)
            logging.info(f"Final verarbeiteter DataFrame geladen von: {path_final_filtered} ({len(df_final_processed)} Zeilen)")
        except FileNotFoundError:
            logging.warning(f"Final verarbeiteter DataFrame NICHT gefunden: {path_final_filtered} (Wird für einige Analysen benötigt)")
        except Exception as e:
            logging.error(f"Fehler beim Laden des final verarbeiteten DataFrames von {path_final_filtered}: {e}")
            
        # 3. Lade einzelne Adapter-DataFrames (dfs_collection)
        intermediate_path_str = self.output_cfg.get("intermediate_adapter_data_path", "data/intermediate_adapter_outputs")
        intermediate_dir = self._resolve_path(intermediate_path_str)
        if intermediate_dir.exists() and intermediate_dir.is_dir():
            # Adapter-Namen sollten aus der Config kommen oder hier dynamischer ermittelt werden,
            # für jetzt nehmen wir die bekannten Namen an.
            adapter_names = self.cfg.get("sources", {}).keys() # Liest Adapter-Namen aus sources-Config
            if not adapter_names: # Fallback falls sources nicht in config
                adapter_names = ["ImdbAdapter", "MovielensAdapter", "MetacriticAdapter", "RottenTomatoesAdapter"]
                logging.warning(f"Keine Adapternamen in Config 'sources' gefunden, verwende Standardliste: {adapter_names}")

            for adapter_name in adapter_names:
                file_path = intermediate_dir / f"{adapter_name}.csv"
                try:
                    df_adapter = pd.read_csv(file_path)
                    dfs_collection_loaded[adapter_name] = df_adapter
                    logging.info(f"Adapter-Daten für '{adapter_name}' geladen von: {file_path}")
                except FileNotFoundError:
                    logging.warning(f"Adapter-CSV für '{adapter_name}' NICHT gefunden: {file_path}")
                except Exception as e:
                    logging.error(f"Fehler beim Laden der Adapter-Daten für '{adapter_name}' von {file_path}: {e}")
        else:
            logging.warning(f"Verzeichnis für zwischengespeicherte Adapter-Daten NICHT gefunden: {intermediate_dir} (Wird für Merge-Bericht-Detailanalyse benötigt)")
            
        return merged_df_raw, df_final_processed, dfs_collection_loaded

    def run_analyses(self):
        logging.info("Starte umfassende Filmdaten-Analyse...")
        merged_df_raw, df_final_processed, dfs_collection_loaded = self.load_data()

        if merged_df_raw is None:
            logging.critical("Kritisch: Roher Merge-DataFrame konnte nicht geladen werden. Viele Analysen können nicht durchgeführt werden.")
            return

        # Konfiguration für Analyse-Ausgabepfade
        raw_analysis_dir_str = self.analysis_cfg.get("raw_ratings_output_dir", "data/analysis/comprehensive_01_raw_merged")
        raw_analysis_output_dir = self._resolve_path(raw_analysis_dir_str)
        raw_analysis_output_dir.mkdir(parents=True, exist_ok=True)

        final_analysis_dir_str = self.analysis_cfg.get("final_ratings_output_dir", "data/analysis/comprehensive_02_final_processed")
        final_analysis_output_dir = self._resolve_path(final_analysis_dir_str)
        final_analysis_output_dir.mkdir(parents=True, exist_ok=True)
        
        report_path_str = self.analysis_cfg.get("analysis_report_path", "data/analysis/comprehensive_merge_report.txt")
        report_output_path = self._resolve_path(report_path_str)

        # --- 1. Merge-Analyse-Bericht (auf rohem Merge-DataFrame) ---
        logging.info(f"Erstelle Merge-Analyse-Bericht -> {report_output_path}")
        generate_merge_analysis_report(merged_df_raw, report_output_path, original_dfs=dfs_collection_loaded)

        # --- 2. Analysen auf dem rohen Merge-DataFrame ---
        logging.info(f"Starte Analysen auf dem rohen Merge-DataFrame (Ausgabe nach: {raw_analysis_output_dir})")
        
        # 2a. Deskriptive Statistiken der Roh-Ratings
        raw_rating_cols_map = {
            'rating_imdb': 'IMDB (Roh)', 'rating_movielens': 'MovieLens (Roh)',
            'rating_metacritic': 'Metacritic (Roh)', 'rating_rt_audience': 'RottenTomatoes (Roh)'
        }
        raw_stats_df = get_rating_statistics(merged_df_raw, raw_rating_cols_map)
        raw_stats_df.to_csv(raw_analysis_output_dir / "stats_01_raw_ratings.csv")
        logging.info(f"Statistiken der Roh-Ratings gespeichert. Inhalt:\n{raw_stats_df}")
        
        # 2b. Verteilungsplots der Roh-Ratings
        run_distribution_plots(merged_df_raw, raw_analysis_output_dir, "raw_ratings")
        
        # 2c. Korrelations-Heatmap der Roh-Ratings
        run_correlation_plots(merged_df_raw, raw_analysis_output_dir, "raw_ratings")
        
        # 2d. Scatter-Plots der Roh-Ratings
        run_rating_scatter_plots(merged_df_raw, raw_analysis_output_dir, list(raw_rating_cols_map.keys()))

        # --- 3. Analysen auf dem final verarbeiteten DataFrame (falls vorhanden) ---
        if df_final_processed is not None:
            logging.info(f"Starte Analysen auf dem final verarbeiteten DataFrame (Ausgabe nach: {final_analysis_output_dir})")
            
            # 3a. Deskriptive Statistiken der normalisierten Ratings und Superscores
            final_rating_cols_map = {
                'imdb_norm': 'IMDB (Norm 0-10)', 'movielens_norm': 'MovieLens (Norm 0-10)',
                'metacritic_norm': 'Metacritic (Norm 0-10)', 'rt_norm': 'RottenTomatoes (Norm 0-10)',
                'superscore_mean': 'Superscore Mean (0-10)', 'superscore_median': 'Superscore Median (0-10)'
            }
            final_stats_df = get_rating_statistics(df_final_processed, final_rating_cols_map)
            final_stats_df.to_csv(final_analysis_output_dir / "stats_02_final_ratings_superscores.csv")
            logging.info(f"Statistiken der finalen Ratings/Superscores gespeichert. Inhalt:\n{final_stats_df}")

            # 3b. Verteilungsplots (Normalisierte Ratings & Superscores)
            run_distribution_plots(df_final_processed, final_analysis_output_dir, "normalized_ratings")
            run_distribution_plots(df_final_processed, final_analysis_output_dir, "superscores_0_10")
            
            # 3c. Korrelations-Heatmap (Normalisierte Ratings & Superscores)
            # Beinhaltet auch den Superscore Mean vs Median Scatter Plot
            run_correlation_plots(df_final_processed, final_analysis_output_dir, "normalized_and_superscores")

            # 3d. Zusätzliche Scatter-Plots für alle normalisierten Ratings
            norm_cols_for_scatter = ['imdb_norm', 'movielens_norm', 'metacritic_norm', 'rt_norm']
            run_rating_scatter_plots(df_final_processed, final_analysis_output_dir, norm_cols_for_scatter)
        else:
            logging.warning("Final verarbeiteter DataFrame nicht geladen. Überspringe Analysen auf diesen Daten.")

        logging.info(f"Umfassende Analyse abgeschlossen. Ergebnisse in '{raw_analysis_output_dir}' und '{final_analysis_output_dir}'.")


if __name__ == '__main__':
    # Konfigurationsdatei relativ zum Skript oder Projektverzeichnis
    # Passen Sie dies ggf. an, wenn Ihre config.yaml woanders liegt.
    # Annahme: Wenn dieses Skript in kobus_testing/ liegt, dann ist config.yaml in kobus_testing/
    # oder im Projekt-Root (z.B. /Users/jakob/ba_etl/config.yaml, wenn das Skript in /Users/jakob/ba_etl/kobus_testing/ liegt)
    
    # Versuche, den Pfad zur config.yaml relativ zum aktuellen Arbeitsverzeichnis oder zum Skriptverzeichnis zu finden
    config_file = "config.yaml" 
    if not Path(config_file).exists():
        # Versuche, es im Verzeichnis dieses Skripts zu finden
        script_dir_config = Path(__file__).resolve().parent / config_file
        if script_dir_config.exists():
            config_file = script_dir_config
        else:
            # Versuche, es im Projekt-Root zu finden (ein Verzeichnis höher als das Skript-Verzeichnis)
            project_root_config = Path(__file__).resolve().parent.parent / config_file
            if project_root_config.exists():
                config_file = project_root_config
            else:
                logging.error(f"config.yaml konnte weder im aktuellen Verzeichnis, noch im Skriptverzeichnis, noch im Projekt-Root gefunden werden.")
                exit(1) # Beende, wenn Config nicht auffindbar

    analyzer = ComprehensiveMovieAnalyzer(config_path_str=str(config_file))
    analyzer.run_analyses()