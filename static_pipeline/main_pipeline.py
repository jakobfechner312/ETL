import yaml
import logging
from pathlib import Path
import pandas as pd
from datetime import datetime

# Adapter-Importe
from adapters.adapters.imdb_adapter import ImdbAdapter
from adapters.adapters.movielens_adapter import MovielensAdapter
from adapters.adapters.metacritic_adapter import MetacriticAdapter
from adapters.adapters.rottentomatoes_adapter import RottenTomatoesAdapter

# Transformations-Importe
from transform.merge import merge_sources
from transform.normalize_ratings import calculate_normalized_ratings_and_superscores

# Loader-Importe
from loaders.csv_loader import CsvLoader
from utils.basic_validator import validate_dataframe


class ETLPipeline:
    """
    Orchestriert den gesamten ETL-Prozess von der Datenextraktion über die
    Transformation bis hin zum Laden der verarbeiteten Daten.
    """

    def __init__(self, config_filename: str = 'config.yaml'):
        """
        Initialisiert die ETL-Pipeline.

        Liest die Konfigurationsdatei ein und initialisiert das Logging.

        Args:
            config_filename: Der Dateiname der YAML-Konfigurationsdatei,
                             relativ zum Speicherort dieses Skripts.

        Raises:
            FileNotFoundError: Wenn die Konfigurationsdatei nicht gefunden wird.
            yaml.YAMLError: Wenn die Konfigurationsdatei nicht gültig ist.
        """
        self.script_dir: Path = Path(__file__).resolve().parent
        config_path: Path = self.script_dir / config_filename

        if not config_path.exists():
            # Loggen bevor eine Exception geworfen wird, kann manchmal hilfreich sein,
            # hier ist es aber durch FileNotFoundError schon recht klar.
            raise FileNotFoundError(
                f"Konfigurationsdatei nicht gefunden: {config_path}")

        try:
            with open(
                    config_path, 'r',
                    encoding='utf-8') as f:  # encoding='utf-8' ist gute Praxis
                self.config: dict = yaml.safe_load(f)
        except yaml.YAMLError as e:
            # Loggen des Fehlers beim Laden der YAML
            logging.error(
                f"Fehler beim Parsen der Konfigurationsdatei {config_path}: {e}"
            )
            raise  # Die Exception weiterwerfen, da die Pipeline ohne Config nicht arbeiten kann

        if self.config is None:  # yaml.safe_load kann None zurückgeben bei leerer Datei
            self.config = {}
            logging.warning(
                f"Konfigurationsdatei {config_path} ist leer oder enthält keine gültige YAML-Struktur."
            )

        log_config: dict = self.config.get('logging', {})
        level_name = log_config.get('level', 'INFO').upper()
        level_value = getattr(logging, level_name, logging.INFO)
        logging.basicConfig(
            level=
            level_value,  # Sicherstellen, dass wir den numerischen Wert übergeben
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S')
        logging.getLogger().setLevel(level_value)  # Root-Logger explizit setzen
        self.logger = logging.getLogger(__name__)  # Logger für diese Klasse
        # Verzeichnis für Validierungsreports vorbereiten
        self.validation_reports_dir: Path = self._resolve_path(
            "data/validation_reports")
        self.validation_reports_dir.mkdir(parents=True, exist_ok=True)

    def _resolve_path(self, path_value: str | Path) -> Path:
        """
        Konvertiert einen Pfadwert aus der Konfiguration in ein absolutes Path-Objekt.

        Wenn der Pfad bereits absolut ist, wird er direkt zurückgegeben.
        Andernfalls wird er relativ zum Verzeichnis dieses Skripts aufgelöst.

        Args:
            path_value: Der Pfadwert (String oder Path-Objekt).

        Returns:
            Ein absolutes Path-Objekt.

        Raises:
            ValueError: Wenn der path_value weder ein String noch ein Path-Objekt ist.
        """
        if isinstance(path_value, Path):
            path_obj = path_value
        elif isinstance(path_value, str):
            path_obj = Path(path_value)
        else:
            # Loggen des Fehlers
            self.logger.error(
                f"Ungültiger Pfadwert in Config: {path_value} (Typ: {type(path_value)})"
            )
            raise ValueError(
                f"Pfadwert muss ein String oder Path-Objekt sein: {path_value}")

        if path_obj.is_absolute():
            return path_obj
        return (self.script_dir / path_obj).resolve()

    def _extract_and_transform_sources(self) -> dict[str, pd.DataFrame]:
        dfs_collection: dict[str, pd.DataFrame] = {}
        adapter_classes_map: dict[str, type[ImdbAdapter | MovielensAdapter |
                                            MetacriticAdapter |
                                            RottenTomatoesAdapter]] = {
                                                "ImdbAdapter":
                                                    ImdbAdapter,
                                                "MovielensAdapter":
                                                    MovielensAdapter,
                                                "MetacriticAdapter":
                                                    MetacriticAdapter,
                                                "RottenTomatoesAdapter":
                                                    RottenTomatoesAdapter
                                            }

        sources_config = self.config.get("sources", {})
        if not sources_config:
            self.logger.warning(
                "Keine Datenquellen in der Konfiguration definiert.")
            return dfs_collection

        for adapter_name, adapter_config_raw in sources_config.items():
            adapter_class = adapter_classes_map.get(adapter_name)
            if not adapter_class:
                self.logger.warning(
                    f"Keine Adapterklasse für '{adapter_name}' gefunden. Überspringe."
                )
                continue

            processed_adapter_config = {
                key: (
                    self._resolve_path(value) if isinstance(value,
                                                            (str, Path)) and
                    (key.endswith("_path") or key.endswith("_paths")) else value
                ) for key, value in adapter_config_raw.items()
            }
            try:
                adapter_instance = adapter_class(processed_adapter_config)
                raw_data = adapter_instance.extract()
                df_ready = adapter_instance.transform(raw_data)

                # --- Grundvalidierung des Adapter-DataFrames ---
                report_path = self.validation_reports_dir / f"{adapter_name}_report.txt"
                invalid_path = self.validation_reports_dir / f"{adapter_name}_invalid_rows.csv"
                # Duplikate lässt der Validator NICHT mehr speichern; wir handhaben sie konsolidiert unten
                ok_adapter, errs_adapter = validate_dataframe(
                    df_ready,
                    df_name=f"{adapter_name}-DF",
                    error_report_path=str(report_path),
                    save_invalid_rows=True,
                    invalid_rows_output_path=str(invalid_path),
                    save_duplicates=False,
                )
                if not ok_adapter:
                    self.logger.warning(
                        f"Validation-Probleme im {adapter_name}: {errs_adapter}"
                    )

                # --- Duplikatlogik: nur EINE Zeile pro (title, year) behalten ---
                if "title" in df_ready.columns and "year" in df_ready.columns:
                    dupes_mask_to_remove = df_ready.duplicated(
                        subset=["title", "year"], keep='first')
                    if dupes_mask_to_remove.any():
                        n_removed = int(dupes_mask_to_remove.sum())
                        self.logger.warning(
                            f"{adapter_name}: {n_removed} Duplikate (title + year) entfernt.")
                        duplicates_dir = self._resolve_path("data/validation_reports/duplicates")
                        duplicates_dir.mkdir(parents=True, exist_ok=True)
                        dupes_path = duplicates_dir / f"{adapter_name}_duplicates_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                        try:
                            df_ready.loc[dupes_mask_to_remove].to_csv(dupes_path, index=False)
                            self.logger.info(
                                f"Duplikate gespeichert unter: {dupes_path}")
                        except Exception as e:
                            self.logger.error(
                                f"Fehler beim Speichern der Duplikate für {adapter_name}: {e}"
                            )
                    df_ready = df_ready[~dupes_mask_to_remove].copy()

                if df_ready is not None and not df_ready.empty:
                    dfs_collection[adapter_name] = df_ready
                    self.logger.info(
                        f"Adapter '{adapter_name}' erfolgreich ausgeführt und Daten geladen."
                    )
                else:
                    self.logger.info(
                        f"Adapter '{adapter_name}' lieferte keine Daten oder ein leeres DataFrame."
                    )
            except Exception as e:
                self.logger.error(
                    f"Fehler beim Ausführen des Adapters '{adapter_name}': {e}",
                    exc_info=True)
                continue

        return dfs_collection

    def _save_intermediate_dfs(self,
                               dfs_collection: dict[str, pd.DataFrame]) -> None:
        """
        Speichert die DataFrames der einzelnen Adapter als CSV-Dateien
        in einem konfigurierten Zwischenverzeichnis.

        Args:
            dfs_collection: Ein Dictionary mit Adapternamen als Schlüssel und
                            den zu speichernden DataFrames als Werte.
        """
        if not dfs_collection:
            self.logger.info(
                "Keine Adapter-DataFrames zum Speichern vorhanden.")
            return

        # Optionales Feature-Flag aus der Config
        save_intermediate = self.config.get("output", {}).get("save_intermediate", True)
        if not save_intermediate:
            self.logger.info("Speichern der Einzel-Adapter-DFs deaktiviert (output.save_intermediate=false).")
            return

        intermediate_output_dir_str = self.config.get("output", {}).get(
            "intermediate_adapter_data_path",
            "data/intermediate_adapter_outputs")
        intermediate_output_dir = self._resolve_path(
            intermediate_output_dir_str)

        try:
            intermediate_output_dir.mkdir(parents=True, exist_ok=True)
            self.logger.info(
                f"Speichere einzelne Adapter-DataFrames nach: {intermediate_output_dir}")
        except OSError as e:
            self.logger.error(
                f"Fehler beim Erstellen des Verzeichnisses {intermediate_output_dir}: {e}",
                exc_info=True)
            # Wenn das Verzeichnis nicht erstellt werden kann, ist ein Speichern nicht möglich.
            return

        for name, df_adapter in dfs_collection.items():
            # Prüfen, ob df_adapter ein DataFrame ist und nicht leer
            if isinstance(df_adapter, pd.DataFrame) and not df_adapter.empty:
                try:
                    file_path = intermediate_output_dir / f"{name}.csv"
                    df_adapter.to_csv(file_path, index=False)
                    self.logger.info(
                        f"  -> Adapter-Daten für '{name}' gespeichert: {file_path}")
                except Exception as e:
                    self.logger.error(
                        f"Fehler beim Speichern des Adapter-DataFrames '{name}' nach {file_path}: {e}",
                        exc_info=True)
            else:
                self.logger.info(
                    f"Adapter-Daten für '{name}' sind leer – überspringe Speichern.")

    def _merge_and_save_raw(
            self, dfs_list: list[pd.DataFrame]) -> pd.DataFrame | None:
        """
        Führt eine Liste von DataFrames zusammen und speichert das rohe,
        ungesäuberte Ergebnis als CSV-Datei.

        Args:
            dfs_list: Eine Liste von Pandas DataFrames, die zusammengeführt werden sollen.

        Returns:
            Das zusammengeführte DataFrame oder None, wenn ein Fehler auftritt
            oder die Eingabeliste leer ist oder der Merge ein leeres DataFrame ergibt.
        """
        if not dfs_list:
            self.logger.warning("Keine DataFrames zum Mergen vorhanden.")
            return None

        self.logger.info("Starte Merge-Prozess...")
        try:
            merged_df_raw = merge_sources(dfs_list)

            for col in merged_df_raw.filter(regex=r"^rating_").columns:
                merged_df_raw[col] = (
                    pd.to_numeric(merged_df_raw[col], errors="coerce")  # Strings → NaN
                      .astype("Float64")                                # nullable float
                )

            # --- Validierung des gemergeten DataFrames ---
            report_path = self.validation_reports_dir / "Merged-DF_report.txt"
            invalid_path = self.validation_reports_dir / "Merged-DF_invalid_rows.csv"
            ok_merge, errs_merge = validate_dataframe(
                merged_df_raw,
                df_name="Merged-DF",
                error_report_path=str(report_path),
                save_invalid_rows=True,
                invalid_rows_output_path=str(invalid_path))
            if not ok_merge:
                self.logger.warning(
                    f"Validation-Probleme im Merged-DF: {errs_merge}")
            if merged_df_raw.empty:
                self.logger.warning("Merge-Prozess ergab ein leeres DataFrame.")
                # Je nach Anforderung könnte hier auch None zurückgegeben oder anders behandelt werden.
                return merged_df_raw  # Gibt leeres DataFrame zurück
            self.logger.info(
                f"Merge abgeschlossen. {len(merged_df_raw)} Einträge im rohen gemergten DataFrame.")
        except Exception as e:
            self.logger.error(f"Fehler während des Merge-Prozesses: {e}",
                              exc_info=True)
            return None

        # Speichern des rohen Merge-Ergebnisses
        raw_merge_output_path_str = self.config.get("output",
                                                    {}).get("csv_path")
        if raw_merge_output_path_str:
            raw_merge_output_path = self._resolve_path(
                raw_merge_output_path_str)
            try:
                raw_merge_output_path.parent.mkdir(parents=True, exist_ok=True)
                # Annahme: CsvLoader erwartet den Pfad als erstes positionsbasiertes Argument
                loader_raw = CsvLoader(raw_merge_output_path)
                loader_raw.load(merged_df_raw)
                self.logger.info(
                    f"Roher Merge-DataFrame gespeichert unter: {raw_merge_output_path}")
            except OSError as e:
                self.logger.error(
                    f"Fehler beim Erstellen des Verzeichnisses für rohen Merge-Output {raw_merge_output_path.parent}: {e}",
                    exc_info=True)
            except Exception as e:  # Fängt andere Fehler beim Laden/Speichern ab
                self.logger.error(
                    f"Fehler beim Speichern des rohen Merge-DataFrames nach {raw_merge_output_path}: {e}",
                    exc_info=True)
        else:
            self.logger.warning(
                "Kein 'output.csv_path' in der Konfiguration gefunden. "
                "Der rohe Merge-DataFrame wird nicht gespeichert.")
        return merged_df_raw

    def _process_and_save_final(self, merged_df: pd.DataFrame) -> None:
        """
        Normalisiert Ratings, berechnet Superscores basierend auf der Konfiguration
        und speichert das finale, gefilterte Ergebnis als CSV-Datei.

        Args:
            merged_df: Das zusammengeführte DataFrame, das verarbeitet werden soll.
                       Sollte nicht None oder leer sein.
        """
        if merged_df is None or merged_df.empty:
            self.logger.warning(
                "Kein zusammengeführtes DataFrame zum Verarbeiten vorhanden. Überspringe Prozessierung."
            )
            return

        self.logger.info(
            "Starte Rating-Normalisierung und Superscore-Berechnung...")
        processing_cfg = self.config.get("processing", {})
        min_ratings_cfg = processing_cfg.get("min_ratings_for_superscore", 2)

        # Lese den globalen Schalter für Ausreißerbehandlung aus der Config.
        # Standard ist True, falls der Key nicht existiert.
        apply_outlier_treatment = processing_cfg.get("apply_outlier_treatment",
                                                     True)

        # Initialisiere ein Dictionary für die optionalen Argumente der Normalisierungsfunktion
        kwargs_for_normalize = {}

        if apply_outlier_treatment:
            self.logger.info("Ausreißerbehandlung wird angewendet.")
            outlier_cfg_from_yaml = processing_cfg.get("outlier_treatment", {})

            # Übertrage Konfigurationswerte, wenn sie in YAML vorhanden sind.
            # Andernfalls werden die Standardwerte der Funktion 'calculate_normalized_ratings_and_superscores' verwendet.
            if "method" in outlier_cfg_from_yaml:
                kwargs_for_normalize[
                    "outlier_treatment_method"] = outlier_cfg_from_yaml[
                        "method"]
            if "iqr_faktor" in outlier_cfg_from_yaml:  # YAML verwendet 'iqr_faktor'
                kwargs_for_normalize[
                    "outlier_iqr_factor"] = outlier_cfg_from_yaml[
                        "iqr_faktor"]  # Funktion erwartet 'outlier_iqr_factor'
            if "lower_percentile" in outlier_cfg_from_yaml:
                kwargs_for_normalize[
                    "outlier_lower_percentile"] = outlier_cfg_from_yaml[
                        "lower_percentile"]
            if "upper_percentile" in outlier_cfg_from_yaml:
                kwargs_for_normalize[
                    "outlier_upper_percentile"] = outlier_cfg_from_yaml[
                        "upper_percentile"]
        else:
            self.logger.info(
                "Ausreißerbehandlung wird übersprungen (apply_outlier_treatment ist false)."
            )
            kwargs_for_normalize["outlier_treatment_method"] = "none"
            # Andere Ausreißerparameter sind irrelevant, wenn die Methode 'none' ist.

        try:
            df_final_processed = calculate_normalized_ratings_and_superscores(
                merged_df,
                min_ratings_for_superscore=min_ratings_cfg,
                **kwargs_for_normalize  # Entpackt die gesammelten Argumente
            )

            # IDs aus dem Merge sicherstellen und nach vorne ziehen
            id_cols_from_merge = [c for c in merged_df.columns if str(c).startswith("ID_")]
            for c in id_cols_from_merge:
                if c not in df_final_processed.columns:
                    df_final_processed[c] = merged_df[c]
            # Reordering: IDs nach vorne
            id_cols_present = [c for c in id_cols_from_merge if c in df_final_processed.columns]
            reordered_cols = id_cols_present + [c for c in df_final_processed.columns if c not in id_cols_present]
            df_final_processed = df_final_processed[reordered_cols]

            # --- Validierung der final verarbeiteten Daten ---
            report_path = self.validation_reports_dir / "Final-Processed-DF_report.txt"
            invalid_path = self.validation_reports_dir / "Final-Processed-DF_invalid_rows.csv"
            ok_final, errs_final = validate_dataframe(
                df_final_processed,
                df_name="Final-Processed-DF",
                error_report_path=str(report_path),
                save_invalid_rows=True,
                invalid_rows_output_path=str(invalid_path))
            if not ok_final:
                self.logger.warning(
                    f"Validation-Probleme im final verarbeiteten DF: {errs_final}")
        except Exception as e:
            self.logger.error(
                f"Fehler bei der Rating-Normalisierung und Superscore-Berechnung: {e}",
                exc_info=True)
            return  # Beende diese Methode, wenn die Prozessierung fehlschlägt

        # Speichern des finalen, gefilterten Ergebnisses
        output_cfg = self.config.get("output", {})
        # Basispfad für Output aus config.csv_path ableiten oder Fallback
        base_output_dir_str = output_cfg.get("csv_path")
        if base_output_dir_str:
            base_output_dir = self._resolve_path(base_output_dir_str).parent
        else:
            base_output_dir = self._resolve_path(
                "data/processed/")  # Fallback-Pfad
            self.logger.warning(
                "output.csv_path nicht in Config für Speicherort der gefilterten Superscores. "
                f"Verwende Fallback: {base_output_dir}")

        final_filtered_filename = output_cfg.get(
            "final_filtered_filename",
            "final_filtered_superscore.csv"  # Standard-Dateiname
        )
        path_only_movies_with_superscores = base_output_dir / final_filtered_filename

        # Filtere den DataFrame explizit, BEVOR er gespeichert wird.
        # Stelle sicher, dass die Spalte 'num_available_ratings' existiert.
        if 'num_available_ratings' in df_final_processed.columns:
            df_actually_filtered_for_saving = df_final_processed[
                df_final_processed['num_available_ratings'] >=
                min_ratings_cfg].copy(
                )  # .copy() um SettingWithCopyWarning zu vermeiden
        else:
            self.logger.warning(
                "Spalte 'num_available_ratings' nicht im DataFrame nach Prozessierung gefunden. "
                "Kann nicht für finales Speichern filtern. Speichere ungefilterte prozessierte Daten, falls vorhanden."
            )
            # Speichere das un-gefilterte, aber prozessierte DataFrame, wenn die Spalte fehlt,
            # oder ein leeres DataFrame, wenn df_final_processed auch leer ist.
            df_actually_filtered_for_saving = df_final_processed.copy(
            ) if df_final_processed is not None else pd.DataFrame()

        if not df_actually_filtered_for_saving.empty:
            # IDs auch im gefilterten Output garantieren und vorne anordnen
            id_cols_present_filtered = [c for c in id_cols_from_merge if c in df_actually_filtered_for_saving.columns]
            reordered_cols_filtered = id_cols_present_filtered + [c for c in df_actually_filtered_for_saving.columns if c not in id_cols_present_filtered]
            df_actually_filtered_for_saving = df_actually_filtered_for_saving[reordered_cols_filtered]
            try:
                path_only_movies_with_superscores.parent.mkdir(parents=True,
                                                               exist_ok=True)
                # Annahme: CsvLoader erwartet den Pfad als erstes positionsbasiertes Argument
                loader_filtered_final = CsvLoader(
                    path_only_movies_with_superscores)
                loader_filtered_final.load(df_actually_filtered_for_saving)
                self.logger.info(
                    f"Finaler, gefilterter DataFrame ({len(df_actually_filtered_for_saving)} Einträge) gespeichert "
                    f"unter: {path_only_movies_with_superscores}")
            except OSError as e:
                self.logger.error(
                    f"Fehler beim Erstellen des Verzeichnisses für finalen Output {path_only_movies_with_superscores.parent}: {e}",
                    exc_info=True)
            except Exception as e:
                self.logger.error(
                    f"Fehler beim Speichern des finalen gefilterten DataFrames nach {path_only_movies_with_superscores}: {e}",
                    exc_info=True)
        else:
            self.logger.info(
                f"Keine Daten zum Speichern nach Filterung für {path_only_movies_with_superscores}. "
                f"Der DataFrame df_actually_filtered_for_saving ist leer.")

    def run(self) -> None:
        """Führt die gesamte ETL-Pipeline aus."""
        self.logger.info("Starte ETL-Pipeline...")

        dfs_collection = self._extract_and_transform_sources()
        if not dfs_collection:  # Prüft, ob das Dictionary leer ist
            self.logger.error(
                "Keine Daten von Adaptern geladen. Pipeline wird beendet.")
            return

        self._save_intermediate_dfs(dfs_collection)

        # Stelle sicher, dass dfs_collection Werte enthält, bevor list() darauf angewendet wird
        dfs_values = list(dfs_collection.values()) if dfs_collection else []
        merged_df = self._merge_and_save_raw(dfs_values)

        if merged_df is None or merged_df.empty:  # Explizite Prüfung auf None und leer
            self.logger.error(
                "Merge-Prozess lieferte keine Daten oder schlug fehl. Pipeline wird beendet.")
            return

        self._process_and_save_final(merged_df)

        self.logger.info(
            "ETL-Prozess abgeschlossen. Verarbeitete Daten wurden gespeichert.")


if __name__ == '__main__':
    # Initialisiert und startet die Pipeline
    pipeline = ETLPipeline(config_filename='config.yaml')
    pipeline.run()
