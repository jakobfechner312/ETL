# Anhang

## A.1 Kernartefakte (Überblick)

| Kategorie | Relativer Pfad (Beispiel) | Pflicht |
|---|---|---|
| Zielschema | `adaptive/schema.json` | ✅ |
| Umgebung | `static_pipeline/requirements.txt` | ✅ |
| Transform-Prompt | `adaptive/run_artifacts/<quelle>_<RUN_ID>/transform_prompt.txt` | ✅ |
| Transform-Code | `adaptive/run_artifacts/<quelle>_<RUN_ID>/transform_code_<hash>.py` | ✅ |
| Validator-Prompt | `adaptive/run_artifacts/<quelle>_<RUN_ID>/validator_prompt.txt` | ✅ |
| Validator-Code | `adaptive/run_artifacts/<quelle>_<RUN_ID>/validator_code_<hash>.py` | ✅ |
| Protokoll | `adaptive/run_artifacts/<quelle>_<RUN_ID>/run.log` | ☐ |
| Manifest | `adaptive/run_artifacts/<quelle>_<RUN_ID>/manifest.json` | ✅ |
| Bereinigte Outputs | `adaptive/cleaned/<quelle>.csv` | ✅ |
| Enddatensatz | `static_pipeline/data/processed/final_filtered_superscore.csv` | ✅ |
| Prüfsummen | `repro_snapshot_<YYYYMMDDTHHMMSSZ>/checksums.csv` | ✅ |

## A.2 Verwendete Läufe (RUN_ID je Quelle)

| Quelle | RUN_ID | Zeit | transform_code_ | validator_code_ |
|---|---|---|---|---|
| imdb | 20250829_103722 | 2025-08-29 10:37 | e79fbd4 | 5c7c03a |
| movielens | 20250829_104624 | 2025-08-29 10:46 | 03d8382 | b1dd8ee |
| metacritic | 20250829_104211 | 2025-08-29 10:42 | 6dd798b | 1c9d5f4 |
| rotten_tomatoes | 20250829_104738 | 2025-08-29 10:47 | 45afcd1 | 8ae2007 |
| my_new_source | 20250829_102535 | 2025-08-29 10:25 | 38d3981 | f7c22f4 |

Zu den Ordnern:
- `adaptive/run_artifacts/imdb_data_20250829_103722/`
- `adaptive/run_artifacts/movielens_aggregated_20250829_104624/`
- `adaptive/run_artifacts/metacritic_movies_20250829_104211/`
- `adaptive/run_artifacts/rotten_tomatoes_movies_20250829_104738/`
- `adaptive/run_artifacts/my_new_source_20250829_102535/`

## A.3 System und Konfiguration (Kurzform)

- Python: 3.10
- OS: macOS (Darwin 24.5.0)
- Fixierter Seed: `RNG_STATE = 42`
- Wichtige Konfig (static pipeline):
  - `processing.min_ratings_for_superscore = 2`
  - `processing.apply_outlier_treatment = false`
  - `outlier_treatment.method = "cap"`
  - `outlier_treatment.iqr_faktor = 1.5`
  - `outlier_treatment.lower_percentile = 0.05`
  - `outlier_treatment.upper_percentile = 0.95`
  - `output.csv_path = '../static_pipeline/data/processed/test_merge_result.csv'`
  - Enddatei i. d. R.: `static_pipeline/data/processed/final_filtered_superscore.csv`

## A.4 Snapshot-Nachweis

Der Ordner `repro_snapshot_20250830T234105Z/` enthält:
- `TREE.txt` (Verzeichnisbaum)
- `checksums.csv` (SHA-256 für alle relevanten Dateien)

Release/Commit: Commit `bf57b61` auf Branch `main`. 