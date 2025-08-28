# Static ETL-Pipeline

Diese Pipeline vereinigt mehrere öffentliche Filmdatenquellen (IMDb, Metacritic, Movielens, Rotten Tomatoes), bereinigt und normalisiert die Ratings und erzeugt einen zusammengeführten Datensatz inklusive Superscore‐Berechnung.

---

## Inhaltsverzeichnis
1. Überblick
2. Projektstruktur
3. Installation
4. Konfiguration (`config.yaml`)
5. Ausführen der Pipeline
6. Ausgabedateien & Verzeichnisse
7. Validierung & Reports
8. Logging‐Verhalten anpassen
9. Erweiterung: neuen Adapter hinzufügen

---

## 1  Überblick
Die **Static Pipeline** liest jeweils eine vorbereitete CSV‐Datei pro Datenquelle, wendet Quell‐spezifische Transformationen an (Adapter), führt die Daten zu einem Wide‐Format zusammen, normalisiert die Einzelratings und berechnet daraus einen Superscore.  
Die Zwischenergebnisse sowie umfangreiche Validierungsreports werden automatisiert abgelegt.

> Ziel: Ein konsolidierter Datensatz mit stabilen, vergleichbaren Filmratings, der in nachgelagerten Analysen oder ML-Pipelines verwendet werden kann.

---

## 2  Projektstruktur (Auszug)
```text
static_pipeline/
├── adapters/          # Quell-spezifische ETL-Adapter
│   ├── adapters/
│   │   ├── imdb_adapter.py
│   │   ├── movielens_adapter.py
│   │   ├── metacritic_adapter.py
│   │   └── rottentomatoes_adapter.py
│   └── base_adapter.py
├── transform/         # Transformationsschritte (merge, normalisieren …)
├── loaders/           # Verschiedene Load-Targets (CSV-Loader etc.)
├── utils/             # Hilfsfunktionen, z. B. `basic_validator.py`
├── data/
│   ├── raw/           # Erwartete Roh-CSV-Dateien (siehe config)
│   ├── intermediate_adapter_outputs/
│   ├── processed/
│   └── validation_reports/
├── main_pipeline.py   # Einstiegspunkt
└── config.yaml        # Zentrale Konfigurationsdatei
```

---

## 3  Installation
1. **Python ≥ 3.10** vorausgesetzt (PyEnv o. Ä. empfohlen).
2. Abhängigkeiten installieren:
   ```bash
   pip install -r requirements.txt
   ```
   (Die wichtigsten Pakete: `pandas`, `pyyaml`, `numpy`, `yapf`.)

---

## 4  Konfiguration (`config.yaml`)
```yaml
logging:
  level: WARNING            # DEBUG, INFO, WARNING, ERROR, CRITICAL

sources:
  ImdbAdapter:
    file_path: "../static_pipeline/data/raw/imdb_data.csv"
  MovielensAdapter:
    file_path: "../static_pipeline/data/raw/movielens_aggregated.csv"
  MetacriticAdapter:
    file_path: "../static_pipeline/data/raw/metacritic_movies.csv"
  RottenTomatoesAdapter:
    file_path: "../static_pipeline/data/raw/rotten_tomatoes_movies.csv"

processing:
  min_ratings_for_superscore: 2
  apply_outlier_treatment: false   # oder true + Details unt.
  outlier_treatment:
    method: "cap"                  # cap, iqr, none …
    iqr_faktor: 1.5
    lower_percentile: 0.05
    upper_percentile: 0.95

output:
  csv_path: "../static_pipeline/data/processed/final_filtered_superscore.csv"
  analysis:                        # optionale Analysepfade
    analysis_report_path: "data/analysis_reports/comprehensive_movie_merge_report.txt"
    raw_ratings_output_dir: "data/analysis_reports/01_raw_movie_data_insights"
    final_ratings_output_dir: "data/analysis_reports/02_processed_movie_data_insights"
```
> **Hinweis:** Pfadangaben können absolut oder relativ sein; relative Pfade sind immer bezogen auf den Speicherort von `main_pipeline.py`.

---

## 5  Ausführen der Pipeline
```bash
# im Projekt-Root
python3 static_pipeline/main_pipeline.py
```
Optional kannst du eine alternative Konfig-Datei angeben:
```bash
python3 static_pipeline/main_pipeline.py --config my_config.yaml  # falls Flag implementiert
```

---

## 6  Ausgabedateien & Verzeichnisse
| Pfad                                   | Inhalt |
|----------------------------------------|--------|
| `data/intermediate_adapter_outputs/*.csv` | geparste & transformierte Roh-Outputs je Adapter |
| `data/validation_reports/*_report.txt` | Textreport mit Validierungsfehlern je Datensatz |
| `data/validation_reports/*_invalid_rows.csv` | Zeilen mit ungültigem Jahr oder Rating; leer, wenn keine Probleme |
| `data/validation_reports/*_duplicates.csv` | identifizierte Duplikate (`title` + Jahr); leer, wenn keine |
| `data/processed/all_movies_wide_unfiltered.csv` | Wide-Merge ohne Filter |
| `data/processed/final_filtered_superscore.csv` | Endresultat inkl. Superscore |
| `data/duplicates/*` | Ablage entfernter Duplikate pro Adapter (Zeitstempel im Dateinamen) |

---

## 7  Validierung & Reports
`utils/basic_validator.py` prüft …
* Pflichtspalten (`title`, `year` oder `release_year`)
* Jahr im Bereich 1888 – nächstes Jahr
* Ratingspalten im datensatz-spezifischen Wertebereich (IMDb 0-10, Movielens 0-5, Metacritic/RT 0-100)
* Duplikate `(title, year)`
* optional: schreibt Report‐TXT, `*_invalid_rows.csv`, `*_duplicates.csv`

Alle Parameter können zentral in der Pipeline übergeben werden; Änderungen hierfür sind nur im Code erforderlich, nicht in der YAML.

---

## 8  Logging‐Verhalten anpassen
* **Level** wird über `config.yaml > logging.level` gesteuert.  
  `WARNING` blendet alle Info-Meldungen aus, nur Warnungen / Fehler bleiben.
* In `main_pipeline.py` wird beim Start das Root‐Logger-Level gesetzt – spätere `basicConfig`-Aufrufe überschreiben das nicht mehr.

---

## 9  Erweiterung: neuen Adapter hinzufügen
1. Neue Klasse `<NewSource>Adapter` in `static_pipeline/adapters/adapters/` anlegen; sie erbt von `BaseAdapter` und implementiert `extract()` und `transform()`.
2. In der `config.yaml` unter `sources:` einen neuen Eintrag erstellen.
3. `main_pipeline.py` → `adapter_classes_map` um die neue Klasse ergänzen.
4. Optional: Validierungsregeln in `basic_validator.py` erweitern (z. B. eigener Rating-Bereich).

---

© 2025 – Bachelorarbeit ETL Pipeline. Bei Fragen: <deine Mail / GitHub> 