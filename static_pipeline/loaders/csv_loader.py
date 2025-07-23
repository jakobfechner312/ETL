import pandas as pd

class CsvLoader:
    def __init__(self, path: str):
        self.path = path

    def load(self, df: pd.DataFrame):
        df.to_csv(self.path, index=False)
        print(f"✅ Merge + Genre‐Auswahl abgeschlossen. Datei unter: {self.path}")