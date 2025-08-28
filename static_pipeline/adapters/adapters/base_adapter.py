from abc import ABC, abstractmethod
import pandas as pd
from utils.save_aux_csv import save_aux_csv

class BaseAdapter(ABC):
    def __init__(self, source_config: dict):
        self.config = source_config

    @abstractmethod
    def extract(self) -> any:
        """Lädt Rohdaten (ein DataFrame oder Roh-Objekte)"""
        pass

    @abstractmethod
    def transform(self, data: any) -> pd.DataFrame:
        """Bereinigt und formatiert die Quelldaten zu einem DataFrame"""
        pass
    
    def _log_aux_files(
        self,
        adapter_name: str,
        invalid_rows: list[dict],
        duplicate_rows: list[dict],
    ) -> None:
        if invalid_rows:
            save_aux_csv("invalid", adapter_name, pd.DataFrame(invalid_rows))
        if duplicate_rows:
            save_aux_csv("duplicates", adapter_name, pd.DataFrame(duplicate_rows))