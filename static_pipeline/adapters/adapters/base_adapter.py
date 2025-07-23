from abc import ABC, abstractmethod
import pandas as pd

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