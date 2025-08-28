from pathlib import Path
import pandas as pd
import yaml

# Pfad zur zentralen Pipeline-Config ermitteln (eine Ebene über utils)
CONFIG_PATH = Path(__file__).resolve().parent.parent / "config.yaml"

try:
    _cfg = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
except FileNotFoundError:
    _cfg = {}

# Verzeichnisse aus Config oder Fallback
_AUX_DIRS = _cfg.get("aux_output_dirs", {
    "invalid": "static_pipeline/data/intermediate_adapter_outputs/invalid",
    "duplicates": "static_pipeline/data/intermediate_adapter_outputs/duplicates",
})


def _get_target_dir(kind: str) -> Path:
    """Liefert das Zielverzeichnis für eine CSV-Art (invalid/duplicates)."""
    return Path(_AUX_DIRS.get(kind, f"static_pipeline/data/intermediate_adapter_outputs/{kind}"))


def save_aux_csv(kind: str, adapter_name: str, df: pd.DataFrame) -> None:
    """Speichert DataFrame unter <dir>/<adapter_name>_<kind>.csv."""
    target_dir = _get_target_dir(kind)
    target_dir.mkdir(parents=True, exist_ok=True)
    out_path = target_dir / f"{adapter_name}_{kind}.csv"
    df.to_csv(out_path, index=False) 