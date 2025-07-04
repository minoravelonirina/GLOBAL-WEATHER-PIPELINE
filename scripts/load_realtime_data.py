import pandas as pd
from datetime import datetime
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
# from validate_and_normalize_data import validate_and_normalize_data
from global_weather_pipeline.scripts.validate_and_normalize_data import validate_and_normalize_data


def load_realtime_data(realtime_path: Path) -> List[pd.DataFrame]:
    """Charge et valide les données en temps réel."""
    realtime_data = []
    
    if not realtime_path.exists():
        logging.error(f"Répertoire temps réel introuvable : {realtime_path}")
        return realtime_data

    for file in realtime_path.glob("*.csv"):
        try:
            df = pd.read_csv(file)
            normalized_df = validate_and_normalize_data(df, file.name)
            
            if normalized_df is not None:
                realtime_data.append(normalized_df)
                logging.info(f"Données temps réel chargées : {file.name}")
            else:
                logging.warning(f"Fichier temps réel ignoré : {file.name}")
                
        except Exception as e:
            logging.error(f"Erreur de lecture {file}: {str(e)}")
    
    return realtime_data
