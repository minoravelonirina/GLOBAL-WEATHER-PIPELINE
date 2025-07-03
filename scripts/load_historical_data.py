import pandas as pd
from datetime import datetime
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional

def load_historical_data(historical_path: Path, cities_normalized: List[str]) -> List[pd.DataFrame]:
    """Charge et valide les données historiques."""
    historical_data = []
    
    if not historical_path.exists():
        logging.error(f"Répertoire historique introuvable : {historical_path}")
        return historical_data

    for city_dir in historical_path.iterdir():
        if not city_dir.is_dir():
            continue
            
        city_name = city_dir.name.lower()
        if city_name not in cities_normalized:
            continue
            
        logging.info(f"Traitement des données historiques pour : {city_name}")
        
        for year_file in city_dir.glob("*.csv"):
            try:
                logging.info(f"Lecture du fichier : {year_file.name}")
                df = pd.read_csv(year_file)
                
                # Validation et normalisation
                normalized_df = validate_and_normalize_data(df, year_file.name)
                if normalized_df is not None:
                    historical_data.append(normalized_df)
                    logging.info(f"Données chargées : {year_file.name} - {len(normalized_df)} lignes")
                else:
                    logging.warning(f"Fichier ignoré : {year_file.name}")
                    
            except Exception as e:
                logging.error(f"Erreur de lecture {year_file}: {str(e)}", exc_info=True)
    
    return historical_data