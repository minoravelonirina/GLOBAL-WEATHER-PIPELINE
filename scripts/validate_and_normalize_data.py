import pandas as pd
from datetime import datetime
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional

def validate_and_normalize_data(df: pd.DataFrame, filename: str) -> Optional[pd.DataFrame]:
    """Valide et normalise la structure des données."""
    try:
        # Normalisation des noms de colonnes
        column_mapping = {
            'city': 'city', 
            'lat': 'lat',
            'lon': 'lon',
            'month': 'month',
            'year': 'year',
            'temp': 'temp',
            'humidity': 'humidity'
        }
        
        df.columns = [column_mapping.get(col.lower(), col) for col in df.columns]
        
        # Vérification des colonnes requises
        required_columns = ['city', 'temp', 'month', 'year']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logging.warning(f"Fichier {filename} - Colonnes manquantes : {missing_columns}")
            return None
            
        # Conversion des types de données
        df['year'] = pd.to_numeric(df['year'], errors='coerce')
        df['month'] = pd.to_numeric(df['month'], errors='coerce')
        df['city'] = df['city'].astype(str).str.strip().str.lower()
        df['temp'] = pd.to_numeric(df['temp'], errors='coerce')
        
        # Nettoyage des données
        df.dropna(subset=['temp', 'month', 'year'], inplace=True)
        
        return df
        
    except Exception as e:
        logging.error(f"Erreur de normalisation pour {filename}: {str(e)}")
        return None
    