import requests
import pandas as pd
import numpy as np
from datetime import datetime
import logging
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests
from pathlib import Path

def extract_historical_data(cities: list, years: int = 4) -> None:
    project_dir = Path(__file__).parent.parent
    input_path = project_dir/"data"/"GlobalWeatherRepository.csv"
    output_dir = project_dir/"data"/"historique"
    
    cols = [
        'location_name',
        'last_updated',
        'temperature_celsius',
        'humidity',
        'pressure_mb'
    ]

    # Chargement avec vérification
    df = pd.read_csv(input_path, parse_dates=['last_updated'], usecols=cols)
    
    # Extraction du nom de ville (version robuste)
    # df['city'] = df['timezone'].str.extract(r'/([^/]+)$')[0].str.replace('_', ' ')
    df = df.rename(columns={'location_name': 'city'})
    df = df.rename(columns={'last_updated' : 'timestamp'} )
    # Filtrage correct
    max_year = df['timestamp'].dt.year.max()
    min_year = max_year - years
    
    # Vérification que cities est une liste non vide
    if not isinstance(cities, list) or len(cities) == 0:
        raise ValueError("L'argument cities doit être une liste non vide")
    
    # Filtrage en deux étapes
    filtered_df = df[df['city'].isin(cities)].copy()  # D'abord par villes
    filtered_df = filtered_df[filtered_df['timestamp'].dt.year >= min_year]  # Puis par années
    
    # Sauvegarde
    output_dir.mkdir(exist_ok=True, parents=True)
    
    for ville in cities:
        ville_data = filtered_df[filtered_df['city'] == ville]
        if not ville_data.empty:
            ville_dir = output_dir / ville.replace(" ", "_")
            ville_dir.mkdir(exist_ok=True)
            
            for year, year_data in ville_data.groupby(ville_data['timestamp'].dt.year):
                year_data.to_csv(ville_dir/f"{year}.csv", index=False)