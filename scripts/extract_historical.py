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


def extract_historical_data(cities: list, years: int) -> None:
    
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

    # Chargement des données avec vérification
    df = pd.read_csv(input_path, parse_dates=['last_updated'], usecols=cols)
    
    # Renommage des colonnes
    df = df.rename(
        columns={
            'location_name': 'city', 
            'last_updated': 'timestamp',
            'temperature_celsius': 'temp',
            'pressure_mb': 'pressure'
            }
        )
    
    # Calcul des années de filtrage
    now = pd.Timestamp.now()
    max_year = now.year - 1
    min_year = max_year - years + 1
    
    # Vérification que cities est une liste non vide
    if not isinstance(cities, list) or len(cities) == 0:
        raise ValueError("L'argument cities doit être une liste non vide")
    
    # Filtrage combiné par villes et années
    filtered_df = df[
        (df['city'].isin(cities)) & 
        (df['timestamp'].dt.year >= min_year) &
        (df['timestamp'].dt.year <= max_year)
    ].copy()
    
    # Sauvegarde
    output_dir.mkdir(exist_ok=True, parents=True)
    
    for ville in cities:
        ville_data = filtered_df[filtered_df['city'] == ville]
        
        if not ville_data.empty:
            ville_dir = output_dir / ville.replace(" ", "_")
            ville_dir.mkdir(exist_ok=True)
            
            for year, year_data in ville_data.groupby(ville_data['timestamp'].dt.year):
                year_data.to_csv(ville_dir/f"{year}.csv", index=False)
                