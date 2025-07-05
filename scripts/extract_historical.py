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
    input_path = project_dir/"data"/"Data_Germany.csv"
    output_dir = project_dir/"data"/"historique"
    
    cols = [
        'City',
        'Latitude',
        'Longitude',
        'Month',
        'Year',
        'Rainfall (mm)',
        'Temperature (°C)',
        'Humidity (%)',
    ]


    # Chargement des données avec vérification
    df = pd.read_csv(input_path, usecols=cols)
    
    # Renommage des colonnes
    df = df.rename(
        columns={
            'City': 'city', 
            'Latitude': 'lat',
            'Longitude': 'lon',
            'Month': 'month',
            'Year': 'year',
            'Rainfall (mm)' : 'rain',
            'Temperature (°C)': 'temp',
            'Humidity (%)': 'humidity'
            }
        )
    
    # Calcul des années de filtrage
    max_year = df['year'].max()
    min_year = max_year - years 
    
    available_years = df['year'].unique()

    if min_year not in available_years:
        min_available_year = df['year'].min()
        logging.warning(
            f"Année {min_year} non présente dans les données. "
            f"Utilisation de l'année minimale disponible : {min_available_year}"
        )
        min_year = min_available_year

    
    # Vérification que cities est une liste non vide
    if not isinstance(cities, list) or len(cities) == 0:
        raise ValueError("L'argument cities doit être une liste non vide")
    
    # Filtrage combiné par villes et années
    filtered_df = df[
        (df['city'].isin(cities)) & 
        (df['year'] >= min_year) &
        (df['year'] <= max_year)
    ].copy()
    
    # Sauvegarde
    output_dir.mkdir(exist_ok=True, parents=True)
    
    for ville in cities:
        ville_data = filtered_df[filtered_df['city'] == ville]
        
        if not ville_data.empty:
            ville_dir = output_dir / ville.replace(" ", "_")
            ville_dir.mkdir(exist_ok=True)
            
            for year, year_data in ville_data.groupby(ville_data['year']):
                year_data.to_csv(ville_dir/f"{year}.csv", index=False)
                