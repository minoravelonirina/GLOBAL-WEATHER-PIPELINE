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

def fetch_realtime_weather(city, api_key) -> bool:
    """Récupère les données temps réel via OpenWeather et les stocke dans un CSV."""

     # 1. Configuration des chemins de manière robuste
    project_dir = Path(__file__).parent.parent  # Remonte de 2 niveaux
    data_dir = project_dir / "global_weather_pipeline" / "data"
    csv_file = data_dir / "realtime_weather.csv"

    # 2. Création du répertoire si inexistant
    data_dir.mkdir(parents=True, exist_ok=True)

    # csv_file = "global_weather_pipeline/data/realtime_weather.csv"

    session = requests.Session()
    
    # Configuration des retries
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504]
    )
    
    session.mount('https://', HTTPAdapter(max_retries=retries))

    try:
        url = "http://api.openweathermap.org/data/2.5/weather"
        params = {
            'q': city,
            'appid': api_key,
            'units': 'metric',
            'lang': 'fr'
        }

        response = requests.get(url, params=params, timeout=(3.05, 27))
        response.raise_for_status()

        data=response.json()
        
        weather_data = {
            "city": city,
            "temp": data["main"]["temp"],
            "humidity": data["main"]["humidity"],
            "pressure": data["main"]["pressure"],
            "timestamp": datetime.now()
        }

        # Convertir en DataFrame
        df = pd.DataFrame([weather_data])
        
        # Vérifier si le fichier existe déjà
        if os.path.exists(csv_file):
            # Charger l'existant et concaténer
            existing_df = pd.read_csv(csv_file)
            df = pd.concat([existing_df, df], ignore_index=True)
        
        # Sauvegarder dans CSV
        df.to_csv(csv_file, index=False)
        
        return True
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Error reseau/API pour {city}: {str(e)}")
        return False
    except KeyError as e:
        logging.error(f"Champ manquant dans la reponse pour {city}: {str(e)}")
        return False
    except Exception as e:
        logging.error(f"Error inattendue pour {city}: {str(e)}")
        return False

# def fetch_and_process_data(cities):
#     """Combine données temps réel + historiques et calcule les métriques."""
#     # Charger les données historiques
#     historical_df = pd.read_csv("data/historical_weather.csv")
    
#     # Collecte pour chaque ville
#     realtime_data = []
#     for city in cities:
#         realtime_stats = fetch_realtime_weather(city)
#         city_history = historical_df[historical_df["city"] == city]
        
#         # Calcul des métriques
#         realtime_stats["temp_variability"] = calculate_variability(city_history["temp"])
#         realtime_stats["stability_score"] = 1 / (1 + realtime_stats["temp_variability"])
        
#         realtime_data.append(realtime_stats)
    
#     return pd.DataFrame(realtime_data)


