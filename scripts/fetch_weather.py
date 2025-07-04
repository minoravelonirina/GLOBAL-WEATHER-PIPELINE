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

def fetch_realtime_weather(city:str, api_key:str, date:str) -> bool:
    """Récupère les données temps réel via OpenWeather et les stocke dans un CSV."""

    # Configuration des chemins
    project_dir = Path(__file__).parent.parent
    data_dir = project_dir/"data"/"realtime"/date
    csv_file = data_dir / f'{city}.csv'
    data_dir.mkdir(parents=True, exist_ok=True)

    # Session HTTP avec retry
    session = requests.Session()
    # retries = Retry(
    #     total=3,
    #     backoff_factor=1,
    #     status_forcelist=[500, 502, 503, 504]
    # )
    # session.mount('https://', HTTPAdapter(max_retries=retries))

    try:
        # Requête API
        response = session.get(
            "https://api.openweathermap.org/data/2.5/weather",
            params={
                'q': city,
                'appid': api_key,
                'units': 'metric', 
                'lang': 'fr'
            },
            timeout=(3.05, 30)  # 3s connexion, 30s lecture
        )
        
        # Affiche le status de la reponse
        response.raise_for_status()
        data = response.json()

        # Préparation des données dans une dictionary
        # weather_data = {
        #     "city": city,
        #     "temp": data["main"]["temp"],
        #     "humidity": data["main"]["humidity"],
        #     "pressure": data["main"]["pressure"],
        #     "timestamp": datetime.now().month
        # }

        weather_data = {
            "city": city,
            "lat": data["coord"]["lat"],
            "lon": data["coord"]["lon"],
            "month": datetime.now().month,
            "year": datetime.now().year,
            "temp": data["main"]["temp"],
            "humidity": data["main"]["humidity"]
        }

        # Met les donnees dans un dataframe
        df = pd.DataFrame([weather_data])
        
        try:
            # Lire le fichier destinataire si il contient deja des donnees
            existing_df = pd.read_csv(csv_file)
            
            # Concatener les donnees deja recolte avec les nouveaux et supprimer les duplications
            df = pd.concat([existing_df, df]).drop_duplicates()
        except FileNotFoundError:
            pass

        # Met les nouveaux donnees dans le fichier destinataire
        df.to_csv(csv_file, index=False)
        return True

    except requests.exceptions.RequestException as e:
        logging.error(f"Erreur réseau/API pour {city}: {str(e)}", exc_info=True)
        return False
    except KeyError as e:
        logging.error(f"Champ manquant dans la réponse pour {city}: {str(e)}")
        return False
    except Exception as e:
        logging.error(f"Erreur inattendue pour {city}: {str(e)}", exc_info=True)
        return False
    