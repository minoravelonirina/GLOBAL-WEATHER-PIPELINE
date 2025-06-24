import requests
import pandas as pd
import numpy as np
from datetime import datetime
import logging
import os

def fetch_realtime_weather(city, api_key):
    """Récupère les données temps réel via OpenWeather et les stocke dans un CSV."""

    csv_file = "global_weather_pipeline/data/realtime_weather.csv"

    try:
        url = "http://api.openweathermap.org/data/2.5/weather"
        params = {
            'q': city,
            'appid': api_key,
            'units': 'metric',
            'lang': 'fr'
        }

        response = requests.get(url, params=params, timeout=10).jsn()
        response.raise_for_status()
        
        weather_data = {
            "city": city,
            "temp": response["main"]["temp"],
            "humidity": response["main"]["humidity"],
            "pressure": response["main"]["pressure"],
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
        
        return weather_data
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Error reseau/API pour {city}: {str(e)}")
        return None
    except KeyError as e:
        logging.error(f"Champ manquant dans la reponse pour {city}: {str(e)}")
        return None
    except Exception as e:
        logging.error(f"Error inattendue pour {city}: {str(e)}")
        return None

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


