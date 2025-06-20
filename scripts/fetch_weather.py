import requests
import pandas as pd
import numpy as np
from datetime import datetime
from scripts.calculate_metrics import calculate_variability
import logging


def fetch_realtime_weather(city, api_key):
    """Récupère les données temps réel via OpenWeather."""
    try:
        url = f"http://api.openweathermap.org/data/2.5/weather"
        params = {
            'q': city,
            'appid': api_key,
            'units': 'metric',
            'lang': 'fr'
        }

        response = requests.get(url).json()
        return {
            "city": city,
            "temp": response["main"]["temp"],
            "humidity": response["main"]["humidity"],
            "pressure": response["main"]["pressure"],
            "timestamp": datetime.now()
        }
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


