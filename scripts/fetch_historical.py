
import requests
import pandas as pd
import numpy as np
from datetime import datetime
from scripts.calculate_metrics import calculate_variability
import logging
from scripts.fetch_weather import fetch_realtime_weather

def fetch_and_process_data(cities):
    """Combine données temps réel + historiques et calcule les métriques."""
    if not cities:
        logging.error("La liste des villes ne peut pas être vide")
        return pd.DataFrame()
    
    try:
        # 1. Charger les données historiques
        try:
            historical_df = pd.read_csv("data/historical_weather.csv")
            if historical_df.empty:
                logging.warning("Le fichier historique est vide")
                return pd.DataFrame()
        except FileNotFoundError:
            logging.error("Fichier historique introuvable")
            return pd.DataFrame()
        except pd.errors.EmptyDataError:
            logging.error("Fichier historique corrompu ou vide")
            return pd.DataFrame()
        except Exception as e:
            logging.error(f"Erreur inattendue lors de la lecture historique: {str(e)}")
            return pd.DataFrame()

        # 2. Collecte pour chaque ville
        realtime_data = []
        for city in cities:
            if not isinstance(city, str) or not city.strip():
                logging.warning(f"Nom de ville invalide: {city}")
                continue

            try:
                # Récupération données temps réel
                realtime_stats = fetch_realtime_weather(city)
                if realtime_stats is None:
                    logging.warning(f"Données temps réel non disponibles pour {city}")
                    continue

                # Filtrage données historiques
                city_history = historical_df[historical_df["city"] == city]
                if city_history.empty:
                    logging.warning(f"Aucune donnée historique pour {city}")
                    continue

                # Calcul des métriques
                try:
                    temp_variability = calculate_variability(city_history["temp"])
                    realtime_stats["temp_variability"] = temp_variability
                    realtime_stats["stability_score"] = 1 / (1 + temp_variability)
                except Exception as e:
                    logging.error(f"Erreur calcul métriques pour {city}: {str(e)}")
                    continue

                realtime_data.append(realtime_stats)

            except Exception as e:
                logging.error(f"Erreur traitement ville {city}: {str(e)}")
                continue

        return pd.DataFrame(realtime_data) if realtime_data else pd.DataFrame()

    except Exception as e:
        logging.error(f"Erreur critique dans fetch_and_process_data: {str(e)}")
        return pd.DataFrame()