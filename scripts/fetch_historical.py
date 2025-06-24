import requests
import pandas as pd
import numpy as np
from datetime import datetime
import logging
import os
from pathlib import Path
from datetime import datetime
from global_weather_pipeline.scripts.fetch_weather import  fetch_realtime_weather
from global_weather_pipeline.scripts.calculate_metrics import  calculate_variability

# Configuration du logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_and_process_data(cities, date):
    """
    Combine données temps réel + historiques et calcule les métriques.
    
    Args:
        cities (list): Liste des noms de villes à traiter
        historical_path (str): Chemin vers le fichier historique
    
    Returns:
        pd.DataFrame: DataFrame contenant les données traitées
    """

    # historical_data_path="global_weather_pipeline.data/historical_weather.csv"
    # Validation des entrées
    if not cities or not isinstance(cities, list):
        logger.error("La liste des villes doit être une liste non vide")
        return pd.DataFrame()
    
    # Convertir en Path object pour une meilleure gestion des chemins
    # historical_path = Path(historical_data_path).absolute()

    project_root = Path(__file__).parent.parent  # Remonte de 2 niveaux depuis le script
    historical_path = project_root / "data" / "historical_weather.csv"
    
    # 1. Charger les données historiques
    try:
        if not historical_path.exists():
            logger.error(f"Fichier historique introuvable: {historical_path}")
            return pd.DataFrame()
            
        historical_df = pd.read_csv(historical_path)
        
        if historical_df.empty:
            logger.warning("Le fichier historique est vide")
            return pd.DataFrame()
            
        # Vérification des colonnes nécessaires
        required_columns = {"city", "temp"}
        if not required_columns.issubset(historical_df.columns):
            missing = required_columns - set(historical_df.columns)
            logger.error(f"Colonnes manquantes dans historique: {missing}")
            return pd.DataFrame()
            
    except pd.errors.EmptyDataError:
        logger.error("Fichier historique corrompu ou vide")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Erreur lecture historique: {str(e)}")
        return pd.DataFrame()

    # 2. Collecte pour chaque ville
    realtime_data = []
    
    for city in cities:
        if not isinstance(city, str) or not city.strip():
            logger.warning(f"Nom de ville invalide ignoré: {city}")
            continue

        try:
            logger.info(f"Traitement de la ville: {city}")
            
            # Récupération données temps réel
            realtime_stats = fetch_realtime_weather(city)
            if realtime_stats is None:
                logger.warning(f"Données temps réel non disponibles pour {city}")
                continue

            # Filtrage données historiques
            city_history = historical_df[historical_df["city"].str.lower() == city.lower()]
            if city_history.empty:
                logger.warning(f"Aucune donnée historique pour {city}")
                continue

            # Calcul des métriques avec gestion d'erreur
            try:
                temp_values = city_history["temp"].dropna()
                if len(temp_values) < 2:
                    logger.warning(f"Données historiques insuffisantes pour calculer la variabilité pour {city}")
                    continue
                    
                temp_variability = calculate_variability(temp_values)
                realtime_stats["temp_variability"] = temp_variability
                realtime_stats["stability_score"] = 1 / (1 + temp_variability)
                
                # Ajout timestamp
                realtime_stats["processing_time"] = datetime.now().isoformat()
                
                realtime_data.append(realtime_stats)
                
            except Exception as e:
                logger.error(f"Erreur calcul métriques pour {city}: {str(e)}")
                continue

        except Exception as e:
            logger.error(f"Erreur traitement ville {city}: {str(e)}")
            continue

    # Création du DataFrame final
    if not realtime_data:
        logger.warning("Aucune donnée valide n'a été traitée")
        return pd.DataFrame()
        
    result_df = pd.DataFrame(realtime_data)

    # Création du répertoire avec la date actuelle
    date = datetime.now().strftime("%Y-%m-%d")
    output_dir = f"data/global_weather_data/{date}"
    os.makedirs(output_dir, exist_ok=True)  # Crée le dossier s'il n'existe pas

    # Chemin complet du fichier CSV
    output_file = f"{output_dir}/weather_data.csv"

    # Vérification et concaténation si le fichier existe déjà
    if os.path.exists(output_file):
        existing_df = pd.read_csv(output_file)
        result_df = pd.concat([existing_df, result_df], ignore_index=True)

    # Sauvegarde dans le fichier CSV
    result_df.to_csv(output_file, index=False)
        
    
    # Vérification finale
    # required_output_columns = {"city", "temp", "temp_variability", "stability_score"}
    # if not required_output_columns.issubset(result_df.columns):
    #     missing = required_output_columns - set(result_df.columns)
    #     logger.error(f"Colonnes manquantes dans le résultat: {missing}")
    #     return pd.DataFrame()
        
    return result_df