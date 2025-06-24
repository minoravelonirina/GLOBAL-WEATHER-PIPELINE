import pandas as pd
from datetime import datetime
import logging
import os
from pathlib import Path
from typing import List, Optional
from global_weather_pipeline.scripts.calculate_metrics import calculate_variability

# Configuration du logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_and_process_data(cities: List, processing_date: Optional[datetime] = None) -> bool:
    """
    Combine données temps réel et historiques pour calculer des métriques météorologiques.
    
    Args:
        cities: Liste des noms de villes à traiter
        processing_date: Date de traitement (optionnelle, utilise la date actuelle si None)
    
    Returns:
        bool: True si le traitement a réussi, False en cas d'échec
    """
    try:
        # 1. Initialisation et validation
        if not cities or not isinstance(cities, list):
            logger.error("La liste des villes doit être une liste non vide")
            return False

        processing_date = processing_date or datetime.now()
        date_str = processing_date.strftime("%Y-%m-%d")
        
        # 2. Configuration des chemins
        project_root = Path(__file__).parent.parent
        data_dir = project_root / "data"
        
        historical_path = data_dir / "historical_weather.csv"
        realtime_path = data_dir / "realtime_weather.csv"
        output_dir = data_dir / "processed" / date_str
        output_file = output_dir / "weather_metrics.csv"
        
        # Création des répertoires si inexistants
        os.makedirs(output_dir, exist_ok=True)

        # 3. Chargement des données
        if not historical_path.exists():
            logger.error(f"Fichier historique introuvable: {historical_path}")
            return False
            
        if not realtime_path.exists():
            logger.error(f"Fichier temps réel introuvable: {realtime_path}")
            return False

        historical_df = pd.read_csv(historical_path)
        realtime_df = pd.read_csv(realtime_path)

        if historical_df.empty or realtime_df.empty:
            logger.error("Les fichiers de données sont vides")
            return False

        # 4. Traitement par ville
        processed_data = []
        
        for city in cities:
            city = str(city).strip()
            if not city:
                logger.warning("Nom de ville vide ignoré")
                continue

            try:
                logger.info(f"Traitement de la ville: {city}")
                
                # Filtrage des données
                city_history = historical_df[
                    historical_df["city"].str.lower() == city.lower()
                ]
                
                city_realtime = realtime_df[
                    realtime_df["city"].str.lower() == city.lower()
                ]
                
                if city_history.empty or city_realtime.empty:
                    logger.warning(f"Données incomplètes pour {city}")
                    continue

                # Calcul des métriques
                temp_values = city_history["temp"].dropna()
                if len(temp_values) < 2:
                    logger.warning(f"Données insuffisantes pour calculer la variabilité pour {city}")
                    continue
                    
                temp_variability = calculate_variability(temp_values)
                
                # Création du résultat
                result = {
                    "city": city,
                    "date": date_str,
                    "temp_variability": temp_variability,
                    "stability_score": 1 / (1 + temp_variability),
                    "processing_time": datetime.now().isoformat()
                }
                
                # Ajout des métriques spécifiques au temps réel
                for col in ["temp", "humidity", "wind_speed"]:
                    if col in city_realtime.columns:
                        result[col] = city_realtime[col].values[0]
                
                processed_data.append(result)
                
            except Exception as e:
                logger.error(f"Erreur lors du traitement de {city}: {str(e)}", exc_info=True)
                continue

        # 5. Sauvegarde des résultats
        if not processed_data:
            logger.warning("Aucune donnée valide n'a été traitée")
            return False
            
        result_df = pd.DataFrame(processed_data)
        
        # Concaténation avec les données existantes si le fichier existe déjà
        if output_file.exists():
            existing_df = pd.read_csv(output_file)
            result_df = pd.concat([existing_df, result_df], ignore_index=True)
            result_df = result_df.drop_duplicates(subset=["city", "date"], keep="last")
        
        # Sauvegarde finale
        result_df.to_csv(output_file, index=False)
        logger.info(f"Données sauvegardées avec succès dans {output_file}")
        
        return True

    except Exception as e:
        logger.error(f"Erreur critique dans fetch_and_process_data: {str(e)}", exc_info=True)
        return False