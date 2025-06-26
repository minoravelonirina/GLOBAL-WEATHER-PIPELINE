# global_weather_pipeline/scripts/process_and_combine_data.py
import pandas as pd
from datetime import datetime
import logging
import os
from pathlib import Path
from typing import List

# Assurez-vous que calculate_variability est disponible.
# Il est crucial que calculate_metrics.py soit correctement importable.
try:
    from global_weather_pipeline.scripts.calculate_metrics import calculate_variability
except ImportError:
    # Fallback pour le développement/test si le module n'est pas directement accessible.
    logging.getLogger(__name__).warning(
        "Impossible d'importer calculate_variability depuis calculate_metrics. "
        "Utilisation d'une implémentation simple (écart-type) pour le développement."
    )
    def calculate_variability(data: pd.Series) -> float:
        """Calcule l'écart-type d'une série de données."""
        if data.empty or len(data) < 2:
            return 0.0
        return data.std()

logger = logging.getLogger(__name__)

def calculate_and_combine_weather_metrics(cities: List[str], date: str) -> bool:
    """
    Lit les données météorologiques historiques et en temps réel (supposées déjà existantes),
    calcule les métriques et les sauvegarde.

    Args:
        cities: Liste des noms de villes à traiter.
        date: Date d'exécution du DAG au format 'YYYY-MM-DD'.

    Returns:
        bool: True si le traitement des métriques a réussi, False en cas d'échec.
    """
    try:
        processing_date = datetime.strptime(date, "%Y-%m-%d")
        date_str = processing_date.strftime("%Y-%m-%d")

        if not cities or not all(isinstance(c, str) and c.strip() for c in cities):
            logger.error("La liste 'cities' doit être une liste non vide de noms de villes valides.")
            return False

        cities_normalized = [city.strip().lower() for city in cities if city.strip()]

        project_root = Path(__file__).resolve().parent.parent
        data_dir = project_root/"data"

        historical_path = data_dir/"historique" # Plusieur fichier pour celui ci, dans ceci, il y a encore /{city]/annees.csv}
        realtime_path = data_dir/"realtime"/date_str # Plusieur fichier pour celui ci mais non un seul
        
        historical_data = pd.DataFrame()
        realtime_data = []
        
        if historical_path.exists():
            for city_dir in historical_path.iterdir():
                if city_dir.is_dir():
                    city_name = city_dir.name
                    city_data = []
                    
                    for year_file in city_dir.glob('*.csv'):
                        try:
                            df = pd.read_csv(year_file, parse_dates=['date'])
                            city_data.append(df)
                        except Exception as e:
                            print(f'Erreur lecture {year_file}: {str(e)}')
                            
                    if city_data:
                        historical_data = pd.concat(city_data, ignore_index=True)
                            
        
        # for file in os.listdir()
        
        for file in os.listdir({date}):
            realtime_data.append(pd.read_csv(f'{realtime_path}/{file}.csv'))
        
        output_dir = data_dir/"processed"/date_str
        output_file = output_dir/"weather_metrics.csv"

        output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Répertoire de sortie '{output_dir}' vérifié/créé.")

        # Chargement des données (elles devraient exister à ce stade)
        try:
            
            # historical_df = pd.read_csv(historical_path)
            # realtime_df = pd.read_csv(realtime_path)
            
            historical_df = historical_data
            realtime_df = pd.DataFrame([realtime_data])
            
            logger.info("Fichiers de données historiques et temps réel chargés pour la combinaison.")
        except FileNotFoundError as e:
            logger.error(f"Un fichier source est manquant pour le calcul des métriques : {e}. Assurez-vous que les tâches précédentes ont généré '{historical_path}' et '{realtime_path}'.")
            return False
        except pd.errors.EmptyDataError:
            logger.error("Un fichier de données source est vide pour le calcul des métriques.")
            return False
        except Exception as e:
            logger.error(f"Erreur lors du chargement des fichiers CSV pour combinaison : {e}", exc_info=True)
            return False

        if historical_df.empty or realtime_df.empty:
            logger.error("L'un des DataFrames source est vide. Impossible de calculer les métriques.")
            return False

        for df in [historical_df, realtime_df]:
            if 'city' in df.columns:
                df['city'] = df['city'].astype(str).str.strip().str.lower()
            else:
                logger.warning("La colonne 'city' est manquante dans un DataFrame source.")

        processed_metrics = []

        for city in cities_normalized:
            city_display_name = city.capitalize()

            city_history = historical_df[historical_df["city"] == city]
            city_realtime = realtime_df[realtime_df["city"] == city]

            if city_history.empty or city_realtime.empty:
                logger.warning(f"Données incomplètes (historique ou temps réel) pour '{city_display_name}'. Cette ville sera ignorée.")
                continue

            try:
                if "temp" not in city_history.columns:
                    logger.warning(f"La colonne 'temp' est manquante dans les données historiques pour '{city_display_name}'.")
                    continue
                temp_values = city_history["temp"].dropna()
                if len(temp_values) < 2:
                    logger.warning(f"Données de température historiques insuffisantes (< 2 points) pour '{city_display_name}'.")
                    temp_variability = 0.0
                else:
                    temp_variability = calculate_variability(temp_values)

                result = {
                    "city": city_display_name,
                    "date": date_str,
                    "temp_variability": temp_variability,
                    "stability_score": 1 / (1 + temp_variability) if temp_variability is not None else None,
                    "processing_time": datetime.now().isoformat()
                }

                realtime_record = city_realtime.iloc[0]
                for col in ["temp", "humidity", "wind_speed"]:
                    col_name = f"realtime_{col}"
                    if col in realtime_record and pd.notna(realtime_record[col]):
                        result[col_name] = realtime_record[col]
                    else:
                        result[col_name] = None

                processed_metrics.append(result)
                logger.info(f"Métriques combinées calculées pour '{city_display_name}'.")

            except Exception as e:
                logger.error(f"Erreur lors du calcul des métriques pour '{city_display_name}': {str(e)}", exc_info=True)
                continue

        if not processed_metrics:
            logger.warning("Aucune métrique combinée valide n'a été traitée.")
            return False

        new_metrics_df = pd.DataFrame(processed_metrics)

        if output_file.exists():
            try:
                existing_df = pd.read_csv(output_file)
                existing_df['city'] = existing_df['city'].astype(str)
                existing_df['date'] = existing_df['date'].astype(str)
                new_metrics_df['city'] = new_metrics_df['city'].astype(str)
                new_metrics_df['date'] = new_metrics_df['date'].astype(str)

                combined_df = pd.concat([existing_df, new_metrics_df], ignore_index=True)
                final_df = combined_df.drop_duplicates(subset=["city", "date"], keep="last")
                logger.info(f"Fichier de métriques existant mis à jour. Total lignes: {len(final_df)}.")
            except Exception as e:
                logger.error(f"Erreur lors de la mise à jour du fichier de métriques existant: {e}. Sauvegarde des nouvelles données seulement.", exc_info=True)
                final_df = new_metrics_df
        else:
            final_df = new_metrics_df
            logger.info("Création d'un nouveau fichier de métriques.")

        final_df.to_csv(output_file, index=False)
        logger.info(f"Métriques météorologiques combinées sauvegardées avec succès dans {output_file}")

        return True

    except Exception as e:
        logger.error(f"Erreur critique dans 'calculate_and_combine_weather_metrics': {str(e)}", exc_info=True)
        return False