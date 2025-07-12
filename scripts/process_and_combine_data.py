import pandas as pd
from datetime import datetime
import logging
from pathlib import Path
from typing import List
from global_weather_pipeline.scripts.calculate_metrics import calculate_variability
from global_weather_pipeline.scripts.load_realtime_data import load_realtime_data
from global_weather_pipeline.scripts.load_historical_data import load_historical_data


def calculate_and_combine_weather_metrics(cities: List[str], date: str) -> bool:
    """Fonction principale pour calculer les métriques météorologiques."""
    try:
        # Validation des entrées
        processing_date = datetime.strptime(date, "%Y-%m-%d")
        date_str = processing_date.strftime("%Y-%m-%d")
        
        if not cities or not all(isinstance(c, str) and c.strip() for c in cities):
            logging.error("Liste de villes invalide")
            return False
            
        cities_normalized = [city.strip().lower() for city in cities if city.strip()]
        
        # Configuration des chemins
        project_root = Path(__file__).resolve().parent.parent
        data_dir = project_root / "data"
        historical_path = data_dir / "historique"
        realtime_path = data_dir / "realtime" / date_str
        output_dir = data_dir / "processed" / date_str
        output_file = output_dir / "weather_metrics.csv"
        
        # Chargement des données
        historical_data = load_historical_data(historical_path, cities_normalized)
        realtime_data = load_realtime_data(realtime_path)
        
        if not historical_data or not realtime_data:
            logging.error("Données insuffisantes pour le traitement")
            return False
            
        historical_df = pd.concat(historical_data, ignore_index=True)
        realtime_df = pd.concat(realtime_data, ignore_index=True)
        
        # Calcul des jours pluvieux par ville (si colonne rain existe)
        if "rain" in historical_df.columns:
            historical_df["is_rainy"] = historical_df["rain"].fillna(0) > 0
            
            # Calcul du total de pluie en mm par ville
            rain_totals = (
                historical_df.groupby("city")["rain"]
                .sum()
                .round(2)
                .to_dict()
            )
        else:
            logging.warning("Error about the rain value")
            
            
        # Traitement par ville
        processed_metrics = []
        for city in cities_normalized:
            try:
                city_history = historical_df[historical_df["city"] == city]
                city_realtime = realtime_df[realtime_df["city"] == city]
                
                if city_history.empty or city_realtime.empty:
                    logging.warning(f"Données incomplètes pour {city}")
                    continue
                
                # Calcul des métriques
                temp_values = city_history["temp"].dropna()
                temp_variability = calculate_variability(temp_values) if len(temp_values) >= 2 else 0.0
                
                realtime_record = city_realtime.iloc[0]
                
                metrics = {
                    "city": city.capitalize(),
                    "month": processing_date.month,
                    "year": processing_date.year,
                    "date": date_str,
                    "rain": rain_totals.get(city, 0.0),
                    "temp_variability": round(temp_variability, 3),
                    "stability_score": round(1 / (1 + temp_variability), 3) if temp_variability != 0 else 1.0,
                    "realtime_temp": realtime_record.get("temp", None),
                    "realtime_humidity": realtime_record.get("humidity", None),
                }
                processed_metrics.append(metrics)
                logging.info(f"Métriques calculées pour {city.capitalize()}")
                
            except Exception as e:
                logging.error(f"Erreur de traitement pour {city}: {str(e)}", exc_info=True)
        
        if not processed_metrics:
            logging.error("Aucune métrique valide générée")
            return False
            
        # Sauvegarde des résultats
        output_dir.mkdir(parents=True, exist_ok=True)
        metrics_df = pd.DataFrame(processed_metrics)
        
            # Conversion des types de données
        metrics_df['rain'] = metrics_df['rain'].astype('float64')
        metrics_df['temp_variability'] = metrics_df['temp_variability'].astype('float64')
        metrics_df['stability_score'] = metrics_df['stability_score'].astype('float64')
        metrics_df['realtime_temp'] = metrics_df['realtime_temp'].astype('float64')
        metrics_df['realtime_humidity'] = metrics_df['realtime_humidity'].astype('float64')
        
        if output_file.exists():
            try:
                existing_df = pd.read_csv(output_file)
                final_df = pd.concat([existing_df, metrics_df])
                final_df.drop_duplicates(subset=["city", "date"], keep="last", inplace=True)
            except Exception as e:
                logging.error(f"Erreur de fusion des données: {str(e)}")
                final_df = metrics_df
        else:
            final_df = metrics_df
        
        final_df.to_csv(output_file, index=False)
        logging.info(f"Résultats sauvegardés dans {output_file}")
        return True
        
    except Exception as e:
        logging.error(f"Erreur critique: {str(e)}", exc_info=True)
        return False