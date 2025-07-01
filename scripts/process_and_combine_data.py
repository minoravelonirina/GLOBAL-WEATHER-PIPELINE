import pandas as pd
from datetime import datetime
import logging
from pathlib import Path
from typing import List
from global_weather_pipeline.scripts.calculate_metrics import calculate_variability

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def calculate_and_combine_weather_metrics(cities: List[str], date: str) -> bool:
    try:
        processing_date = datetime.strptime(date, "%Y-%m-%d")
        date_str = processing_date.strftime("%Y-%m-%d")

        if not cities or not all(isinstance(c, str) and c.strip() for c in cities):
            logger.error("La liste 'cities' doit être une liste non vide de noms de villes valides.")
            return False

        cities_normalized = [city.strip().lower() for city in cities if city.strip()]
        project_root = Path(__file__).resolve().parent.parent
        data_dir = project_root / "data"
        historical_path = data_dir / "historique"
        realtime_path = data_dir / "realtime" / date_str
        output_dir = data_dir / "processed" / date_str
        output_file = output_dir / "weather_metrics.csv"

        historical_data = []

        if historical_path.exists():
            for city_dir in historical_path.iterdir():
                logger.info(f"Nom du dossier ville détecté : {city_dir.name}")
                logger.info(f"Villes normalisées attendues : {cities_normalized}")
                if city_dir.is_dir() and city_dir.name in cities_normalized:
                    for year_file in city_dir.glob('*.csv'):
                        try:
                            df = pd.read_csv(year_file, parse_dates=['timestamp'], header=0)
                            logger.info(f"Lecture {year_file.name} : {df.shape[0]} lignes")
                            logger.info(df.head().to_string(index=False))
                            historical_data.append(df)
                            logger.warning(f"{len(historical_data)} fichiers historique chargés.")
                            
                        except Exception as e:
                            logger.warning(f"Erreur lecture {year_file}: {e}")

        if not realtime_path.exists():
            logger.error(f"Répertoire temps réel manquant: {realtime_path}")
            return False

        realtime_data = []
        for file in realtime_path.glob("*.csv"):
            try:
                df = pd.read_csv(file)
                realtime_data.append(df)
                logger.info(f"{len(realtime_data)} fichiers temps réel chargés.")
                
            except Exception as e:
                logger.warning(f"Erreur lecture {file}: {e}")

        if not historical_data or not realtime_data:
            logger.error("Pas assez de données historiques ou temps réel.")
            return False

        historical_df = pd.concat(historical_data, ignore_index=True)
        realtime_df = pd.concat(realtime_data, ignore_index=True)

        for df in [historical_df, realtime_df]:
            if 'city' in df.columns:
                df['city'] = df['city'].astype(str).str.strip().str.lower()

        processed_metrics = []
        logger.info(f"Fichiers historiques trouvés : {[str(f) for f in historical_path.rglob('*.csv')]}")
        logger.info(f"Fichiers temps réel trouvés : {[str(f) for f in realtime_path.glob('*.csv')]}")


        for city in cities_normalized:
            city_history = historical_df[historical_df["city"] == city]
            city_realtime = realtime_df[realtime_df["city"] == city]

            if city_history.empty or city_realtime.empty:
                logger.warning(f"Données manquantes pour '{city}'. Ignoré.")
                continue

            try:
                temp_values = city_history["temp"].dropna()
                temp_variability = calculate_variability(temp_values) if len(temp_values) >= 2 else 0.0

                realtime_record = city_realtime.iloc[0]

                result = {
                    "city": city.capitalize(),
                    "date": date_str,
                    "timestamp": datetime.now().isoformat(),
                    "temp_variability": temp_variability,
                    "stability_score": 1 / (1 + temp_variability)
                }

                for col in ["temp", "humidity"]:
                    result[f"realtime_{col}"] = realtime_record[col] if col in realtime_record and pd.notna(realtime_record[col]) else None

                processed_metrics.append(result)
                logger.info(f"Métriques traitées pour {city.capitalize()}")

            except Exception as e:
                logger.error(f"Erreur métriques {city}: {e}", exc_info=True)

        if not processed_metrics:
            logger.warning("Aucune métrique valide générée.")
            return False

        new_metrics_df = pd.DataFrame(processed_metrics)

        if output_file.exists():
            try:
                existing_df = pd.read_csv(output_file)
                final_df = pd.concat([existing_df, new_metrics_df], ignore_index=True)
                final_df.drop_duplicates(subset=["city", "timestamp"], keep="last", inplace=True)
            except Exception as e:
                logger.warning(f"Erreur fusion fichier existant: {e}")
                final_df = new_metrics_df
        else:
            final_df = new_metrics_df

        output_dir.mkdir(parents=True, exist_ok=True)
        final_df.to_csv(output_file, index=False)
        logger.info(f"Fichier sauvegardé : {output_file}")

        return True

    except Exception as e:
        logger.error(f"Erreur critique: {e}", exc_info=True)
        return False
