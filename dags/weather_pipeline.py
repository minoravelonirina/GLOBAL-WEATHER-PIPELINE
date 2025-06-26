# Mon_DAG_Meteo.py (ou le nom de votre fichier DAG)
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Importations des scripts externes. Assurez-vous que ces chemins sont corrects
# et que Python/Airflow peut les trouver.
from global_weather_pipeline.scripts.fetch_weather import fetch_realtime_weather
from global_weather_pipeline.scripts.extract_historical import extract_historical_data # Nouvelle importation
from global_weather_pipeline.scripts.process_and_combine_data import calculate_and_combine_weather_metrics # Nouvelle importation

# Configuration par défaut du DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 19)
}

# Liste des villes à traiter
CITIES = [
    'Paris', 'Berlin', 'Washington D.C.', 'Tokyo', 'Pékin', 'New Delhi',
    'Brasilia', 'Moscou', 'Londres', 'Rome', 'Ottawa', 'Madrid',
    'Canberra', 'Mexico', 'Pretoria', 'Le Caire', 'Abuja', 'Antananarivo'
]

with DAG(
    dag_id='weather_pipeline_separated_tasks', # ID du DAG mis à jour
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    max_active_runs=1
) as dag:

    # 1. Tâche pour s'assurer que les données historiques sont prêtes
    # Cette tâche pourrait être plus complexe si les données historiques doivent être téléchargées
    # ou agrégées quotidiennement. Ici, elle vérifie juste la disponibilité du CSV.
    extract_historical_task = PythonOperator(
        task_id='extract_and_validate_historical_data',
        python_callable=extract_historical_data,
        op_args=[CITIES, "{{ds}}", [2008, 2009, 2010]]
    )

    # 2. Tâches de récupération des don nées temps réel (une par ville)
    fetch_realtime_tasks = []
    for city in CITIES:
        task_id = f'fetch_realtime_for_{city.lower().replace(" ", "_").replace(".", "")}'
        fetch_realtime_task = PythonOperator(
            task_id=task_id,
            python_callable=fetch_realtime_weather,
            op_args=[city, "{{ var.value.API_KEY }}", "{{ds}}"],
        )
        fetch_realtime_tasks.append(fetch_realtime_task)

    # 3. Tâche de fusion et de calcul des métriques
    # Cette tâche dépend à la fois de l'extraction historique et de toutes les extractions temps réel.
    calculate_and_combine_metrics_task = PythonOperator(
        task_id="calculate_combined_weather_metrics",
        python_callable=calculate_and_combine_weather_metrics,
        op_args=[CITIES, "{{ds}}"], # Passe la liste des villes et la date d'exécution
    )

    # ======= Orchestration des Tâches ======== #
    # Les tâches de récupération temps réel ET la tâche d'extraction historique
    # doivent toutes se terminer AVANT que la tâche de calcul des métriques ne commence.
    extract_historical_task >> calculate_and_combine_metrics_task
    fetch_realtime_tasks >> calculate_and_combine_metrics_task

    # Vous pouvez ajouter ici des tâches supplémentaires, par exemple pour sauvegarder les résultats
    # dans une base de données après le calcul des métriques.
    # calculate_and_combine_metrics_task >> save_metrics_to_database_task