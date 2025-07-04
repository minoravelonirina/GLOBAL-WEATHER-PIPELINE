from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from global_weather_pipeline.scripts.fetch_weather import fetch_realtime_weather
from global_weather_pipeline.scripts.extract_historical import extract_historical_data
from global_weather_pipeline.scripts.process_and_combine_data import calculate_and_combine_weather_metrics

# Configuration par défaut du DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 19)
}

# Liste des villes à traiter
CITIES = [
    'Berlin', 'Munich', 'Hamburg', 'Cologne', 
    'Frankfurt', 'Stuttgart', 'Dusseldorf',
    'Dresden','Leipzig', 'Hanover'
]

with DAG(
    dag_id='weather_pipeline_separated_tasks',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    max_active_runs=1
) as dag:

    # 1. Tâche d'extraction des données historique
    extract_historical_task = PythonOperator(
        task_id='extract_and_validate_historical_data',
        python_callable=extract_historical_data,
        op_args=[CITIES, 3]
    )

    # 2. Tâches de récupération des données temps réel
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
    calculate_and_combine_metrics_task = PythonOperator(
        task_id="calculate_combined_weather_metrics",
        python_callable=calculate_and_combine_weather_metrics,
        op_args=[CITIES, "{{ds}}"],
    )

    # ======= Orchestration des Tâches ======== #
    extract_historical_task >> calculate_and_combine_metrics_task
    fetch_realtime_tasks >> calculate_and_combine_metrics_task
