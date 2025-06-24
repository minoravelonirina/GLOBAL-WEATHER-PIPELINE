# import sys
# sys.path.insert(0, "/home/minosoa/airflow/dags/global_weather_pipeline")
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from global_weather_pipeline.scripts.fetch_historical import fetch_and_process_data
# from .scripts.fetch_historical import fetch_and_process_data
# from global_weather_pipeline.scripts.database_utils import save_to_db

# Configuration par defaut du DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 19),
}

# Liste des villes a traiter
CITIES = [
    'Paris', 'Berlin', 'Washington D.C.', 'Tokyo', 'Pékin', 'New Delhi', 
    'Brasilia', 'Moscou', 'Londres', 'Rome', 'Ottawa', 'Madrid', 
    'Canberra', 'Mexico', 'Pretoria', 'Le Caire', 'Abuja', 'Antananarivo'
    ]

with DAG(
    'weather_comparaison_pipeline',
    default_args=default_args,
    schedule='@daily', # Execution quotidienne
    catchup=False,              # Ne pas rattraper les executions passees
    max_active_runs=1,          # Pour eviter les conflits
) as dag:
    
    # =========== Fetch task ============ #
    fetch_tasks = [
        PythonOperator(
            task_id=f'fetch_{city.lower().replace(" ", "_")}',
            python_callable=fetch_and_process_data,
            op_args=[city, "{{ var.value.API_KEY }}", "{{ds}}"],
        )
        for city in CITIES
    ]
    
    # ======= Orchestration ======== #
    # for task in fetch_tasks:
    #     task
    fetch_tasks



    # fetch_task = PythonOperator(
    #     task_id="fetch_weather_data",
    #     python_callable=fetch_and_process_data,
    #     op_kwargs=[CITIES, "{{var.value.API_KEY}}", "{{ds}}"],
    #     # op_kwargs={"cities": ["Paris", "Berlin", "Tokyo", "Antananarivo"]},
    # )

    
# /projet_meteo/
# │
# ├── /dags/                          # Dossier Airflow
# │   └── weather_pipeline.py          # DAG principal
# │
# ├── /scripts/                       # Scripts Python
# │   ├── fetch_weather.py            # Collecte des données (OpenWeather + Kaggle)
# │   ├── calculate_metrics.py        # Calcul stabilité/variabilité
# │   └── database_utils.py           # Gestion base de données
# │
# ├── /data/                          # Données brutes
# │   ├── historical_weather.csv      # Données Kaggle (3 ans)
# │   └── realtime_weather.json       # Exemple de sortie API
# │
# ├── weather_dashboard.pbix          # Fichier Power BI
# └── README.md                       # Documentation
