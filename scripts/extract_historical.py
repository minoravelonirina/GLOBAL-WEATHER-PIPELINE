import pandas as pd
from pathlib import Path
from datetime import datetime

def extract_historical_data(cities: list, extract_date: str, years: int = 3) -> None:
    """
    Filtre et stocke les données historiques pour les villes spécifiées et les N années
    
    Args:
        cities: Liste des villes à filtrer
        years: Nombre d'années à conserver
    """
    project_dir = Path(__file__).parent.parent
    input_path = project_dir/"data"/"historical_weather.csv"
    output_dir = project_dir/"data"/"historique"

    
    # Chargement des données
    df = pd.read_csv(input_path, parse_dates=['date'])
    
    # Trouver la dernière année disponible dans les données
    max_data_year = df['date'].dt.year.max()
    min_year = max_data_year - years + 1  # +1 pour inclure l'année de départ
    
    filtered_df = df[
        (df['ville'].isin(cities)) & 
        (df['date'].dt.year >= min_year)
    ].copy()
    
    # Création du répertoire de sortie
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    # Stockage par ville et année
    for ville in cities:
        ville_dir = output_path / ville
        ville_dir.mkdir(exist_ok=True)
        
        ville_data = filtered_df[filtered_df['ville'] == ville]
        
        for year in range(min_year, max_data_year + 1):
            year_data = ville_data[ville_data['date'].dt.year == year]
            if not year_data.empty:
                year_data.to_csv(ville_dir / f"{year}.csv", index=False)

# Exemple d'utilisation
# cities_of_interest = ["Paris", "Lyon", "Marseille"]
# extract_historical_data(
#     input_path="historical_weather.csv",
#     output_dir="processed_data",
#     cities=cities_of_interest,
#     years=3
# )