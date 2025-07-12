import pandas as pd
from datetime import datetime
from pathlib import Path

def transform_data(date: str):
    # Charger les données météo brutes
    processing_date = datetime.strptime(date, "%Y-%m-%d")
    date_str = processing_date.strftime("%Y-%m-%d")
    project_dir = Path(__file__).parent.parent
    input_path = project_dir/"data"/"processed"/date_str/"weather_metrics.csv"
    df = pd.read_csv(input_path)

    # Dossier de sortie
    output_dir = project_dir/"data"/"dimensional_model"
    output_dir.mkdir(parents=True, exist_ok=True)

    # ---------- 1. DIMENSION Ville ----------
    df_city = pd.DataFrame({"city_name": df["city"].unique()})
    df_city["city_id"] = df_city.index + 1
    df_city.to_csv(output_dir / "dim_city.csv", index=False)

    # Mapping ville -> ID
    city_id_map = dict(zip(df_city["city_name"], df_city["city_id"]))
    df["city_id"] = df["city"].map(city_id_map)

    # ---------- 2. DIMENSION Date ----------
    today = datetime.today()
    date_id = int(today.strftime("%Y%m%d"))
    df_date = pd.DataFrame([{
        "date_id": date_id,
        "full_date": today.date(),
        "day": today.day,
        "month": today.month,
        "year": today.year
    }])
    df_date.to_csv(output_dir / "dim_date.csv", index=False)
    df["date_id"] = date_id

    # ---------- 3. TABLE DE FAITS ----------
    fact_df = df[[
        "city_id", "date_id", "rain", "realtime_temp", "temp_variability", "stability_score"
    ]].rename(columns={ 
        "rain": "total_rain",
        "realtime_temp": "avg_temp"
    })
    fact_df.to_csv(output_dir / "fact_weather_metrics.csv", index=False)
