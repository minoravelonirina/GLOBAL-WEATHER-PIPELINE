import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px
from datetime import datetime
from pathlib import Path

def plot_rain_total(df):
    plt.figure(figsize=(12,6))
    sns.barplot(data=df, x="city", y="rain", palette="Blues_d")
    plt.xticks(rotation=45)
    plt.title("Pluie totale par ville (mm)")
    plt.ylabel("Rainfall (mm)")
    plt.tight_layout()
    plt.show()

def plot_stability_score(df):
    plt.figure(figsize=(12,6))
    sns.barplot(data=df, x="city", y="stability_score", palette="coolwarm")
    plt.xticks(rotation=45)
    plt.title("Stability Score par ville (1 = très stable)")
    plt.ylabel("Stability Score")
    plt.tight_layout()
    plt.show()

def plot_rain_vs_stability(df):
    plt.figure(figsize=(8,6))
    sns.scatterplot(data=df, x="rain", y="stability_score", hue="city", s=100)
    plt.title("Pluie vs Stabilité climatique")
    plt.xlabel("Pluie totale (mm)")
    plt.ylabel("Stability Score")
    plt.tight_layout()
    plt.show()

def plot_temp_vs_humidity(df):
    plt.figure(figsize=(12,6))
    sns.scatterplot(data=df, x="realtime_temp", y="realtime_humidity", hue="city", s=120)
    plt.title("Température vs Humidité actuelle")
    plt.xlabel("Température (°C)")
    plt.ylabel("Humidité (%)")
    plt.tight_layout()
    plt.show()
    plt.savefig("nom_du_fichier.png")


def plot_interactive_scatter(df):
    fig = px.scatter(df, x="rain", y="stability_score", color="city",
                     size="temp_variability", hover_data=["realtime_temp", "realtime_humidity"],
                     title="Relation pluie / stabilité avec taille = variabilité temp.")
    fig.show()

def main():
    
    processing_date = datetime.now()
    date_str = processing_date.strftime("%Y-%m-%d")
    
    # Configuration des chemins
    project_root = Path(__file__).resolve().parent.parent
    data_dir = project_root / "data"
    output_dir = data_dir / "processed" / date_str
    output_file = output_dir / "weather_metrics.csv"
    
    df = pd.read_csv("weather_metrics.csv")

    plot_rain_total(df)
    plot_stability_score(df)
    plot_rain_vs_stability(df)
    plot_temp_vs_humidity(df)
    plot_interactive_scatter(df)

if __name__ == "__main__":
    main()
