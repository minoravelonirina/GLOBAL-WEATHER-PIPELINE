# from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey
# from sqlalchemy.ext.declarative import declarative_base
# from sqlalchemy.orm import sessionmaker
# import pandas as pd
# from datetime import datetime
# from dotenv import load_dotenv
# import os


# load_dotenv()

# db_user = os.environ['DB_USER']
# pwd = os.environ['DB_PASSWORD']
# host = os.environ['DB_HOST']
# port = os.environ['DB_PORT']
# db_name = os.environ['DB_NAME']

# # 1. Modélisation des tables avec SQLAlchemy
# Base = declarative_base()

# # 2. Connexion à la base de données
# db_url = f"postgresql://{db_user}:{pwd}@{host}:{port}/{db_name}"
# engine = create_engine(db_url)

# class DimCity(Base):
#     """Table de dimension pour les villes."""
#     __tablename__ = 'dim_city'
#     city_id = Column(Integer, primary_key=True)
#     city_name = Column(String(50), unique=True)
#     country = Column(String(50))
#     latitude = Column(Float)
#     longitude = Column(Float)

# class FactWeather(Base):
#     """Table de fait pour les données météo."""
#     __tablename__ = 'fact_weather'
#     id = Column(Integer, primary_key=True)
#     city_id = Column(Integer, ForeignKey('dim_city.city_id'))
#     timestamp = Column(DateTime, default=datetime.utcnow)
#     temperature = Column(Float)
#     humidity = Column(Float)
#     air_quality = Column(Integer)


# Base.metadata.create_all(engine)  # Crée les tables si elles n'existent pas

# # 3. Fonction pour sauvegarder des données (via Pandas)
# def save_to_db(df):
#     """
#     Sauvegarde un DataFrame dans la table fact_weather.
#     Args:
#         df (pd.DataFrame): Doit contenir les colonnes 'city_id', 'temperature', 'humidity', 'air_quality'.
#     """
#     df.to_sql(
#         name="fact_weather",
#         con=engine,
#         if_exists="append",
#         index=False,
#         method="multi"  # Optimise l'insertion de plusieurs lignes
#     )
#     print(f"Données sauvegardées dans fact_weather : {len(df)} lignes ajoutées.")
#     save_to_db(df)


# # 4. Exemple d'utilisation
# # if __name__ == "__main__":
#     # Créer un exemple de DataFrame
#     # sample_data = pd.DataFrame({
#     #     "city_id": [1, 2],
#     #     "temperature": [25.3, 18.7],
#     #     "humidity": [45, 82],
#     #     "air_quality": [12, 23]
#     # })
    
#     # Sauvegarder dans la base