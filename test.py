import pandas as pd  

data = {  
    "Pays": ["France", "Allemagne", "États-Unis", "Japon", "Chine", "Inde", "Brésil", "Russie", "Royaume-Uni", "Italie", "Canada", "Espagne", "Australie", "Mexique", "Afrique du Sud", "Égypte", "Nigeria", "Madagascar"],  
    "Capitale": ["Paris", "Berlin", "Washington D.C.", "Tokyo", "Pékin", "New Delhi", "Brasilia", "Moscou", "Londres", "Rome", "Ottawa", "Madrid", "Canberra", "Mexico", "Pretoria", "Le Caire", "Abuja", "Antananarivo"]  
}  

df = pd.DataFrame(data)  
df.to_csv("capitales_pays_populaires.csv", index=False, encoding="utf-8")  