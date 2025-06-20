import numpy as np

def calculate_variability(temperature_series):
    """Calcule l'écart-type des températures (variabilité)."""
    return np.std(temperature_series)

def calculate_stability(variability_score):
    """Convertit la variabilité en score de stabilité (0-1)."""
    return 1 / (1 + variability_score)