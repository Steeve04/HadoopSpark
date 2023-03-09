import csv

# Fonction Mapper
def mapper(row):
    # Extraire la satisfaction du passager et la distance parcourue
    satisfaction = row[24]
    distance = int(row[7])
    # Mapper la ligne sur la clé de satisfaction et la distance parcourue
    if satisfaction == "satisfied":
        key = "satisfied"
    elif satisfaction == "neutral or dissatisfied":
        key = "neutral or dissatisfied"
    return (key, distance)

# Fonction Reducer
def reducer(key, values):
    # Calculer la moyenne de la distance parcourue pour chaque groupe
    count = 0
    total_distance = 0
    for distance in values:
        count += 1
        total_distance += distance
    average_distance = total_distance / count
    return(key, average_distance)

# Ouvrir le fichier CSV
with open("test.csv", "r") as f:
    # Ignorer la première ligne (en-tête)
    next(f)
    # Initialiser le dictionnaire des résultats
    results = {"satisfied": [], "neutral or dissatisfied": []}
    # Mapper chaque ligne et stocker les résultats dans le dictionnaire approprié
    for row in csv.reader(f):
        key, distance = mapper(row)
        results[key].append(distance)
    # Réduire les résultats en calculant la moyenne pour chaque groupe
    average_distance_l = []
    for key, values in results.items():
        average_distance = reducer(key, values)
        average_distance_l.append(average_distance)
