import csv
import pandas as pd
from pymongo import MongoClient

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

    # Stockage sous format liste de dictionnaire
    avg_dist_dict = [{item[0]: str(item[1])} for item in average_distance_l]

    # Connexion à la base de MongoDB avec user='test', password='pwd', cluster='cluster.1t7qkeq'
    client = MongoClient("mongodb+srv://test:pwd@cluster.1t7qkeq.mongodb.net/?retryWrites=true&w=majority")

    # Affectation de db à la base
    db = client['Satisfaction']

    # Affectation à la collection
    avg_dist_collection = db['Distance']

    # Insertion de la liste de dictionnaire dans la collection de la base de donnée MongoDB
    avg_dist_collection.insert_many(avg_dist_dict)

    # Récupération des données sous forme de liste de dictionnaires
    data_coll = list(avg_dist_collection.find())

    # Transformation des données en DataFrame
    df_avg_dist = pd.DataFrame(data_coll)

    # Pivotage des colonnes satisfied et neutral or dissatisfied en Satisfaction
    df_avg_dist = df_avg_dist.melt(id_vars=['_id'], value_vars=['satisfied', 'neutral or dissatisfied'], var_name='Satisfaction', value_name='Distance_moyenne')
    
    # Garde seulement les colonnes nécessaires
    df_avg_dist = df_avg_dist[['Satisfaction', 'Distance_moyenne']]
    
    # Suppression des valeurs NaN
    df_avg_dist = df_avg_dist.dropna()
    
    # Déconnexion à la base
    client.close()
