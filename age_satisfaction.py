import csv
import pandas as pd
from pymongo import MongoClient

def mapper(row):
    # Vérifier si la satisfaction est "satisfied" et groupé par les tranches d'âges
    if row['satisfaction'] == 'satisfied':
        if int(row['Age']) > 0 and int(row['Age']) <= 10:
            yield ('[0-10]', 1)
        elif int(row['Age']) > 10 and int(row['Age']) < 18:
            yield ('[11-17]', 1)
        elif int(row['Age']) >= 18 and int(row['Age']) < 35:
            yield ('[18-35]', 1)
        elif int(row['Age']) >= 35 and int(row['Age']) <= 50:
            yield ('[35-50]', 1)
        elif int(row['Age']) > 50:
            yield ('[>50]', 1)

def reducer(key, values):
    # Initialiser le compteur à 0
    count = 0
    # Compter le nombre de lignes pour la tranche d'âge
    for value in values:
        count += value
    # Retourner le résultat sous forme de tuple (clé, nombre de lignes)
    return(key, count)

# Ouvrir le fichier CSV
with open('test.csv', 'r') as csvfile:
    # Créer un lecteur CSV
    reader = csv.DictReader(csvfile)

    # Initialiser un dictionnaire pour stocker les résultats
    results = {}

    # Mapper chaque ligne et regrouper les résultats par clé
    for row in reader:
        for key, value in mapper(row):
            if key not in results:
                results[key] = []
            results[key].append(value)

    # Réduire les résultats et afficher le nombre de lignes pour chaque tranche d'âge
    age_satisfied_l = []
    for key, values in results.items():
        age_satisfied = reducer(key, values)
        age_satisfied_l.append(age_satisfied)

     # Stockage sous format liste de dictionnaire
    age_dict = [{item[0]: str(item[1])} for item in age_satisfied_l]
    # Connexion à la base de MongoDB avec user='test', password='pwd', cluster='cluster.1t7qkeq'
    client = MongoClient("mongodb+srv://test:pwd@cluster.1t7qkeq.mongodb.net/?retryWrites=true&w=majority")

    # Affectation de db à la base
    db = client['Satisfaction']

    # Affectation à la collection
    age_collection = db['Age']

    # Insertion de la liste de dictionnaire dans la collection de la base de donnée MongoDB
    age_collection.insert_many(age_dict)

    # Récupération des données sous forme de liste de dictionnaires
    data_coll = list(age_collection.find())

    # Transformation des données en DataFrame
    df_age = pd.DataFrame(data_coll)

    # Pivotage des colonnes de tranche d'âges en Age
    df_age = df_age.melt(id_vars=['_id'], value_vars=['[>50]', '[35-50]', '[11-17]', '[18-35]', '[0-10]'], var_name='Age', value_name='Nb_satisfaction')
    
    # Garde seulement les cloonnes nécessaires
    df_age = df_age[['Age', 'Nb_satisfaction']]
    
    # Suppression des valeurs NaN
    df_age = df_age.dropna()
    
    # Déconnexion à la base
    client.close()
