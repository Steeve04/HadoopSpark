import csv
import pandas as pd
from pymongo import MongoClient

def mapper(row):
    # Vérifier si la satisfaction est "satisfied" par classe des clients
    if row['satisfaction'] == 'satisfied':
        if row['Class'] == 'Business':
            yield ('Business', 1)
        elif row['Class'] == 'Eco':
            yield ('Eco', 1)
        elif row['Class'] == 'Eco Plus':
            yield ('Eco Plus', 1)

def reducer(key, values):
    # Initialiser le compteur à 0
    count = 0
    # Compter le nombre de lignes pour la colonne classe
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

    # Réduire les résultats et afficher le nombre de lignes pour chaque classe
    class_satisfied_l = []
    for key, values in results.items():
        class_satisfied = reducer(key, values)
        class_satisfied_l.append(class_satisfied)

    # Stockage sous format liste de dictionnaire
    class_dict = [{item[0]: str(item[1])} for item in class_satisfied_l]
    # Connexion à la base de MongoDB avec user='test', password='pwd', cluster='cluster.1t7qkeq'
    client = MongoClient("mongodb+srv://test:pwd@cluster.1t7qkeq.mongodb.net/?retryWrites=true&w=majority")

    # Affectation de db à la base
    db = client['Satisfaction']

    # Affectation à la collection
    class_collection = db['Classe']

    # Insertion de la liste de dictionnaire dans la collection de la base de donnée MongoDB
    class_collection.insert_many(class_dict)

    # Récupération des données sous forme de liste de dictionnaires
    data_coll = list(class_collection.find())

    # Transformation des données en DataFrame
    df_class = pd.DataFrame(data_coll)

    # Pivotage des colonnes de classes
    df_class = df_class.melt(id_vars=['_id'], value_vars=['Eco', 'Business', 'Eco Plus'], var_name='Classe', value_name='Nb_satisfaction')
    
    # Garde seulement les cloonnes nécessaires
    df_class = df_class[['Classe', 'Nb_satisfaction']]
    
    # Suppression des valeurs NaN
    df_class = df_class.dropna()
    
    # Déconnexion à la base
    client.close()
