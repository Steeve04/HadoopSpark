import csv
import pandas as pd
from pymongo import MongoClient

def mapper(row):
    # Vérifier si la satisfaction est "satisfied" et si les colonnes critères sont égales à 5
    if row['satisfaction'] == 'satisfied':
        if row['Inflight wifi service'] == '5':
            yield ('Inflight wifi service', 1)
        elif row['Departure/Arrival time convenient'] == '5':
            yield ('Departure/Arrival time convenient', 1)
        elif row['Ease of Online booking'] == '5':
            yield ('Ease of Online booking', 1)
        elif row['Gate location'] == '5':
            yield ('Gate location', 1)
        elif row['Food and drink'] == '5':
            yield ('Food and drink', 1)
        elif row['Online boarding'] == '5':
            yield ('Online boarding', 1)
        elif row['Seat comfort'] == '5':
            yield ('Seat comfort', 1)
        elif row['Inflight entertainment'] == '5':
            yield ('Inflight entertainment', 1)
        elif row['On-board service'] == '5':
            yield ('On-board service', 1)
        elif row['Leg room service'] == '5':
            yield ('Leg room service', 1)
        elif row['Baggage handling'] == '5':
            yield ('Baggage handling', 1)
        elif row['Checkin service'] == '5':
            yield ('Checkin service', 1)
        elif row['Inflight service'] == '5':
            yield ('Inflight service', 1)
        elif row['Cleanliness'] == '5':
            yield ('Cleanliness', 1)

def reducer(key, values):
    # Initialiser le compteur à 0
    count = 0
    # Compter le nombre de lignes pour la colonne critère
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

    # Réduire les résultats et afficher le nombre de lignes pour chaque colonne critère
    criteria_count_satisfied_l = []
    for key, values in results.items():
        criteria_count_satisfied = reducer(key, values)
        criteria_count_satisfied_l.append(criteria_count_satisfied)

    # Stockage sous format liste de dictionnaire
    criteria_count_dict = [{item[0]: str(item[1])} for item in criteria_count_satisfied_l]

    # Connexion à la base de MongoDB avec user='test', password='pwd', cluster='cluster.1t7qkeq'
    client = MongoClient("mongodb+srv://test:pwd@cluster.1t7qkeq.mongodb.net/?retryWrites=true&w=majority")

    # Affectation de db à la base
    db = client['Satisfaction']

    # Affectation à la collection
    criteria_count_collection = db['Critere']

    # Insertion de la liste de dictionnaire dans la collection de la base de donnée MongoDB
    criteria_count_collection.insert_many(criteria_count_dict)

    # Récupération des données sous forme de liste de dictionnaires
    data_coll = list(criteria_count_collection.find())

    # Transformation des données en DataFrame
    df_criteria_count = pd.DataFrame(data_coll)

    # Pivotage des colonnes satisfied et neutral or dissatisfied en Satisfaction
    df_criteria_count = df_criteria_count.melt(id_vars=['_id'], value_vars=['Inflight wifi service', 'Food and drink', 'Seat comfort', 
                                                                            'Checkin service', 'Leg room service', 'On-board service',
                                                                            'Inflight service', 'Departure/Arrival time convenient',
                                                                            'Online boarding', 'Ease of Online booking', 'Inflight entertainment',
                                                                            'Cleanliness', 'Gate location', 'Baggage handling'], var_name='Critere', value_name='Nb_satisfaction')
    
    # Garde seulement les colonnes nécessaires
    df_criteria_count = df_criteria_count[['Critere', 'Nb_satisfaction']]
    
    # Suppression des valeurs NaN
    df_criteria_count = df_criteria_count.dropna()
    
    # Déconnexion à la base
    client.close()
