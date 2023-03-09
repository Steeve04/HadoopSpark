import csv

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
    for key, values in results.items():
        age_satisfied = reducer(key, values)
        print(f'{key}: {age_satisfied}')
