import csv

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
    for key, values in results.items():
        class_satisfied = reducer(key, values)
        print(f'{key}: {class_satisfied}')
