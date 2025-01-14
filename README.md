# Pipeline de traitement de données avec Google Cloud Platform (GCP)

## Description
Ce projet met en œuvre un pipeline automatisé pour :
- Valider et nettoyer les fichiers CSV.
- Charger les données nettoyées dans une table BigQuery.
- Organiser les fichiers dans des dossiers spécifiques sur Google Cloud Storage.

## Structure des dossiers
- **input/** : Contient les fichiers bruts.
- **clean/** : Contient les fichiers nettoyés.
- **done/** : Contient les fichiers traités et chargés dans BigQuery.
- **error/** : Contient les fichiers contenant des erreurs.

## Fichiers du projet
- **data_processing.py** : Nettoie et valide les données.
- **data_loader.py** : Charge les fichiers nettoyés dans BigQuery.
- **main.py** : Orchestre l'ensemble du pipeline.
- **schema.json** : Définit le schéma BigQuery.
- **requirements.txt** : Liste des dépendances Python.

## Instructions
1. **Configurer votre environnement GCP** :
   - Authentifiez-vous avec :
     ```bash
     gcloud auth application-default login
     ```
   - Configurez votre projet :
     ```bash
     gcloud config set project <PROJECT_ID>
     ```

2. **Préparer l'environnement Python** :
   - Créez un environnement virtuel et installez les dépendances :
     ```bash
     python -m venv env
     source env/bin/activate  # Sur Windows : env\Scripts\activate
     pip install -r requirements.txt
     ```

3. **Exécuter le pipeline** :
   - Placez vos fichiers CSV dans `input/` sur votre bucket Cloud Storage.
   - Lancez le script principal :
     ```bash
     python main.py
     ```

4. **Résultats** :
   - Les fichiers nettoyés seront chargés dans BigQuery.
   - Les fichiers traités seront déplacés dans le dossier `done/`.

## Auteur
- **Maramata Diop**







