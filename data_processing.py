import os
import pandas as pd
from google.cloud import storage
import logging

# Configuration du logging
logging.basicConfig(level=logging.INFO)

# Initialiser le client GCS
client = storage.Client()
bucket_name = "m2dsia-maramata-diop-data"
bucket = client.bucket(bucket_name)

def list_files_in_folder(folder):
    """Liste les fichiers dans un dossier GCS."""
    return [blob.name for blob in client.list_blobs(bucket_name, prefix=folder) if not blob.name.endswith("/")]

def download_file(gcs_path, local_path):
    """Télécharge un fichier de GCS vers le local."""
    blob = bucket.blob(gcs_path)
    blob.download_to_filename(local_path)
    logging.info(f"Téléchargé : {gcs_path} -> {local_path}")

def upload_file(local_path, gcs_path):
    """Télécharge un fichier local vers GCS."""
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_path)
    logging.info(f"Uploadé : {local_path} -> {gcs_path}")

def validate_and_clean_data(local_file):
    """Valide et nettoie les données."""
    try:
        df = pd.read_csv(local_file)
        
        # Validation des colonnes requises
        required_columns = [
            "transaction_id", "product_name", "category", "price", "quantity", "date",
            "customer_name", "customer_email"
        ]
        if not all(col in df.columns for col in required_columns):
            raise ValueError("Colonnes manquantes dans le fichier d'entrée.")

        # Nettoyage des données
        valid_data = df.copy()
        invalid_data = pd.DataFrame()

        # Identifiez les lignes invalides (ex : prix ou quantité non valides)
        valid_data["price"] = pd.to_numeric(valid_data["price"], errors="coerce")
        valid_data["quantity"] = pd.to_numeric(valid_data["quantity"], errors="coerce")
        valid_data = valid_data.dropna(subset=["price", "quantity"])  # Garder les lignes valides

        # Lignes rejetées
        invalid_data = df[~df.index.isin(valid_data.index)]
        
        # Générer des noms de fichiers spécifiques
        cleaned_file = local_file.replace(".csv", "_cleaned.csv")
        error_file = local_file.replace(".csv", "_errors.csv")
        
        # Enregistrer les fichiers nettoyés et rejetés
        valid_data.to_csv(cleaned_file, index=False)
        invalid_data.to_csv(error_file, index=False)
        
        logging.info(f"Fichier nettoyé : {cleaned_file}")
        logging.info(f"Fichier d'erreurs : {error_file}")
        
        return cleaned_file, error_file
    except Exception as e:
        logging.error(f"Erreur de validation/cleaning : {e}")
        return None, None

def process_files():
    """Traite les fichiers du dossier input/."""
    files = list_files_in_folder("input/")
    for file_path in files:
        local_input_path = os.path.basename(file_path)
        download_file(file_path, local_input_path)

        cleaned_file, error_file = validate_and_clean_data(local_input_path)
        
        if cleaned_file:
            upload_file(cleaned_file, f"clean/{os.path.basename(cleaned_file)}")
        if error_file:
            upload_file(error_file, f"error/{os.path.basename(error_file)}")

        # Suppression des fichiers locaux
        os.remove(local_input_path)
        if cleaned_file and os.path.exists(cleaned_file):
            os.remove(cleaned_file)
        if error_file and os.path.exists(error_file):
            os.remove(error_file)

if __name__ == "__main__":
    process_files()
