import os
import pandas as pd
from google.cloud import storage
import logging

# Configuration du logging
logging.basicConfig(level=logging.INFO)

# Initialisation des clients GCS
storage_client = storage.Client()
bucket_name = "m2dsia-maramata-diop-data"
bucket = storage_client.bucket(bucket_name)

def list_files_in_folder(folder):
    """Liste les fichiers dans un dossier GCS."""
    return [blob.name for blob in storage_client.list_blobs(bucket_name, prefix=folder) if not blob.name.endswith("/")]

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
        required_columns = [
            "transaction_id", "product_name", "category", "price", "quantity", "date",
            "customer_name", "customer_email"
        ]
        if not all(col in df.columns for col in required_columns):
            raise ValueError("Colonnes manquantes")

        df["product_name"] = df["product_name"].fillna("Unknown Product")
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
        df = df.dropna()
        cleaned_file = local_file.replace(".csv", "_cleaned.csv")
        df.to_csv(cleaned_file, index=False)
        logging.info(f"Fichier nettoyé : {cleaned_file}")
        return cleaned_file, None
    except Exception as e:
        logging.error(f"Erreur de validation/cleaning : {e}")
        return None, str(e)

def process_files():
    """Traite les fichiers du dossier input/."""
    files = list_files_in_folder("input/")
    for file_path in files:
        local_input_path = os.path.basename(file_path)
        download_file(file_path, local_input_path)

        cleaned_file, error = validate_and_clean_data(local_input_path)
        if cleaned_file:
            upload_file(cleaned_file, f"clean/{os.path.basename(cleaned_file)}")
        else:
            upload_file(local_input_path, f"error/{os.path.basename(local_input_path)}")

        os.remove(local_input_path)
        if cleaned_file:
            os.remove(cleaned_file)

if __name__ == "__main__":
    process_files()
