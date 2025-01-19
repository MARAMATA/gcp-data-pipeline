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

def validate_file_schema(df, required_columns):
    """Valide si un DataFrame respecte le schéma requis."""
    # Vérifie si toutes les colonnes requises sont présentes
    if not all(col in df.columns for col in required_columns):
        return False
    # Vérifie les types des colonnes
    try:
        df["transaction_id"] = pd.to_numeric(df["transaction_id"], errors="coerce")
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)
        df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d", errors="coerce")
        if df[["transaction_id", "price", "quantity", "date"]].isnull().any().any():
            return False
    except Exception as e:
        logging.error(f"Erreur lors de la validation du schéma : {e}")
        return False
    return True

def process_file(file_path):
    """Valide et traite un fichier, en le classant dans clean/ ou error/."""
    local_file = os.path.basename(file_path)
    download_file(file_path, local_file)

    try:
        # Lire le fichier
        df = pd.read_csv(local_file)
        
        # Schéma requis
        required_columns = [
            "transaction_id", "product_name", "category", "price", "quantity", "date",
            "customer_name", "customer_email"
        ]
        
        # Valider tout le fichier
        if not validate_file_schema(df, required_columns):
            raise ValueError("Le fichier contient des lignes non conformes au schéma.")
        
        # Si tout est valide, déplacer vers clean/
        cleaned_file = local_file.replace(".csv", "_cleaned.csv")
        df.to_csv(cleaned_file, index=False)
        upload_file(cleaned_file, f"clean/{cleaned_file}")
        logging.info(f"Fichier nettoyé et uploadé : {cleaned_file}")
        os.remove(cleaned_file)
    
    except Exception as e:
        logging.error(f"Erreur de validation/cleaning : {e}")
        # Déplacer tout le fichier dans error/ en cas d'erreur
        upload_file(local_file, f"error/{local_file}")
    
    finally:
        # Nettoyer les fichiers locaux
        if os.path.exists(local_file):
            os.remove(local_file)

def process_files():
    """Traite tous les fichiers du dossier input/."""
    files = list_files_in_folder("input/")
    for file_path in files:
        process_file(file_path)

if __name__ == "__main__":
    process_files()

