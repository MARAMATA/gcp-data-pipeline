import os
from google.cloud import bigquery, storage
import logging

# Configuration du logging
logging.basicConfig(level=logging.INFO)

# Configuration des clients GCP
bq_client = bigquery.Client()
storage_client = storage.Client()

bucket_name = "m2dsia-maramata-diop-data"
dataset_id = "dataset_maramata_diop"  # Nom du dataset sans le project_id
table_id = f"{bq_client.project}.{dataset_id}.transactions"  # Table avec project_id.dataset_id.table_name
bucket = storage_client.bucket(bucket_name)

def list_clean_files():
    """Liste les fichiers dans le dossier clean/ en excluant .placeholder."""
    return [
        blob.name for blob in storage_client.list_blobs(bucket_name, prefix="clean/")
        if not blob.name.endswith("/") and not blob.name.endswith(".placeholder")
    ]

def load_to_bigquery(file_path):
    """Charge un fichier dans BigQuery."""
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        schema=[
            bigquery.SchemaField("transaction_id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("product_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("category", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("price", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("quantity", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("customer_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("customer_email", "STRING", mode="NULLABLE"),
        ],
        write_disposition="WRITE_APPEND"  # Ajout des données sans écraser
    )
    uri = f"gs://{bucket_name}/{file_path}"
    load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result()  # Attendre la fin du job
    logging.info(f"Données chargées dans BigQuery depuis : {uri}")

def move_to_done(file_path):
    """Déplace un fichier vers le dossier done/."""
    source_blob = bucket.blob(file_path)
    new_blob = bucket.blob(file_path.replace("clean", "done"))
    bucket.copy_blob(source_blob, bucket, new_blob.name)
    source_blob.delete()
    logging.info(f"Fichier déplacé : {file_path} -> {new_blob.name}")

def process_clean_files():
    """Charge les fichiers nettoyés et les déplace vers done/."""
    files = list_clean_files()
    for file_path in files:
        try:
            load_to_bigquery(file_path)
            move_to_done(file_path)
        except Exception as e:
            logging.error(f"Erreur lors du traitement de {file_path} : {e}")

if __name__ == "__main__":
    process_clean_files()



