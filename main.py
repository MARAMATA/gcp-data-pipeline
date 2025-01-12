import subprocess
import sys
import os
import logging

# Configuration du logging
logging.basicConfig(level=logging.INFO)

def run_script(script_name):
    """Exécute un script Python avec un chemin explicite."""
    try:
        script_path = os.path.abspath(script_name)
        logging.info(f"Exécution de {script_name} ({script_path})...")
        subprocess.run([sys.executable, script_path], check=True)
        logging.info(f"{script_name} terminé avec succès.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Erreur lors de l'exécution de {script_name}: {e}")
        exit(1)

def main():
    # Étape 1 : Nettoyage des données
    run_script("data_processing.py")

    # Étape 2 : Chargement des données nettoyées dans BigQuery
    run_script("data_loader.py")

if __name__ == "__main__":
    main()


