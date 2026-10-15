
from datetime import timedelta
import logging
import subprocess
import sys
from pathlib import Path

import pendulum
from airflow.sdk import dag, task


PROJECT_ROOT = Path("/opt/airflow/project")
PYTHON = sys.executable


def run_script(script_path: str, *args: str) -> None:
    """Ejecuta un script y registra toda su salida, incluso si falla."""
    full_path = PROJECT_ROOT / script_path

    if not full_path.is_file():
        raise FileNotFoundError(f"No existe el script: {full_path}")

    command = [PYTHON, str(full_path), *args]
    logging.info("Ejecutando comando: %s", command)

    try:
        result = subprocess.run(
            command,
            cwd=PROJECT_ROOT,
            check=True,
            capture_output=True,
            text=True,
        )

        if result.stdout:
            logging.info("Salida:\n%s", result.stdout)

        if result.stderr:
            logging.info("Mensajes de stderr:\n%s", result.stderr)

    except subprocess.CalledProcessError as exc:
        logging.error(
            "El script %s terminó con código %s.",
            script_path,
            exc.returncode,
        )

        if exc.stdout:
            logging.error("Salida stdout del script:\n%s", exc.stdout)

        if exc.stderr:
            logging.error("Salida stderr del script:\n%s", exc.stderr)

        raise


@dag(
    dag_id="etl_workshop_pipeline",
    description="Extracción, validación, transformación e integración del ETL Spotify-Grammy.",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "etl",
        "retries": 0,
        "execution_timeout": timedelta(hours=2),
    },
    tags=["etl", "spotify", "grammy", "dimensional-model"],
)
def etl_workshop_pipeline():

    @task
    def extract_spotify():
        run_script("src/extract_spotify.py")

    @task
    def extract_grammy():
        run_script("src/extract_grammy.py")

    @task
    def validate_raw_spotify():
        run_script("gx/raw_validation.py", "--source", "spotify")

    @task
    def validate_raw_grammy():
        run_script("gx/raw_validation.py", "--source", "grammy")

    @task
    def transform_and_integrate():
        run_script("src/transform_integrate.py")
        run_script("src/integrate_artists.py")

    @task
    def validate_prepared():
        run_script("src/prepared_validation.py")

    @task
    def load_data_warehouse():
        run_script("src/load_dw.py")

    spotify_extracted = extract_spotify()
    grammy_extracted = extract_grammy()

    spotify_validated = validate_raw_spotify()
    grammy_validated = validate_raw_grammy()

    transformed = transform_and_integrate()
    prepared_validated = validate_prepared()
    warehouse_loaded = load_data_warehouse()

    spotify_extracted >> spotify_validated
    grammy_extracted >> grammy_validated

    [spotify_validated, grammy_validated] >> transformed
    transformed >> prepared_validated >> warehouse_loaded


etl_workshop_pipeline()