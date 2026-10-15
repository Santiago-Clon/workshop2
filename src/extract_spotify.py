from pathlib import Path
import os
import pandas as pd
from sqlalchemy import URL, create_engine, text


# ---------------------------------------------------------
# Project paths
# ---------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_PATH = PROJECT_ROOT / "data" / "spotify_dataset.csv"

DB_URL = URL.create(
    drivername="postgresql+psycopg2",
    username=os.getenv("ETL_DB_USER", "etl_user"),
    password=os.getenv("ETL_DB_PASSWORD", "etl_password"),
    host=os.getenv("ETL_DB_HOST", "postgres"),
    port=int(os.getenv("ETL_DB_PORT", "5432")),
    database=os.getenv("ETL_DB_NAME", "etl_workshop"),
)


# ---------------------------------------------------------
# Extract Spotify dataset
# ---------------------------------------------------------

def extract_spotify():
    print("Starting Spotify dataset extraction...")

    df = pd.read_csv(
        DATA_PATH,
        dtype=str
    )

    df = df.rename(
        columns={"Unnamed: 0": "Column1"}
    )

    print(f"Source records: {len(df)}")
    print(f"Source columns: {len(df.columns)}")

    return df


# ---------------------------------------------------------
# Load RAW layer
# ---------------------------------------------------------

def load_raw(df):
    print("Loading data into raw.spotify...")

    if df.empty:
        raise ValueError(
            "El dataset de Spotify está vacío. "
            "Se cancela la carga para proteger raw.spotify."
        )

    engine = create_engine(DB_URL)

    try:
        with engine.begin() as connection:
            connection.execute(
                text("TRUNCATE TABLE raw.spotify")
            )

            df.to_sql(
                "spotify",
                connection,
                schema="raw",
                if_exists="append",
                index=False,
                chunksize=5000
            )

        print(f"RAW records loaded: {len(df)}")

    finally:
        engine.dispose()

# ---------------------------------------------------------
# Reconcile source and RAW counts
# ---------------------------------------------------------

def reconcile(df):
    engine = create_engine(DB_URL)

    with engine.connect() as connection:
        result = connection.execute(
            text("SELECT COUNT(*) FROM raw.spotify")
        )

        raw_count = result.scalar()

    source_count = len(df)

    print(f"Source count: {source_count}")
    print(f"RAW count: {raw_count}")

    if source_count != raw_count:
        raise RuntimeError(
            "Record count mismatch between source and RAW."
        )

    print("Reconciliation successful.")


# ---------------------------------------------------------
# Main execution
# ---------------------------------------------------------

if __name__ == "__main__":
    df = extract_spotify()
    load_raw(df)
    reconcile(df)

    print("Spotify extraction completed successfully.")