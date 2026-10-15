from pathlib import Path
import os
import pandas as pd
from sqlalchemy import URL, create_engine, text


# ---------------------------------------------------------
# Project paths
# ---------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_PATH = (
    PROJECT_ROOT
    / "data"
    / "the_grammy_awards.csv"
)


# ---------------------------------------------------------
# PostgreSQL connection
# ---------------------------------------------------------

DB_URL = URL.create(
    "postgresql+psycopg2",
    username=os.getenv("ETL_DB_USER", "etl_user"),
    password=os.getenv("ETL_DB_PASSWORD", "etl_password"),
    host=os.getenv("ETL_DB_HOST", "postgres"),
    port=int(os.getenv("ETL_DB_PORT", "5432")),
    database=os.getenv("ETL_DB_NAME", "etl_workshop"),
)

engine = create_engine(DB_URL)


# ---------------------------------------------------------
# Extraction
# ---------------------------------------------------------

def extract_grammy():
    print("Starting Grammy dataset extraction...")

    df = pd.read_csv(
        DATA_PATH,
        sep=";",
        dtype=str
    )

    print(f"Source records: {len(df)}")
    print(f"Source columns: {len(df.columns)}")

    return df


# ---------------------------------------------------------
# RAW load
# ---------------------------------------------------------

def load_raw(df):
    print("Loading data into raw.grammy...")

    if df.empty:
        raise ValueError(
            "El dataset de Grammy está vacío. "
            "Se cancela la carga para proteger raw.grammy."
        )

    engine = create_engine(DB_URL)

    try:
        with engine.begin() as connection:
            connection.execute(
                text("TRUNCATE TABLE raw.grammy")
            )

            df.to_sql(
                "grammy",
                connection,
                schema="raw",
                if_exists="append",
                index=False,
                method="multi"
            )

        print(f"RAW records loaded: {len(df)}")

    finally:
        engine.dispose()


# ---------------------------------------------------------
# Reconciliation
# ---------------------------------------------------------

def reconcile(df):
    engine = create_engine(DB_URL)

    with engine.connect() as connection:
        result = connection.execute(
            text("SELECT COUNT(*) FROM raw.grammy")
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
# Main
# ---------------------------------------------------------

if __name__ == "__main__":
    df = extract_grammy()
    load_raw(df)
    reconcile(df)

    print("Grammy extraction completed successfully.")