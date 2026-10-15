
from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL


# =========================================================
# Project configuration
# =========================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

DB_HOST = os.getenv("ETL_DB_HOST", "localhost")
DB_PORT = int(os.getenv("ETL_DB_PORT", "5433"))
DB_NAME = os.getenv("ETL_DB_NAME", "etl_workshop")
DB_USER = os.getenv("ETL_DB_USER", "etl_user")
DB_PASSWORD = os.getenv("ETL_DB_PASSWORD", "etl_password")

PREPARED_SCHEMA = "prepared"

engine = create_engine(
    URL.create(
        drivername="postgresql+psycopg2",
        username=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
    ),
    pool_pre_ping=True,
)


# =========================================================
# Shared helpers
# =========================================================

def clean_text(series: pd.Series) -> pd.Series:
    """Trim strings and convert empty strings to nulls."""
    result = series.astype("string").str.strip()
    return result.mask(result.eq(""))


def normalized_text(series: pd.Series) -> pd.Series:
    """Create a normalized comparison key without replacing the label."""
    return clean_text(series).str.casefold()


def numeric(series: pd.Series) -> pd.Series:
    """Convert numeric strings to numbers; invalid values become null."""
    return pd.to_numeric(series, errors="coerce")


def count_conversion_failures(
    original: pd.Series,
    converted: pd.Series,
) -> int:
    """Count non-null source values that failed conversion."""
    source_present = clean_text(original).notna()
    return int((source_present & converted.isna()).sum())


def record_exception(
    report: dict,
    rule_id: str,
    source: str,
    description: str,
    count: int,
    severity: str = "WARNING",
) -> None:
    if count:
        report["exceptions"].append(
            {
                "rule_id": rule_id,
                "source": source,
                "description": description,
                "count": int(count),
                "severity": severity,
            }
        )


def publish_dataframe(
    connection,
    dataframe: pd.DataFrame,
    table_name: str,
) -> None:
    """
    Write to a staging table, then replace the target table.
    Called within a single database transaction.
    """
    staging_name = f"{table_name}_staging"

    dataframe.to_sql(
        name=staging_name,
        con=connection,
        schema=PREPARED_SCHEMA,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=2000,
    )

    connection.execute(
        text(
            f'DROP TABLE IF EXISTS "{PREPARED_SCHEMA}"."{table_name}"'
        )
    )

    connection.execute(
        text(
            f'ALTER TABLE "{PREPARED_SCHEMA}"."{staging_name}" '
            f'RENAME TO "{table_name}"'
        )
    )


# =========================================================
# Spotify transformation
# =========================================================

def transform_spotify(
    source: pd.DataFrame,
    report: dict,
) -> pd.DataFrame:

    required = {
        "Column1", "track_id", "artists", "album_name",
        "track_name", "popularity", "duration_ms", "explicit",
        "danceability", "energy", "key", "loudness", "mode",
        "speechiness", "acousticness", "instrumentalness",
        "liveness", "valence", "tempo", "time_signature",
        "track_genre",
    }

    missing_columns = required - set(source.columns)
    if missing_columns:
        raise ValueError(
            f"Spotify is missing required columns: {sorted(missing_columns)}"
        )

    df = source.copy()
    input_rows = len(df)

    # Preserve source-row lineage, but do not use it as a business key.
    df["source_row_id"] = numeric(df["Column1"]).astype("Int64")

    text_columns = [
        "track_id", "artists", "album_name",
        "track_name", "track_genre",
    ]

    for column in text_columns:
        df[column] = clean_text(df[column])

    # The agreed grain is one observation per track and genre.
    missing_key = df["track_id"].isna() | df["track_genre"].isna()
    missing_key_count = int(missing_key.sum())

    record_exception(
        report,
        "R-SPT-KEY-01",
        "spotify",
        "Rows with missing track_id or track_genre; transformation stops "
        "to avoid silently dropping business-key records.",
        missing_key_count,
        severity="CRITICAL",
    )

    if missing_key_count:
        raise ValueError(
            f"Spotify contains {missing_key_count} rows with missing "
            "track_id or track_genre."
        )

    # Deterministic duplicate handling: select the smallest source row ID.
    # Prior profiling found 444 repeated key combinations and 450 extra rows.
    df["_source_order"] = df["source_row_id"].fillna(
        pd.Series(range(len(df)), index=df.index)
    )

    df = df.sort_values(
        ["track_id", "track_genre", "_source_order"],
        kind="stable",
    )

    duplicate_groups = int(
        df.loc[
            df.duplicated(["track_id", "track_genre"], keep=False),
            ["track_id", "track_genre"],
        ].drop_duplicates().shape[0]
    )

    duplicate_extra_rows = int(
        df.duplicated(["track_id", "track_genre"], keep="first").sum()
    )

    record_exception(
        report,
        "R-SPT-01",
        "spotify",
        "Additional observations removed for repeated "
        "track_id + track_genre combinations.",
        duplicate_extra_rows,
        severity="INFO",
    )

    df = df.drop_duplicates(
        subset=["track_id", "track_genre"],
        keep="first",
    ).copy()

    # Parse numeric fields.
    integer_columns = [
        "popularity", "duration_ms", "key",
        "mode", "time_signature",
    ]

    for column in integer_columns:
        original = df[column].copy()
        converted = numeric(original)

        record_exception(
            report,
            f"R-SPT-NUM-{column.upper()}",
            "spotify",
            f"Values in {column} that could not be converted to numeric.",
            count_conversion_failures(original, converted),
        )

        df[column] = converted.round().astype("Int64")

    float_columns = [
        "danceability", "energy", "loudness", "speechiness",
        "acousticness", "instrumentalness", "liveness",
        "valence", "tempo",
    ]

    for column in float_columns:
        original = df[column].copy()
        converted = numeric(original)

        record_exception(
            report,
            f"R-SPT-NUM-{column.upper()}",
            "spotify",
            f"Values in {column} that could not be converted to numeric.",
            count_conversion_failures(original, converted),
        )

        df[column] = converted.astype("Float64")

   # Spotify source attributes already use their standard scales.
    # Do not divide these attributes by 1000.
    # Loudness remains in decibels (dB), and tempo remains in BPM.

    # Instrumentalness mixes normalized and x1000 representations.
    df["instrumentalness"] = df["instrumentalness"].map(
        lambda value: (
            pd.NA if pd.isna(value)
            else value / 1000 if value > 1
            else value
        )
    ).astype("Float64")

    # instrumentalness mixes normalized and x1000 representations.
    df["instrumentalness"] = df["instrumentalness"].map(
        lambda value: (
            pd.NA if pd.isna(value)
            else value / 1000 if value > 1
            else value
        )
    ).astype("Float64")

    # Boolean mapping is explicit; unknown values are not guessed.
    explicit_original = df["explicit"].copy()
    explicit_map = {
        "true": True,
        "verdadero": True,
        "false": False,
        "falso": False,
    }

    explicit_clean = clean_text(explicit_original).str.casefold()
    df["explicit"] = explicit_clean.map(explicit_map).astype("boolean")

    explicit_invalid = int(
        (explicit_clean.notna() & df["explicit"].isna()).sum()
    )

    record_exception(
        report,
        "R-SPT-03",
        "spotify",
        "Unrecognized explicit values; represented as null.",
        explicit_invalid,
    )

    # Out-of-domain values are retained as exceptions and nulled.
    bounded_columns = [
        "danceability", "energy", "speechiness",
        "acousticness", "instrumentalness", "liveness", "valence",
    ]

    for column in bounded_columns:
        invalid = df[column].notna() & (
            (df[column] < 0) | (df[column] > 1)
        )
        invalid_count = int(invalid.sum())

        record_exception(
            report,
            f"R-SPT-RANGE-{column.upper()}",
            "spotify",
            f"{column} outside the valid [0, 1] interval; set to null.",
            invalid_count,
            severity="WARNING",
        )

        df.loc[invalid, column] = pd.NA

    zero_tempo = df["tempo"].eq(0).fillna(False)
    zero_tempo_count = int(zero_tempo.sum())

    record_exception(
        report,
        "R-SPT-04D",
        "spotify",
        "Tempo equal to zero is treated as unknown and set to null.",
        zero_tempo_count,
    )

    df.loc[zero_tempo, "tempo"] = pd.NA

    # Preserve original descriptive labels and add normalized match keys.
    df["track_name_norm"] = normalized_text(df["track_name"])
    df["album_name_norm"] = normalized_text(df["album_name"])
    df["artists_norm"] = normalized_text(df["artists"])

    # Rename fields to stable analytical names.
    df = df.rename(
        columns={
            "artists": "artists",
            "track_name": "track_name",
            "album_name": "album_name",
            "track_genre": "track_genre",
        }
    )

    # Remove technical source index and internal sort field.
    df = df.drop(columns=["Column1", "_source_order"])

    report["sources"]["spotify"] = {
        "input_rows": input_rows,
        "output_rows": len(df),
        "duplicate_key_groups": duplicate_groups,
        "duplicate_extra_rows_removed": duplicate_extra_rows,
        "distinct_track_ids": int(df["track_id"].nunique()),
        "distinct_genres": int(df["track_genre"].nunique()),
        "grain": "one row per track_id + track_genre",
    }

    return df


# =========================================================
# Grammy transformation
# =========================================================

def transform_grammy(
    source: pd.DataFrame,
    report: dict,
) -> pd.DataFrame:

    required = {
        "year", "title", "published_at", "updated_at",
        "category", "nominee", "artist", "workers", "img", "winner",
    }

    missing_columns = required - set(source.columns)
    if missing_columns:
        raise ValueError(
            f"Grammy is missing required columns: {sorted(missing_columns)}"
        )

    df = source.copy()
    input_rows = len(df)

    text_columns = [
        "title", "category", "nominee",
        "artist", "workers", "img", "winner",
    ]

    for column in text_columns:
        df[column] = clean_text(df[column])

    # Event year is different from source publication/update timestamps.
    year_original = df["year"].copy()
    year_numeric = numeric(year_original)

    year_failures = count_conversion_failures(
        year_original, year_numeric
    )

    record_exception(
        report,
        "R-GRM-YEAR-01",
        "grammy",
        "Years that could not be converted to integers.",
        year_failures,
        severity="CRITICAL",
    )

    if year_failures or year_numeric.isna().any():
        raise ValueError("Grammy contains invalid or missing year values.")

    df["year"] = year_numeric.astype("Int64")

    # Parse the known source format; do not infer ambiguous date formats.
    for column in ["published_at", "updated_at"]:
        original = df[column].copy()

        df[column] = pd.to_datetime(
            original,
            format="%d/%m/%Y %H:%M",
            errors="coerce",
        )

        failures = count_conversion_failures(original, df[column])

        record_exception(
            report,
            f"R-GRM-DATE-{column.upper()}",
            "grammy",
            f"Unparseable {column} source metadata.",
            failures,
        )

    # Normalized category key preserves the original category label.
    df["category_norm"] = normalized_text(df["category"])
    df["nominee_norm"] = normalized_text(df["nominee"])

    # Keep all rows with missing nominees and make their keys deterministic.
    missing_nominee = df["nominee"].isna()
    missing_nominee_count = int(missing_nominee.sum())

    record_exception(
        report,
        "R-GRM-02",
        "grammy",
        "Nominations without nominee; rows preserved with deterministic "
        "technical keys.",
        missing_nominee_count,
    )

    # Profiling established that year + category + nominee identifies rows
    # when nominee is present. Missing nominee rows use year + category.
    def make_nomination_key(row: pd.Series) -> str:
        if pd.notna(row["nominee_norm"]):
            return (
                f"{int(row['year'])}|"
                f"{row['category_norm']}|"
                f"{row['nominee_norm']}"
            )

        if pd.notna(row["category_norm"]):
            return (
                f"MISSING_NOMINEE|{int(row['year'])}|"
                f"{row['category_norm']}"
            )

        # This branch is only a defensive fallback; the source profile
        # identified a category for the six missing-nominee records.
        raise ValueError(
            "Grammy row lacks both nominee and category; "
            "cannot create a stable nomination key."
        )

    df["nomination_key"] = df.apply(make_nomination_key, axis=1)
    df["nominee_missing"] = missing_nominee.astype("boolean")

    # Explicitly map winner; do not impute unknown values.
    winner_original = df["winner"].copy()
    winner_clean = clean_text(winner_original).str.casefold()

    winner_map = {
        "true": True,
        "verdadero": True,
        "false": False,
        "falso": False,
    }

    df["winner"] = winner_clean.map(winner_map).astype("boolean")

    winner_invalid = int(
        (winner_clean.notna() & df["winner"].isna()).sum()
    )

    record_exception(
        report,
        "R-GRM-WINNER-01",
        "grammy",
        "Unrecognized winner values; represented as null.",
        winner_invalid,
    )

    winner_values = sorted(
        bool(value) for value in df["winner"].dropna().unique()
    )

    report["sources"]["grammy"] = {
        "input_rows": input_rows,
        "output_rows": len(df),
        "missing_nominee_rows_preserved": missing_nominee_count,
        "distinct_years": int(df["year"].nunique()),
        "distinct_categories": int(df["category_norm"].nunique()),
        "winner_distinct_values": winner_values,
        "grain": "one nomination per Grammy edition/category/nominee",
    }

    return df


# =========================================================
# Pipeline entry point
# =========================================================

def main() -> None:
    started_at = datetime.now(timezone.utc)

    report = {
        "stage": "TRANSFORM_INTEGRATE",
        "started_at_utc": started_at.isoformat(),
        "execution_status": "RUNNING",
        "global_success": False,
        "sources": {},
        "exceptions": [],
    }

    try:
        # RAW tables are read-only inputs for this stage.
        with engine.connect() as connection:
            spotify_raw = pd.read_sql_query(
                text('SELECT * FROM raw.spotify'),
                connection,
            )
            grammy_raw = pd.read_sql_query(
                text('SELECT * FROM raw.grammy'),
                connection,
            )

        if spotify_raw.empty or grammy_raw.empty:
            raise ValueError("One or both RAW source tables are empty.")

        spotify_prepared = transform_spotify(spotify_raw, report)
        grammy_prepared = transform_grammy(grammy_raw, report)

        # Atomic replacement of both PREPARED outputs.
        with engine.begin() as connection:
            connection.execute(
                text(f'CREATE SCHEMA IF NOT EXISTS "{PREPARED_SCHEMA}"')
            )

            publish_dataframe(
                connection,
                spotify_prepared,
                "spotify_tracks",
            )
            publish_dataframe(
                connection,
                grammy_prepared,
                "grammy_nominations",
            )

        report["execution_status"] = "SUCCESS"
        report["global_success"] = True

    except Exception as exc:
        report["execution_status"] = "ERROR"
        report["global_success"] = False
        report["error"] = {
            "type": type(exc).__name__,
            "message": str(exc),
        }
        raise

    finally:
        report["finished_at_utc"] = datetime.now(timezone.utc).isoformat()

        report_path = LOG_DIR / "transform_integrate_latest.json"
        with report_path.open("w", encoding="utf-8") as file:
            json.dump(report, file, ensure_ascii=False, indent=2, default=str)

        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()