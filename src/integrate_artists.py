
from datetime import datetime, timezone
from pathlib import Path
import json
import os
import re
import unicodedata

import pandas as pd
from sqlalchemy import URL, create_engine, inspect, text


# ---------------------------------------------------------
# Project configuration
# ---------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = PROJECT_ROOT / "logs"
REPORT_PATH = LOG_DIR / "artist_integration_latest.json"

SCHEMA = "prepared"

SPOTIFY_TABLE = "spotify_tracks"
GRAMMY_TABLE = "grammy_nominations"

OUTPUT_TABLES = {
    "spotify_artists": "spotify_track_artists",
    "grammy_candidates": "grammy_artist_candidates",
    "matches": "track_grammy_matches",
}

engine_url = URL.create(
    drivername="postgresql+psycopg2",
    username=os.getenv("ETL_DB_USER", "etl_user"),
    password=os.getenv("ETL_DB_PASSWORD", "etl_password"),
    host=os.getenv("ETL_DB_HOST", "localhost"),
    port=int(os.getenv("ETL_DB_PORT", "5433")),
    database=os.getenv("ETL_DB_NAME", "etl_workshop"),
)

engine = create_engine(engine_url, pool_pre_ping=True)


# ---------------------------------------------------------
# Report
# ---------------------------------------------------------

report = {
    "stage": "ARTIST_INTEGRATION",
    "started_at_utc": datetime.now(timezone.utc).isoformat(),
    "execution_status": "RUNNING",
    "global_success": False,
    "rules": [],
    "metrics": {},
    "exceptions": [],
}


def add_rule(rule_id, description, passed, details=None):
    report["rules"].append({
        "rule_id": rule_id,
        "description": description,
        "status": "PASS" if passed else "FAIL",
        "details": details or {},
    })


def save_report():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    report["finished_at_utc"] = datetime.now(
        timezone.utc
    ).isoformat()

    REPORT_PATH.write_text(
        json.dumps(report, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


# ---------------------------------------------------------
# Normalization
# ---------------------------------------------------------

def clean_text(value):
    if pd.isna(value):
        return None

    value = str(value).strip()

    return value or None


def normalize_name(value):
    """
    Conservative name normalization:
    - Unicode NFKC
    - case folding
    - whitespace normalization

    Punctuation and accents are preserved.
    """
    value = clean_text(value)

    if value is None:
        return None

    value = unicodedata.normalize("NFKC", value)
    value = re.sub(r"\s+", " ", value).strip()

    return value.casefold() or None


# ---------------------------------------------------------
# Read and validate PREPARED inputs
# ---------------------------------------------------------

def read_prepared_table(table_name, required_columns):
    inspector = inspect(engine)

    if not inspector.has_table(table_name, schema=SCHEMA):
        raise RuntimeError(
            f"Required table missing: {SCHEMA}.{table_name}"
        )

    actual_columns = {
        column["name"]
        for column in inspector.get_columns(
            table_name,
            schema=SCHEMA,
        )
    }

    missing = set(required_columns) - actual_columns

    if missing:
        raise RuntimeError(
            f"{SCHEMA}.{table_name} missing columns: "
            f"{sorted(missing)}"
        )

    query = text(
        f'SELECT * FROM "{SCHEMA}"."{table_name}"'
    )

    with engine.connect() as connection:
        df = pd.read_sql(query, connection)

    if df.empty:
        raise RuntimeError(
            f"Required table is empty: {SCHEMA}.{table_name}"
        )

    return df


# ---------------------------------------------------------
# Spotify: one row per track, genre and individual artist
# ---------------------------------------------------------

def build_spotify_artists(spotify):
    rows = []

    for row in spotify[
        ["track_id", "track_genre", "artists"]
    ].itertuples(index=False, name=None):

        track_id, genre, artists = row

        track_id = clean_text(track_id)
        genre = clean_text(genre)
        artists = clean_text(artists)

        if not track_id or not genre or not artists:
            continue

        # The semicolon delimiter was observed in profiling.
        # Do not split on commas or ampersands.
        for position, raw_artist in enumerate(
            artists.split(";"), start=1
        ):
            artist_name = clean_text(raw_artist)
            normalized = normalize_name(artist_name)

            if not artist_name or not normalized:
                continue

            rows.append({
                "track_id": track_id,
                "track_genre": genre,
                "artist_name": artist_name,
                "artist_name_normalized": normalized,
                "artist_position": position,
            })

    result = pd.DataFrame(rows)

    if result.empty:
        raise RuntimeError(
            "No individual Spotify artists could be extracted."
        )

    result = result.drop_duplicates(
        subset=[
            "track_id",
            "track_genre",
            "artist_name_normalized",
        ]
    ).reset_index(drop=True)

    return result


# ---------------------------------------------------------
# Grammy: candidates from the explicit artist field only
# ---------------------------------------------------------

def build_grammy_candidates(grammy):
    candidates = grammy[
        [
            "nomination_key",
            "year",
            "category",
            "category_norm",
            "artist",
        ]
    ].copy()

    candidates["artist_name"] = candidates["artist"].map(
        clean_text
    )

    candidates["artist_name_normalized"] = (
        candidates["artist_name"].map(normalize_name)
    )

    candidates = candidates.loc[
        candidates["artist_name_normalized"].notna()
    ].copy()

    candidates["match_source"] = "artist"
    candidates = candidates.drop(columns=["artist"])

    candidates = candidates.drop_duplicates(
        subset=[
            "nomination_key",
            "artist_name_normalized",
        ]
    ).reset_index(drop=True)

    return candidates


# ---------------------------------------------------------
# Exact normalized-name matching
# ---------------------------------------------------------

def build_matches(
    spotify_artists: pd.DataFrame,
    grammy_candidates: pd.DataFrame,
) -> pd.DataFrame:

    output_columns = [
        "track_id",
        "track_genre",
        "artist_name",
        "artist_name_normalized",
        "nomination_key",
        "grammy_year",
        "grammy_category",
        "grammy_category_normalized",
        "match_method",
        "match_role",
    ]

    # Resultado vacío con esquema estable.
    if spotify_artists.empty or grammy_candidates.empty:
        return pd.DataFrame(columns=output_columns)

    # Seleccionar y renombrar explícitamente las columnas
    # antes de realizar la unión.
    spotify = spotify_artists[
        [
            "track_id",
            "track_genre",
            "artist_name",
            "artist_name_normalized",
        ]
    ].copy()

    grammy = grammy_candidates[
        [
            "nomination_key",
            "year",
            "category",
            "category_norm",
            "artist_name_normalized",
        ]
    ].copy()

    grammy = grammy.rename(
        columns={
            "year": "grammy_year",
            "category": "grammy_category",
            "category_norm": "grammy_category_normalized",
        }
    )

    # Coincidencia exacta de las claves normalizadas.
    matches = spotify.merge(
        grammy,
        on="artist_name_normalized",
        how="inner",
        validate="many_to_many",
    )

    matches["match_method"] = "EXACT_NORMALIZED_NAME"
    matches["match_role"] = "GRAMMY_ARTIST_FIELD"

    matches = matches[output_columns].drop_duplicates(
        subset=[
            "track_id",
            "track_genre",
            "nomination_key",
            "artist_name_normalized",
        ]
    )

    return matches.reset_index(drop=True)


# ---------------------------------------------------------
# Publish outputs atomically
# ---------------------------------------------------------

def publish_outputs(outputs):
    """
    Build staging tables and replace the three outputs
    in one PostgreSQL transaction.
    """
    with engine.begin() as connection:
        for target in OUTPUT_TABLES.values():
            staging = f"{target}__stg"

            connection.execute(text(
                f'DROP TABLE IF EXISTS "{SCHEMA}"."{staging}"'
            ))

            outputs[target].to_sql(
                name=staging,
                schema=SCHEMA,
                con=connection,
                if_exists="replace",
                index=False,
                method="multi",
                chunksize=1000,
            )

        for target in OUTPUT_TABLES.values():
            staging = f"{target}__stg"

            connection.execute(text(
                f'DROP TABLE IF EXISTS "{SCHEMA}"."{target}"'
            ))

            connection.execute(text(
                f'ALTER TABLE "{SCHEMA}"."{staging}" '
                f'RENAME TO "{target}"'
            ))


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

def main():
    spotify = read_prepared_table(
        SPOTIFY_TABLE,
        ["track_id", "track_genre", "artists"],
    )

    grammy = read_prepared_table(
        GRAMMY_TABLE,
        [
            "nomination_key",
            "year",
            "category",
            "category_norm",
            "artist",
        ],
    )

    spotify_artists = build_spotify_artists(spotify)
    grammy_candidates = build_grammy_candidates(grammy)

    matches = build_matches(
        spotify_artists,
        grammy_candidates,
    )

    matched_artist_keys = set(
        matches["artist_name_normalized"]
    )

    spotify_artist_keys = set(
        spotify_artists["artist_name_normalized"]
    )

    grammy_artist_keys = set(
        grammy_candidates["artist_name_normalized"]
    )

    report["metrics"] = {
        "spotify_prepared_rows": len(spotify),
        "grammy_prepared_rows": len(grammy),
        "spotify_individual_artist_associations": len(
            spotify_artists
        ),
        "spotify_distinct_artist_names": len(
            spotify_artist_keys
        ),
        "grammy_artist_candidates": len(grammy_candidates),
        "grammy_distinct_artist_names": len(
            grammy_artist_keys
        ),
        "matched_distinct_artist_names": len(
            matched_artist_keys
        ),
        "unmatched_spotify_artist_names": len(
            spotify_artist_keys - grammy_artist_keys
        ),
        "unmatched_grammy_artist_names": len(
            grammy_artist_keys - spotify_artist_keys
        ),
        "confirmed_track_nomination_matches": len(matches),
    }

    add_rule(
        "MAT-01",
        "Spotify artist associations have nonempty normalized names",
        spotify_artists["artist_name_normalized"].notna().all(),
    )

    add_rule(
        "MAT-02",
        "Grammy candidates have nonempty normalized names",
        grammy_candidates[
            "artist_name_normalized"
        ].notna().all(),
    )

    add_rule(
        "MAT-03",
        "Every match uses exact normalized-name equality",
        (
            matches.empty
            or matches["match_method"].eq(
                "EXACT_NORMALIZED_NAME"
            ).all()
        ),
        {
            "matching_strategy": "exact normalized name",
            "fuzzy_matching_enabled": False,
        },
    )

    add_rule(
        "MAT-04",
        "Output match keys are unique",
        not matches.duplicated([
            "track_id",
            "track_genre",
            "nomination_key",
            "artist_name_normalized",
        ]).any(),
    )

    outputs = {
        OUTPUT_TABLES["spotify_artists"]: spotify_artists,
        OUTPUT_TABLES["grammy_candidates"]: grammy_candidates,
        OUTPUT_TABLES["matches"]: matches,
    }

    publish_outputs(outputs)

    report["global_success"] = True
    report["execution_status"] = "SUCCESS"


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        report["global_success"] = False
        report["execution_status"] = "ERROR"
        report["exceptions"].append({
            "type": type(exc).__name__,
            "message": str(exc),
        })
        raise
    finally:
        save_report()
        engine.dispose()

    if not report["global_success"]:
        raise SystemExit(1)

    print(json.dumps(report, indent=2, ensure_ascii=False))