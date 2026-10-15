
from datetime import datetime, timezone
import json
import os
from pathlib import Path

from sqlalchemy import URL, create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError


# ---------------------------------------------------------
# Project paths and database configuration
# ---------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = PROJECT_ROOT / "logs"
REPORT_PATH = LOG_DIR / "prepared_validation_latest.json"

# DATABASE_URL is optional. If absent, use the local Docker defaults.
DATABASE_URL = os.getenv("DATABASE_URL")

if DATABASE_URL:
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
else:
    database_url = URL.create(
        drivername="postgresql+psycopg2",
        username=os.getenv("PGUSER", "etl_user"),
        password=os.getenv("PGPASSWORD", "etl_password"),
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5433")),
        database=os.getenv("PGDATABASE", "etl_workshop"),
    )
    engine = create_engine(database_url, pool_pre_ping=True)


# ---------------------------------------------------------
# Validation report helpers
# ---------------------------------------------------------

report = {
    "stage": "PREPARED_VALIDATION",
    "started_at_utc": datetime.now(timezone.utc).isoformat(),
    "execution_status": "RUNNING",
    "global_success": False,
    "tables": {},
    "rules": [],
    "exceptions": [],
}


def add_rule(rule_id, description, passed, severity, details=None):
    """Register one validation result."""
    result = {
        "rule_id": rule_id,
        "description": description,
        "status": "PASS" if passed else "FAIL",
        "severity": severity,
        "details": details or {},
    }
    report["rules"].append(result)
    return passed


def save_report():
    """Persist the current report, including technical failures."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    report["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
    REPORT_PATH.write_text(
        json.dumps(report, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )


# ---------------------------------------------------------
# Validation process
# ---------------------------------------------------------

def main():
    try:
        with engine.connect() as connection:
            inspector = inspect(connection)

            required_columns = {
                "spotify_tracks": {
                    "track_id",
                    "track_genre",
                    "tempo",
                    "danceability",
                    "energy",
                    "instrumentalness",
                    "valence",
                },
                "grammy_nominations": {
                    "nominee",
                    "nominee_missing",
                    "published_at",
                    "updated_at",
                },
            }

            # Rule PV-01: required tables and columns exist.
            schema_errors = []

            for table_name, columns in required_columns.items():
                if not inspector.has_table(table_name, schema="prepared"):
                    schema_errors.append(
                        f"Missing table: prepared.{table_name}"
                    )
                    continue

                actual_columns = {
                    column["name"]
                    for column in inspector.get_columns(
                        table_name, schema="prepared"
                    )
                }

                missing_columns = columns - actual_columns

                if missing_columns:
                    schema_errors.append(
                        f"prepared.{table_name} missing columns: "
                        f"{sorted(missing_columns)}"
                    )

            schema_ok = add_rule(
                "PV-01",
                "Required PREPARED tables and columns exist.",
                not schema_errors,
                "CRITICAL",
                {"errors": schema_errors},
            )

            if not schema_ok:
                raise RuntimeError(
                    "PREPARED schema validation failed; "
                    "dependent checks cannot be executed."
                )

            # Rule PV-02: both tables contain records.
            row_counts = {}

            for table_name in required_columns:
                count = connection.execute(
                    text(
                        f'SELECT COUNT(*) FROM prepared."{table_name}"'
                    )
                ).scalar_one()

                row_counts[table_name] = int(count)

            report["tables"] = row_counts

            add_rule(
                "PV-02",
                "Both PREPARED tables contain records.",
                all(count > 0 for count in row_counts.values()),
                "CRITICAL",
                row_counts,
            )

            # Rule PV-03: Spotify business key is unique.
            duplicate_keys = connection.execute(text("""
                SELECT COUNT(*) 
                FROM (
                    SELECT track_id, track_genre
                    FROM prepared.spotify_tracks
                    GROUP BY track_id, track_genre
                    HAVING COUNT(*) > 1
                ) AS duplicate_groups;
            """)).scalar_one()

            add_rule(
                "PV-03",
                "No duplicate Spotify track_id + track_genre keys.",
                int(duplicate_keys) == 0,
                "CRITICAL",
                {"duplicate_key_groups": int(duplicate_keys)},
            )

            # Rule PV-04: tempo zero must have been normalized to NULL.
            tempo_stats = connection.execute(text("""
                SELECT
                    COUNT(*) FILTER (WHERE tempo IS NULL) AS tempo_nulls,
                    COUNT(*) FILTER (WHERE tempo = 0) AS tempo_zero
                FROM prepared.spotify_tracks;
            """)).mappings().one()

            add_rule(
                "PV-04",
                "Spotify tempo zero values are normalized to NULL.",
                int(tempo_stats["tempo_zero"]) == 0,
                "CRITICAL",
                {
                    "tempo_nulls": int(tempo_stats["tempo_nulls"]),
                    "tempo_zero_remaining": int(tempo_stats["tempo_zero"]),
                },
            )

            # Rule PV-05: selected normalized attributes are in [0, 1].
            range_stats = connection.execute(text("""
                SELECT
                    COUNT(*) FILTER (
                        WHERE danceability < 0 OR danceability > 1
                    ) AS invalid_danceability,
                    COUNT(*) FILTER (
                        WHERE energy < 0 OR energy > 1
                    ) AS invalid_energy,
                    COUNT(*) FILTER (
                        WHERE instrumentalness < 0
                           OR instrumentalness > 1
                    ) AS invalid_instrumentalness,
                    COUNT(*) FILTER (
                        WHERE valence < 0 OR valence > 1
                    ) AS invalid_valence
                FROM prepared.spotify_tracks;
            """)).mappings().one()

            invalid_ranges = {
                key: int(value)
                for key, value in range_stats.items()
            }

            add_rule(
                "PV-05",
                "Selected Spotify attributes fall within [0, 1].",
                all(value == 0 for value in invalid_ranges.values()),
                "CRITICAL",
                invalid_ranges,
            )

            # Rule PV-06: missing Grammy nominees are explicitly flagged.
            nominee_stats = connection.execute(text("""
                SELECT
                    COUNT(*) FILTER (
                        WHERE nominee IS NULL
                    ) AS null_nominees,
                    COUNT(*) FILTER (
                        WHERE nominee_missing IS TRUE
                    ) AS flagged_missing,
                    COUNT(*) FILTER (
                        WHERE nominee IS NULL
                          AND nominee_missing IS NOT TRUE
                    ) AS unflagged_missing,
                    COUNT(*) FILTER (
                        WHERE nominee IS NOT NULL
                          AND nominee_missing IS TRUE
                    ) AS incorrectly_flagged
                FROM prepared.grammy_nominations;
            """)).mappings().one()

            nominee_details = {
                key: int(value)
                for key, value in nominee_stats.items()
            }

            nominee_flags_ok = (
                nominee_details["unflagged_missing"] == 0
                and nominee_details["incorrectly_flagged"] == 0
            )

            add_rule(
                "PV-06",
                "Missing Grammy nominees are preserved and correctly flagged.",
                nominee_flags_ok,
                "CRITICAL",
                nominee_details,
            )

            # Rule PV-07: update timestamp is not earlier than publication.
            invalid_date_order = connection.execute(text("""
                SELECT COUNT(*)
                FROM prepared.grammy_nominations
                WHERE published_at IS NOT NULL
                  AND updated_at IS NOT NULL
                  AND updated_at < published_at;
            """)).scalar_one()

            add_rule(
                "PV-07",
                "Grammy updated_at is not earlier than published_at.",
                int(invalid_date_order) == 0,
                "CRITICAL",
                {"invalid_date_order": int(invalid_date_order)},
            )

        critical_failures = [
            rule
            for rule in report["rules"]
            if rule["severity"] == "CRITICAL"
            and rule["status"] != "PASS"
        ]

        report["global_success"] = len(critical_failures) == 0
        report["execution_status"] = (
            "SUCCESS" if report["global_success"] else "FAILED"
        )

    except (SQLAlchemyError, RuntimeError, OSError, ValueError) as exc:
        report["execution_status"] = "ERROR"
        report["global_success"] = False
        report["exceptions"].append({
            "stage": "PREPARED_VALIDATION",
            "exception_type": type(exc).__name__,
            "message": str(exc),
        })

    finally:
        save_report()
        engine.dispose()

        print(json.dumps(report, indent=2, ensure_ascii=False))

    if not report["global_success"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()  