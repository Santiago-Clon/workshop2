from pathlib import Path
import json
from datetime import datetime, timezone
import os
import pandas as pd
import great_expectations as gx
from sqlalchemy import create_engine, text


# ---------------------------------------------------------
# Project paths
# ---------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent

SPOTIFY_PATH = PROJECT_ROOT / "data" / "spotify_dataset.csv"
GRAMMY_PATH = PROJECT_ROOT / "data" / "the_grammy_awards.csv"

DB_URL = (
    "postgresql+psycopg2://"
    f"{os.getenv('ETL_DB_USER', 'etl_user')}:"
    f"{os.getenv('ETL_DB_PASSWORD', 'etl_password')}@"
    f"{os.getenv('ETL_DB_HOST', 'localhost')}:"
    f"{os.getenv('ETL_DB_PORT', '5433')}/"
    f"{os.getenv('ETL_DB_NAME', 'etl_workshop')}"
)


# ---------------------------------------------------------
# Expected RAW schemas
# ---------------------------------------------------------

SPOTIFY_COLUMNS = [
    "Column1",
    "track_id",
    "artists",
    "album_name",
    "track_name",
    "popularity",
    "duration_ms",
    "explicit",
    "danceability",
    "energy",
    "key",
    "loudness",
    "mode",
    "speechiness",
    "acousticness",
    "instrumentalness",
    "liveness",
    "valence",
    "tempo",
    "time_signature",
    "track_genre",
]

GRAMMY_COLUMNS = [
    "year",
    "title",
    "published_at",
    "updated_at",
    "category",
    "nominee",
    "artist",
    "workers",
    "img",
    "winner",
]

# ---------------------------------------------------------
# RAW validation rule catalog
# ---------------------------------------------------------

RAW_RULES = {
    "SPT-RAW-01": {
        "rule_name": "Spotify schema validation",
        "source_name": "Spotify",
        "severity": "Critical",
        "expectation": "ExpectTableColumnsToMatchOrderedList",
        "expected_condition": "Las columnas coinciden con el esquema esperado y su orden.",
        "failure_category": "SCHEMA_MISMATCH",
    },
    "SPT-RAW-02": {
        "rule_name": "Spotify non-empty validation",
        "source_name": "Spotify",
        "severity": "Critical",
        "expectation": "ExpectTableRowCountToBeBetween",
        "expected_condition": "El dataset contiene al menos una fila.",
        "failure_category": "EMPTY_DATASET",
    },
    "SPT-RAW-03": {
        "rule_name": "Spotify source-to-RAW reconciliation",
        "source_name": "Spotify",
        "severity": "Critical",
        "expectation": "CustomRecordCountReconciliation",
        "expected_condition": "source_count == raw_count",
        "failure_category": "RECONCILIATION_MISMATCH",
    },
    "GRM-RAW-01": {
        "rule_name": "Grammy schema validation",
        "source_name": "Grammy",
        "severity": "Critical",
        "expectation": "ExpectTableColumnsToMatchOrderedList",
        "expected_condition": "Las columnas coinciden con el esquema esperado y su orden.",
        "failure_category": "SCHEMA_MISMATCH",
    },
    "GRM-RAW-02": {
        "rule_name": "Grammy non-empty validation",
        "source_name": "Grammy",
        "severity": "Critical",
        "expectation": "ExpectTableRowCountToBeBetween",
        "expected_condition": "El dataset contiene al menos una fila.",
        "failure_category": "EMPTY_DATASET",
    },
    "GRM-RAW-03": {
        "rule_name": "Grammy source-to-RAW reconciliation",
        "source_name": "Grammy",
        "severity": "Critical",
        "expectation": "CustomRecordCountReconciliation",
        "expected_condition": "source_count == raw_count",
        "failure_category": "RECONCILIATION_MISMATCH",
    },
}

# ---------------------------------------------------------
# Database helpers
# ---------------------------------------------------------

def get_engine():
    return create_engine(DB_URL)


def get_raw_count(table_name):
    engine = get_engine()

    with engine.connect() as connection:
        result = connection.execute(
            text(f"SELECT COUNT(*) FROM raw.{table_name}")
        )

        return result.scalar()


# ---------------------------------------------------------
# Load RAW data
# ---------------------------------------------------------

def load_raw_table(table_name):
    engine = get_engine()

    return pd.read_sql(
        f"SELECT * FROM raw.{table_name}",
        engine,
    )


# ---------------------------------------------------------
# Validate schema
# ---------------------------------------------------------

def validate_schema(df, expected_columns, rule_id):
    context = gx.get_context(mode="ephemeral")

    datasource = context.data_sources.add_pandas(
        name=f"{rule_id}_datasource"
    )

    data_asset = datasource.add_dataframe_asset(
        name=f"{rule_id}_asset"
    )

    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        f"{rule_id}_batch"
    )

    batch = batch_definition.get_batch(
        batch_parameters={"dataframe": df}
    )

    expectation = gx.expectations.ExpectTableColumnsToMatchOrderedList(
        column_list=expected_columns
    )

    result = batch.validate(expectation)

    return result


# ---------------------------------------------------------
# Validate non-empty dataset
# ---------------------------------------------------------

def validate_non_empty(df, rule_id):
    context = gx.get_context(mode="ephemeral")

    datasource = context.data_sources.add_pandas(
        name=f"{rule_id}_datasource"
    )

    data_asset = datasource.add_dataframe_asset(
        name=f"{rule_id}_asset"
    )

    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        f"{rule_id}_batch"
    )

    batch = batch_definition.get_batch(
        batch_parameters={"dataframe": df}
    )

    expectation = gx.expectations.ExpectTableRowCountToBeBetween(
        min_value=1
    )

    result = batch.validate(expectation)

    return result

# ---------------------------------------------------------
# Validate source vs RAW record count
# ---------------------------------------------------------

def validate_reconciliation(source_count, raw_count, rule_id):
    success = source_count == raw_count

    return {
        "rule_id": rule_id,
        "success": success,
        "source_count": source_count,
        "raw_count": raw_count,
    }


# ---------------------------------------------------------
# Build standardized rule result
# ---------------------------------------------------------

def build_rule_result(
    rule_id,
    success,
    metrics=None,
    failure_reason=None,
    diagnostic_details=None,
):
    rule = RAW_RULES[rule_id]

    if success:
        action = "CONTINUE"
        failure_category = None
    elif rule["severity"] == "Critical":
        action = "STOP"
        failure_category = rule["failure_category"]
    else:
        action = "CONTINUE"
        failure_category = rule["failure_category"]

    return {
        "rule_id": rule_id,
        "rule_name": rule["rule_name"],
        "source_name": rule["source_name"],
        "severity": rule["severity"],
        "expectation": rule["expectation"],
        "expected_condition": rule["expected_condition"],
        "success": success,
        "failure_category": failure_category,
        "metrics": metrics or {},
        "failure_reason": failure_reason,
        "suspected_cause": None,
        "action": action,
        "diagnostic_details": diagnostic_details,
    }


# ---------------------------------------------------------
# Run validation for one source
# ---------------------------------------------------------

# ---------------------------------------------------------
# Run validation for one source
# ---------------------------------------------------------

def validate_source(
    source_name,
    source_df,
    raw_df,
    expected_columns,
    schema_rule_id,
    non_empty_rule_id,
    reconciliation_rule_id,
):
    print(f"\nValidating RAW source: {source_name}")

    results = []

    # Schema validation
    schema_result = validate_schema(
        raw_df,
        expected_columns,
        schema_rule_id,
    )

    results.append(
        build_rule_result(
            rule_id=schema_rule_id,
            success=bool(schema_result.success),
            failure_reason=(
                None
                if schema_result.success
                else "El esquema observado no coincide con el esperado."
            ),
            diagnostic_details=(
                None
                if schema_result.success
                else str(schema_result)
            ),
        )
    )

    print(
        f"{schema_rule_id}: "
        f"{'PASS' if schema_result.success else 'FAIL'}"
    )

    # Non-empty validation
    non_empty_result = validate_non_empty(
        raw_df,
        non_empty_rule_id,
    )

    results.append(
        build_rule_result(
            rule_id=non_empty_rule_id,
            success=bool(non_empty_result.success),
            metrics={
                "observed_row_count": len(raw_df),
                "minimum_expected_row_count": 1,
            },
            failure_reason=(
                None
                if non_empty_result.success
                else "La tabla RAW no contiene registros."
            ),
            diagnostic_details=(
                None
                if non_empty_result.success
                else str(non_empty_result)
            ),
        )
    )

    print(
        f"{non_empty_rule_id}: "
        f"{'PASS' if non_empty_result.success else 'FAIL'}"
    )

    # Source-to-RAW reconciliation
    reconciliation = validate_reconciliation(
        len(source_df),
        len(raw_df),
        reconciliation_rule_id,
    )

    difference = (
        reconciliation["source_count"]
        - reconciliation["raw_count"]
    )

    results.append(
        build_rule_result(
            rule_id=reconciliation_rule_id,
            success=bool(reconciliation["success"]),
            metrics={
                "source_count": reconciliation["source_count"],
                "raw_count": reconciliation["raw_count"],
                "difference": difference,
            },
            failure_reason=(
                None
                if reconciliation["success"]
                else "Los conteos de origen y RAW no coinciden."
            ),
            diagnostic_details=(
                None
                if reconciliation["success"]
                else (
                    f"Origen: {reconciliation['source_count']}; "
                    f"RAW: {reconciliation['raw_count']}; "
                    f"Diferencia: {difference}."
                )
            ),
        )
    )

    print(
        f"{reconciliation_rule_id}: "
        f"{'PASS' if reconciliation['success'] else 'FAIL'}"
    )

    return results

# ---------------------------------------------------------
# Persist validation evidence
# ---------------------------------------------------------

RESULTS_DIR = PROJECT_ROOT / "gx" / "results"


def save_validation_results(report):
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    output_path = RESULTS_DIR / (
        f"raw_validation_"
        f"{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_%f')}.json"
    )

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(
            report,
            file,
            indent=4,
            ensure_ascii=False,
            default=str,
        )

    print(f"Validation evidence saved: {output_path}")



# ---------------------------------------------------------
# Main RAW validation process
# ---------------------------------------------------------


def run_raw_validation(source="all"):
    source = source.strip().lower()

    if source not in {"all", "spotify", "grammy"}:
        raise ValueError(
            "source debe ser 'all', 'spotify' o 'grammy'."
        )

    print(f"Starting RAW validation: {source}")

    # Track the operation where a technical error occurs
    current_stage = "INITIALIZATION"

    # Keep completed validation results when possible
    spotify_results = []
    grammy_results = []

    try:
        # -----------------------------------------------------
        # Load source files
        # -----------------------------------------------------
        spotify_source = None
        grammy_source = None

        if source in {"all", "spotify"}:
            spotify_source = pd.read_csv(
                SPOTIFY_PATH,
                dtype=str,
            ).rename(columns={"Unnamed: 0": "Column1"})

        if source in {"all", "grammy"}:
            grammy_source = pd.read_csv(
                GRAMMY_PATH,
                sep=";",
                dtype=str,
            )

        # -----------------------------------------------------
        # Load RAW tables
        # -----------------------------------------------------
        current_stage = "RAW_LOADING"

        spotify_raw = (
            load_raw_table("spotify")
            if source in {"all", "spotify"}
            else None
        )

        grammy_raw = (
            load_raw_table("grammy")
            if source in {"all", "grammy"}
            else None
        )

        # -----------------------------------------------------
        # Validate Spotify
        # -----------------------------------------------------
        current_stage = "SPOTIFY_VALIDATION"

        if source in {"all", "spotify"}:
            spotify_results = validate_source(
                source_name="Spotify",
                source_df=spotify_source,
                raw_df=spotify_raw,
                expected_columns=SPOTIFY_COLUMNS,
                schema_rule_id="SPT-RAW-01",
                non_empty_rule_id="SPT-RAW-02",
                reconciliation_rule_id="SPT-RAW-03",
            )

        # -----------------------------------------------------
        # Validate Grammy
        # -----------------------------------------------------
        current_stage = "GRAMMY_VALIDATION"
        if source in {"all", "grammy"}:
            grammy_results = validate_source(
                source_name="Grammy",
                source_df=grammy_source,
                raw_df=grammy_raw,
                expected_columns=GRAMMY_COLUMNS,
                schema_rule_id="GRM-RAW-01",
                non_empty_rule_id="GRM-RAW-02",
                reconciliation_rule_id="GRM-RAW-03",
            )

    except Exception as exc:
        # -----------------------------------------------------
        # Technical error: preserve evidence and stop execution
        # -----------------------------------------------------
        completed_results = spotify_results + grammy_results

        error_report = {
            "pipeline_stage": "RAW",
            "execution_timestamp_utc": (
                datetime.now(timezone.utc).isoformat()
            ),
            "global_success": False,
            "execution_status": "ERROR",
            "summary": {
                "total_rules": len(completed_results),
                "passed_rules": sum(
                    result["success"]
                    for result in completed_results
                ),
                "failed_rules": sum(
                    not result["success"]
                    for result in completed_results
                ),
                "critical_failures": sum(
                    not result["success"]
                    and result["severity"] == "Critical"
                    for result in completed_results
                ),
                "warning_failures": sum(
                    not result["success"]
                    and result["severity"] == "Warning"
                    for result in completed_results
                ),
                "info_failures": sum(
                    not result["success"]
                    and result["severity"] == "Info"
                    for result in completed_results
                ),
            },
            "rules": completed_results,
            "error": {
                "stage": current_stage,
                "error_type": type(exc).__name__,
                "error_message": str(exc),
            },
        }

        print(
            f"\nTechnical error during {current_stage}: "
            f"{type(exc).__name__}: {exc}"
        )

        try:
            save_validation_results(error_report)
        except Exception as save_exc:
            # Do not hide the original technical error
            print(
                "WARNING: Could not save technical error evidence: "
                f"{type(save_exc).__name__}: {save_exc}"
            )

        # Re-raise the original exception so the pipeline stops
        raise

    # ---------------------------------------------------------
    # Final quality results
    # ---------------------------------------------------------
    all_results = spotify_results + grammy_results

    global_success = all(
        result["success"] for result in all_results
    )

    critical_failures = [
        result
        for result in all_results
        if (
            not result["success"]
            and result["severity"] == "Critical"
        )
    ]

    warning_failures = [
        result
        for result in all_results
        if (
            not result["success"]
            and result["severity"] == "Warning"
        )
    ]

    info_failures = [
        result
        for result in all_results
        if (
            not result["success"]
            and result["severity"] == "Info"
        )
    ]

    summary = {
        "total_rules": len(all_results),
        "passed_rules": sum(
            result["success"] for result in all_results
        ),
        "failed_rules": sum(
            not result["success"] for result in all_results
        ),
        "critical_failures": len(critical_failures),
        "warning_failures": len(warning_failures),
        "info_failures": len(info_failures),
    }

    execution_status = (
        "FAILED"
        if critical_failures
        else "COMPLETED"
    )

    report = {
        "pipeline_stage": "RAW",
        "execution_timestamp_utc": (
            datetime.now(timezone.utc).isoformat()
        ),
        "global_success": global_success,
        "execution_status": execution_status,
        "summary": summary,
        "rules": all_results,
    }

    # Save evidence before enforcing the quality gate
    save_validation_results(report)

    print("\nRAW validation summary")
    print(f"Execution status: {execution_status}")
    print(f"Global success: {global_success}")
    print(f"Rules passed: {summary['passed_rules']}")
    print(f"Rules failed: {summary['failed_rules']}")
    print(f"Critical failures: {summary['critical_failures']}")

    if critical_failures:
        raise RuntimeError(
            "RAW validation failed. "
            "At least one Critical rule did not pass."
        )

    print("\nRAW validation completed successfully.")

    return report


# ---------------------------------------------------------
# Script entry point
# ---------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Validate RAW Spotify and Grammy datasets."
    )

    parser.add_argument(
        "--source",
        choices=["all", "spotify", "grammy"],
        default="all",
        help="Source to validate.",
    )

    args = parser.parse_args()
    run_raw_validation(source=args.source)