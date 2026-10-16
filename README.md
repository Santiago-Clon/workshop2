# ETL Workshop 2: Reliable Music Data Pipeline

A reproducible ETL pipeline that integrates Spotify track metadata and Grammy Awards records into a PostgreSQL dimensional data warehouse, orchestrated with Apache Airflow and validated with Great Expectations. Power BI provides the analytical layer.

## 1. Project Overview

### Problem Statement

Music data is distributed across datasets with different structures, identifiers, naming conventions, and levels of detail. The goal of this workshop is to build a reliable data pipeline that prepares, validates, integrates, and loads these sources into a dimensional warehouse for analytical reporting.

### Analytical Objective

Enable analysis of Spotify track popularity and audio characteristics by genre, and explore relationships between tracks and Grammy Awards records. Grammy associations are not necessarily proof that a track or artist won an award.

## 2. Analytical Requirements

- **RQ-01 — Popularity by Genre:** Analyze average track popularity across genres.
- **RQ-02 — Audio Features by Genre:** Compare audio characteristics, including danceability, energy, loudness, speechiness, acousticness, instrumentalness, liveness, valence, and tempo, across genres.
- **RQ-03 — Popularity by Grammy Year and Category:** Analyze average popularity for tracks associated with Grammy records, grouped by year and category.

The Power BI report implements these analytical views using the dimensional warehouse as its analytical data source.

## 3. Data Sources

| Source | Description | Role |
|---|---|---|
| `data/spotify_dataset.csv` | Spotify track metadata, genre, popularity, and audio features | Main source for track-level analytics |
| `data/the_grammy_awards.csv` | Grammy Awards records, including years and categories | Context for Grammy-related analysis |

The source files are stored locally in the repository. Their original provenance, licensing, and any preprocessing performed before the workshop should be documented when confirmed.

## 4. Repository Structure

```text
Workshop2/
├── dags/
│   └── etl_workshop_pipeline.py
├── data/
│   ├── analytics/
│   ├── spotify_dataset.csv
│   └── the_grammy_awards.csv
├── docs/
│   └── evidence/
├── gx/
│   ├── raw_validation.py
│   └── results/
├── notebooks/
│   ├── profiling_grammy.ipynb
│   ├── profiling_spotify.ipynb
│   └── prueba.ipynb
├── sql/
│   ├── analytics_queries.sql
│   └── source_setup.sql
├── src/
│   ├── extract_grammy.py
│   ├── extract_spotify.py
│   ├── integrate_artists.py
│   ├── load_dw.py
│   ├── prepared_validation.py
│   └── transform_integrate.py
├── config/
├── logs/
├── notebooks/
├── backups/
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md
```

Generated logs, temporary results, local environment files, and database backups should be excluded from version control unless selected as intentional evidence. The `.env` file must not be committed.

## 5. Pipeline Architecture

The pipeline separates orchestration from reusable ETL logic. Airflow coordinates the workflow, while Python modules implement extraction, transformation, integration, validation, and loading.

```text
Spotify CSV ───────┐
                   ├─> Extraction ─> Raw validation
Grammy CSV ────────┘                     │
                                        v
                              Transformation and integration
                                        │
                                        v
                               Prepared-data validation
                                        │
                                        v
                           PostgreSQL dimensional warehouse
                                        │
                                        v
                              Analytical SQL queries
                                        │
                                        v
                                 Power BI report
```

The Airflow DAG is named `etl_workshop_pipeline`. The PostgreSQL service for project data is exposed on host port `5433`; the Airflow interface is exposed on port `8080`.

## 6. Data Profiling and Quality Findings

Initial profiling was performed in `notebooks/profiling_spotify.ipynb` and `notebooks/profiling_grammy.ipynb`.

The analysis focused on schema consistency, missing values, data types, duplicate records, identifier quality, categorical variation, and suitability for integration.

### Key findings and risks

- Spotify data includes track metadata, genre labels, popularity, and numerical audio features.
- Track and artist names may vary in formatting, spelling, or representation across sources.
- A track can be associated with multiple artists, so artist relationships require a separate bridge table.
- Grammy records are not equivalent to Spotify tracks and cannot be reliably joined using a simple one-to-one relationship.
- Duplicate business identifiers and repeated records must be distinguished from complete-row duplicates.
- Missing or inconsistent values may affect analytical coverage and aggregation results.

The prepared Spotify dataset contains **113,550 track-genre records** in the recorded baseline. The prepared Grammy dataset contains **4,810 records**. These counts describe the recorded pipeline baseline and should be rechecked after source changes.

## 7. Quality Rules and Great Expectations

Raw-data validation is implemented in `gx/raw_validation.py`, while prepared-data validation is implemented in `src/prepared_validation.py`.

The validation design checks whether data satisfies the conditions required for safe transformation and loading. Relevant rule groups include:

- **Schema validation:** expected columns and data structure are present.
- **Type and domain validation:** numerical measures and categorical fields conform to expected formats and domains.
- **Completeness validation:** required identifiers and attributes meet completeness requirements.
- **Uniqueness and duplicate analysis:** duplicate behavior is assessed using appropriate keys rather than assuming every repeated value is invalid.
- **Referential integrity:** integrated records can be linked to their corresponding dimensional entities.
- **Prepared-data validation:** transformed datasets meet the requirements of the warehouse loading stage.

Great Expectations is pinned to version `1.24.0` in the project requirements. Validation results are stored under `gx/results/`, with selected evidence in `docs/evidence/`.

A validation failure should prevent invalid data from silently proceeding through mandatory pipeline gates. The exact rule identifiers, thresholds, and failure actions should be maintained alongside the validation implementation.

## 8. Transformation and Data Integration

The transformation and integration logic is separated into reusable modules:

- `src/extract_spotify.py` extracts the Spotify source data.
- `src/extract_grammy.py` extracts the Grammy source data.
- `src/transform_integrate.py` prepares and integrates source records.
- `src/integrate_artists.py` handles artist integration and candidate matching.
- `src/prepared_validation.py` validates prepared datasets.
- `src/load_dw.py` loads the dimensional warehouse.

Artist names and other matching attributes may require normalization before matching. Matching results should be interpreted according to the matching method and available evidence, rather than assuming every candidate represents a confirmed identity.

The integration process preserves many-to-many relationships where necessary. Grammy associations should be interpreted as dataset-derived relationships, not as definitive proof of a Grammy win by a track or artist.

## 9. Dimensional Data Warehouse

The analytical warehouse is implemented in PostgreSQL under the `dw` schema.

### Main tables

| Table | Purpose |
|---|---|
| `dw.fact_track` | Track-genre-level analytical measures |
| `dw.dim_artist` | Artist descriptive attributes |
| `dw.dim_genre` | Genre dimension |
| `dw.dim_grammy_record` | Grammy year and category attributes |
| `dw.bridge_track_artist` | Many-to-many relationship between tracks and artists |
| `dw.bridge_track_grammy` | Many-to-many relationship between tracks and Grammy records |

### Fact table grain

The grain of `dw.fact_track` is **one row per Spotify track and genre**, identified by the combination of track identity and genre. A track appearing under multiple genres can therefore contribute multiple rows to the fact table. Aggregations must account for this grain to avoid unintentionally counting the same track more than once.

The fact table stores popularity and audio measures, including duration, danceability, energy, loudness, speechiness, acousticness, instrumentalness, liveness, valence, and tempo.

The bridge tables support relationships that cannot be represented accurately by a single foreign key in the fact table. The Grammy bridge also stores match metadata such as `match_method` and `match_role`.

## 10. Airflow Orchestration and Reliability

The DAG `etl_workshop_pipeline` coordinates the ETL stages and their dependencies. Processing logic remains in `src/` and validation logic in `gx/` and `src/prepared_validation.py`, rather than placing all transformation code inside the DAG.

The project runs Airflow using Docker Compose with a Celery-based execution architecture, Redis as the message broker, and a separate PostgreSQL service for Airflow metadata. The project warehouse uses a different PostgreSQL service.

### Failure handling and retries

The DAG should define retries for transient failures and avoid retrying deterministic data-quality failures indefinitely. Mandatory validation failures should be visible in Airflow task status and execution logs.

The controlled-failure test is represented by the task `inject_spotify_reconciliation_failure` in the execution evidence. Its purpose is to demonstrate that an intentional failure can be observed through the orchestration layer. The final evidence register should identify the exact run, expected failure, and observed downstream behavior.

### Repeatability and safe reruns

A rerun should not create unintended duplicate warehouse records or inconsistent bridge relationships. Repeatability depends on stable business keys, deterministic transformations, and the conflict-handling behavior implemented in `src/load_dw.py`.

A successful initial run alone does not prove safe rerun behavior. The evidence should include a rerun of the same input and a comparison of relevant warehouse row counts and key constraints before and after the rerun.

## 11. Execution Evidence

Selected evidence is maintained in `docs/evidence/`, with validation results also available under `gx/results/` and `logs/`.

Examples of existing evidence files include:

- `docs/evidence/DAG en Airflow.png` — DAG visualization.
- `docs/evidence/Grid de Airflow.png` — Airflow task grid.
- `docs/evidence/dw_row_counts.txt` — warehouse row counts.
- `docs/evidence/prepared_validation_latest.json` — prepared-data validation result.
- `docs/evidence/transform_integrate_latest.json` — transformation and integration evidence.
- `docs/evidence/artist_integration_latest.json` — artist integration evidence.
- `docs/evidence/load_dw_latest.json` — warehouse loading evidence.
- `docs/evidence/analytics_results.txt` — analytical query evidence.

For assessment, evidence should clearly distinguish a successful pipeline run, a controlled failed run, and a safe rerun. Each item should identify the relevant execution or test and the expected versus observed result.

## 12. Analytical Outputs and Power BI

The Power BI report presents the analytical requirements through three views:

- **RQ-01:** average popularity by genre.
- **RQ-02:** comparison of audio features by genre.
- **RQ-03:** average popularity by Grammy year and category.

The analytical SQL queries are maintained in `sql/analytics_queries.sql`. The RQ-03 analysis uses the track-to-Grammy bridge to associate tracks with Grammy records and applies the selected track set to the fact table. Results should be interpreted as popularity among associated tracks, not necessarily winners or nominees exclusively unless the source data and matching logic establish that distinction.

Add the actual Power BI `.pbix` filename or a relative link here once confirmed. The report should be checked to ensure that it uses the dimensional warehouse rather than an unrelated local extract.

## 13. Setup and Execution

### Prerequisites

- Docker Desktop with Docker Compose.
- Git.
- The source CSV files included in `data/`.

### 1. Clone the repository

```bash
git clone https://github.com/Santiago-Clon/workshop2.git
cd workshop2
```

### 2. Configure environment variables

Create a local `.env` file based on a documented example. Do not commit `.env` or real credentials. Configure the Airflow UID and any other variables required by the Compose file.

### 3. Build the Airflow image

```bash
docker compose build
```

The Dockerfile installs the dependencies from `requirements.txt` using the Airflow version-specific constraints file.

### 4. Initialize Airflow

```bash
docker compose up airflow-init
```

Wait for initialization to complete successfully.

### 5. Start the services

```bash
docker compose up -d
```

Open the Airflow interface at `http://localhost:8080`. Use the credentials configured for the local environment. Confirm that the DAG `etl_workshop_pipeline` is available before triggering a run.

The project PostgreSQL service is exposed at `localhost:5433`. Use the credentials configured for the project database when connecting with a SQL client or Power BI.

### 6. Run and verify the pipeline

Trigger the DAG and inspect task status and logs in Airflow. Confirm that raw and prepared validation results are generated, warehouse loading completes, and expected warehouse tables contain records. Run the analytical queries in `sql/analytics_queries.sql` and compare their results with the Power BI report.

Do not remove Docker volumes when restarting the project unless intentionally resetting the database. A command such as `docker compose down -v` can delete persistent data.

## 14. Assumptions and Limitations

- Results depend on the versions and contents of the source CSV files.
- Name-based integration can produce ambiguous or incomplete matches.
- A Grammy record association does not, by itself, establish that a track or artist won an award.
- The fact table grain is track plus genre, which affects aggregation and duplicate interpretation.
- Popularity is a source-provided measure and may not represent a permanent or directly comparable measure across time.
- Local Docker configuration and credentials must be configured independently by each user.
- Successful execution, controlled failure handling, and safe rerun behavior must be demonstrated with execution evidence rather than inferred from the existence of the DAG.

## 15. Deliverable Checklist

- [x] Profiling notebooks and ETL source modules organized by responsibility.
- [x] Great Expectations raw-data validation implementation.
- [x] Prepared-data validation implementation.
- [x] Airflow DAG and Docker Compose environment.
- [x] PostgreSQL dimensional warehouse and analytical SQL queries.
- [x] Power BI report included in the repository.
- [ ] Quality-rule catalog with rule IDs, thresholds, failure actions, and links to analytical requirements.
- [ ] Architecture and dimensional-model diagrams included as documentation.
- [ ] Successful execution evidence linked to a specific run.
- [ ] Controlled failure evidence with expected and observed behavior.
- [ ] Safe rerun evidence demonstrating that repeated execution does not introduce unintended duplicates.
- [ ] Reproducible setup verified from a clean clone and fresh environment.

The checklist must be updated to reflect only capabilities that have been verified in the final repository and execution environment.

## 16. Conclusion

This workshop demonstrates an ETL architecture that separates orchestration from processing logic, applies data-quality checks before analytical loading, integrates sources with different structures, and exposes the resulting data through a dimensional warehouse and Power BI. The reliability of the final system is demonstrated through validation results, controlled failure behavior, repeatability tests, and reproducible setup instructions.
