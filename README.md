
# Spotify and Grammy Awards — ETL Data Warehouse

## 1. Project Overview

This project implements an end-to-end ETL pipeline to integrate Spotify track metadata and Grammy Awards records into a dimensional data warehouse for analytical purposes.

The solution covers the complete data lifecycle, including source profiling, data quality assessment, extraction, transformation, artist integration, dimensional modeling, orchestration with Apache Airflow, and analytical queries consumed by a dashboard.

The main objective is to provide a structured and reproducible analytical environment for exploring the musical characteristics and popularity of Spotify tracks associated with artists appearing in Grammy records.

### Main Technologies

- Python
- Pandas
- PostgreSQL
- SQLAlchemy
- Apache Airflow
- Docker Compose
- Great Expectations and custom data quality validation scripts
- SQL for analytical queries
- A dashboard connected to the dimensional data warehouse

## 2. Business Requirements

The project was designed around three analytical requirements.

### RQ-01 — Average Popularity by Genre

Analyze the average Spotify popularity of tracks associated with artists appearing in Grammy records, differentiating results by musical genre.

**Analytical objective:** Identify differences in average track popularity across genres within the integrated dataset.

### RQ-02 — Musical Characteristics by Genre

Analyze the musical characteristics of Spotify tracks associated with artists recognized in Grammy records, identifying differences according to musical genre.

**Analytical objective:** Compare audio attributes such as danceability, energy, loudness, speechiness, acousticness, instrumentalness, liveness, valence, and tempo.

### RQ-03 — Popularity by Grammy Year and Category

Analyze the current Spotify popularity distribution of tracks associated with artists present in Grammy records, differentiating results by Grammy year and award category.

**Analytical objective:** Explore how current track popularity varies across Grammy records and categories.

**Important interpretation note:** RQ-03 groups current Spotify popularity by Grammy year and category. It does not measure historical Spotify popularity over time because the Spotify dataset does not provide the required business-time history.

## 3. Data Sources

The project uses two CSV datasets.

### 3.1 Spotify Dataset

**File:** `data/spotify_dataset.csv`

The dataset contains track metadata, artist names, genre classifications, popularity measurements, and audio features.

Relevant attributes include:

- `track_id`
- `track_name`
- `artists`
- `album_name`
- `track_genre`
- `popularity`
- `duration_ms`
- `danceability`
- `energy`
- `loudness`
- `speechiness`
- `acousticness`
- `instrumentalness`
- `liveness`
- `valence`
- `tempo`
- `explicit`

The original dataset contains 114,000 rows.

### 3.2 Grammy Awards Dataset

**File:** `data/the_grammy_awards.csv`

This dataset contains Grammy-related records, including nominee information, award categories, years, winner indicators, and source metadata.

Relevant attributes include:

- Nominee
- Category
- Grammy year
- Winner
- `published_at`
- `updated_at`

The original dataset contains 4,810 rows.

The fields `published_at` and `updated_at` describe source metadata and must not be interpreted as the Grammy ceremony year.

## 4. Initial Data Profiling

Data profiling was performed before implementing the complete pipeline to identify structural inconsistencies, missing values, duplicate records, and potential integration problems.

### 4.1 Spotify Profiling Findings

| Finding | Observation | Decision |
|---|---|---|
| Candidate business key | `track_id + track_genre` | Use as the track-genre grain |
| Duplicate key groups | 444 | Investigate repeated observations |
| Rows involved in duplicate groups | 894 | Compare business attributes |
| Additional rows removed | 450 | Retain one observation per key |
| Exact full-row duplicates | None detected | Distinguish key duplication from full-row duplication |
| Distinct genres | 114 | Preserve genre-level analysis |
| Missing descriptive attributes | Three missing values in `artists`, `album_name`, and `track_name` were identified during profiling | Preserve and handle according to transformation rules |
| Numeric scale inconsistencies | Several audio attributes used inconsistent scales | Normalize before analytical use |
| Zero tempo values | 157 after transformation | Convert to NULL as unknown values |

The 450 additional rows were removed because they represented repeated `track_id + track_genre` combinations. The evaluated business attributes showed no differences across those duplicate-key groups.

The resulting Spotify prepared table contains **113,550 rows**, with no duplicate `track_id + track_genre` keys.

A track may appear in more than one genre. Therefore, `track_id` alone does not identify a row at the selected analytical grain.

The source field `Column1` was treated as a technical field rather than a business key.

### 4.2 Grammy Profiling Findings

| Finding | Observation | Decision |
|---|---|---|
| Original records | 4,810 | Preserve the source records |
| Missing nominees | 6 | Preserve and flag the records |
| Category text variations | Identified during profiling | Normalize for integration |
| Multivalued descriptive fields | Present | Handle according to transformation requirements |
| Winner values | Only `True` was observed | Do not use this field to distinguish winners from non-winners |

The six records without nominee names were preserved and assigned deterministic technical keys where required by the transformation process.

Their missing nominee values were flagged rather than silently discarded.

The constant `winner` value is a source limitation. It does not provide sufficient variation to distinguish winning and non-winning records in the available dataset.

## 5. Data Quality Risks and Transformation Decisions

The profiling process identified several risks that influenced the ETL design.

### 5.1 Spotify Quality Risks

- **R-SPT-01 — Duplicate track-genre observations:** Repeated `track_id + track_genre` combinations were evaluated, and 450 additional rows were removed.
- **R-SPT-02 — Missing descriptive attributes:** Missing track, artist, or album descriptions may affect identification and integration.
- **R-SPT-03 — Inconsistent categorical representation:** Textual boolean values and categorical domains require standardization.
- **R-SPT-04 — Inconsistent numeric scales:** Audio features and measurements required normalization.
- **R-SPT-04B — Mixed instrumentalness representation:** Values represented at different scales required consistent treatment.
- **R-SPT-04C — Audio feature domain:** Selected normalized attributes must fall within the interval `[0, 1]`.
- **R-SPT-04D — Invalid tempo values:** Tempo equal to zero is treated as unknown and converted to NULL.
- **R-SPT-05 — Text normalization:** Differences in artist and category text may prevent exact integration.
- **R-SPT-06 — Technical and business attributes:** `Column1` is not a business identifier, and popularity may vary across genre observations for the same track.
- **R-SPT-07 — Lack of business-time history:** The source does not provide the historical Spotify popularity snapshots needed for temporal trend analysis.

### 5.2 Grammy Quality Risks

- **R-GRM-01 — Missing descriptive attributes:** Missing values must be evaluated according to their business meaning.
- **R-GRM-02 — Missing nominees:** Six records are retained and explicitly flagged.
- **R-GRM-03 — Category text variations:** Normalization is required for consistent category matching.
- **R-GRM-04 — Multivalued fields:** Some text attributes may represent multiple entities.
- **R-GRM-05 — Source timestamps:** Publication and update timestamps must not be confused with the Grammy award year.
- **R-GRM-06 — Constant winner indicator:** The available values do not distinguish winning and non-winning records.

## 6. Data Quality Validation Strategy

The pipeline uses stage-specific validation scripts and rule identifiers to make data quality checks explicit and auditable.

### 6.1 RAW Validation

**Script:** `gx/raw_validation.py`

RAW validation checks source structure, non-empty datasets, and source-to-RAW reconciliation.

| Rule | Description | Severity |
|---|---|---|
| SPT-RAW-01 | Spotify schema validation | Critical |
| SPT-RAW-02 | Spotify non-empty validation | Critical |
| SPT-RAW-03 | Spotify source-to-RAW reconciliation | Critical |
| GRM-RAW-01 | Grammy schema validation | Critical |
| GRM-RAW-02 | Grammy non-empty validation | Critical |
| GRM-RAW-03 | Grammy source-to-RAW reconciliation | Critical |

A critical validation failure prevents the pipeline from proceeding to the subsequent transformation stage.

The validation output records individual rule statuses, execution status, exceptions, and an overall success indicator.

### 6.2 PREPARED Validation

**Script:** `src/prepared_validation.py`

PREPARED validation verifies that the transformation process produces data that satisfies the defined structural and business constraints.

| Rule | Validation |
|---|---|
| PV-01 | Required PREPARED tables and columns exist |
| PV-02 | Spotify and Grammy prepared tables are non-empty |
| PV-03 | No duplicate Spotify `track_id + track_genre` keys |
| PV-04 | Spotify tempo zero values are normalized to NULL |
| PV-05 | Selected Spotify audio attributes fall within `[0, 1]` |
| PV-06 | Missing Grammy nominees are preserved and correctly flagged |
| PV-07 | Grammy `updated_at` is not earlier than `published_at` |

All seven rules passed in the successful execution dated October 16, 2026.

### 6.3 Artist Integration Validation

**Script:** `src/integrate_artists.py`

| Rule | Validation |
|---|---|
| MAT-01 | Spotify artist associations have non-empty normalized names |
| MAT-02 | Grammy candidates have non-empty normalized names |
| MAT-03 | Every match uses exact normalized-name equality |
| MAT-04 | Output match keys are unique |

All four rules passed in the reported execution.

The matching strategy uses exact equality after name normalization. Fuzzy matching is disabled to avoid introducing unverified associations.

### 6.4 Data Warehouse Load Validation

**Script:** `src/load_dw.py`

The DW load process executes PREPARED validation as a preflight check and verifies the dimensional load using rules LD-01 through LD-06.

In the October 16, 2026 execution:

- All six load rules passed.
- No exceptions were reported.
- No duplicate track-genre fact keys were detected.
- No orphan rows were detected in either bridge table.
- The global load status was successful.

These controls demonstrate structural and referential consistency. They do not, by themselves, prove that every source-level artist association is semantically correct.

## 7. Transformation and Integration

### 7.1 Spotify Transformation

The Spotify transformation applies normalization and data quality decisions before producing the prepared track dataset.

The main operations include:

1. Handling duplicate `track_id + track_genre` combinations.
2. Normalizing audio feature scales.
3. Converting zero tempo values to NULL.
4. Standardizing text values used for integration.
5. Preparing artist associations.
6. Preserving the selected track-genre grain.

The resulting prepared Spotify dataset contains 113,550 rows.

### 7.2 Grammy Transformation

The Grammy transformation standardizes category and nominee-related values and preserves records with missing nominees.

The resulting prepared nomination dataset contains 4,810 rows.

Six missing nominee values remain explicitly flagged, preserving the source information without inventing nominee names.

### 7.3 Artist Integration Strategy

The integration is performed at the artist level.

The process prepares individual Spotify artist associations and Grammy artist candidates, normalizes their names, and matches records using exact normalized-name equality.

Reported integration metrics:

| Metric | Result |
|---|---:|
| Spotify prepared rows | 113,550 |
| Grammy prepared rows | 4,810 |
| Spotify individual artist associations | 157,530 |
| Spotify distinct artist names | 29,811 |
| Grammy artist candidates | 2,970 |
| Grammy distinct artist names | 1,656 |
| Matched distinct artist names | 524 |
| Unmatched Spotify artist names | 29,287 |
| Unmatched Grammy artist names | 1,132 |
| Confirmed track-Grammy matches | 31,925 |

The unmatched names are not automatically data quality failures. Differences in source coverage, spelling, naming conventions, and artist representation may prevent exact matches.

The approach deliberately avoids fuzzy matching. This reduces the risk of false positive associations but may also reduce match coverage.

**Business interpretation limitation:** A Spotify track associated with a Grammy record through an artist match is not necessarily the specific track nominated for or awarded a Grammy. The relationship supports artist-level analytical association, not proof of a direct track nomination or win.

## 8. Data Warehouse Architecture

The project separates the data lifecycle into logical layers.

- **RAW:** Stores extracted source data for reconciliation and initial quality validation.
- **PREPARED:** Stores normalized and integrated datasets ready for dimensional loading.
- **DW:** Stores the dimensional model used for analytical queries and dashboard consumption.

PostgreSQL is used as the database engine. Docker Compose provides the containerized execution environment, and Apache Airflow orchestrates the pipeline.

The database includes the schemas `raw`, `prepared`, `dw`, and `public`.

## 9. Dimensional Model and Granularity

The dimensional model supports the analytical requirements while preserving the selected track-genre grain.

### Dimensional Model Diagram

The following diagram illustrates the dimensional model, including the fact table, dimensions, bridge tables, key attributes, and relationships.

![Spotify and Grammy Awards Dimensional Model](docs/images/dimensional_model.png)


### 9.1 Fact Table

**`dw.fact_track`**

**Grain:** One Spotify track associated with one genre, identified by `track_id + track_genre`.

The table contains the track-level attributes and measures used in analysis, including popularity, duration, and musical features.

The selected grain allows a track to appear in multiple genres without incorrectly treating `track_id` as the unique row identifier.

### 9.2 Dimension Tables

- **`dw.dim_artist`:** Stores artist identities and normalized names.
- **`dw.dim_genre`:** Stores the distinct genre values.
- **`dw.dim_grammy_record`:** Stores Grammy-related records used for analytical association.

### 9.3 Bridge Tables

- **`dw.bridge_track_artist`:** Represents track-to-artist associations.
- **`dw.bridge_track_grammy`:** Represents associations between Spotify tracks and Grammy records established through the integration process.

The bridge tables support many-to-many relationships without duplicating the core fact records for each artist or Grammy association.

### 9.4 Reported Dimensional Row Counts

| Table | Rows |
|---|---:|
| `dw.fact_track` | 113,550 |
| `dw.dim_artist` | 30,943 |
| `dw.dim_genre` | 114 |
| `dw.dim_grammy_record` | 4,430 |
| `dw.bridge_track_artist` | 157,530 |
| `dw.bridge_track_grammy` | 31,917 |

The counts represent the reported successful load on October 16, 2026.

**Reconciliation note:** The PREPARED integration output reports 31,925 track-Grammy matches, whereas the dimensional bridge contains 31,917 rows. This eight-row difference remains a reconciliation item. The absence of duplicate or orphan bridge rows does not establish that the two counts should be identical or explain the difference.

## 10. Apache Airflow Orchestration

The pipeline is orchestrated by the `etl_workshop_pipeline` DAG.

### 10.1 DAG Tasks

1. `extract_spotify`
2. `extract_grammy`
3. `validate_raw_spotify`
4. `validate_raw_grammy`
5. `transform_and_integrate`
6. `validate_prepared`
7. `load_data_warehouse`

### 10.2 Dependency Design

The two extraction tasks execute independently.

Each extraction is followed by its corresponding RAW validation. Both RAW validations must succeed before the transformation and integration stage can proceed.

The PREPARED validation then verifies the transformed datasets, and the DW load performs its own preflight checks before inserting or updating dimensional records.

This design makes the quality gates part of the execution flow rather than treating validation as an optional post-processing activity.

### 10.3 Execution Environment

The project uses Docker Compose to manage the database and Airflow services.

Key components include:

- PostgreSQL for source layers, prepared datasets, and the data warehouse.
- Airflow API server for the web interface.
- Airflow scheduler and DAG processor for workflow management.
- Airflow worker for task execution.
- Redis and the Airflow metadata database for orchestration support.

Airflow UI: `http://localhost:8080`

The project database is `etl_workshop`.

Within the Docker network, the PostgreSQL service is addressed as `postgres` on port `5432`. Host-side database access uses port `5433`.

## 11. Failure Handling, Testing, and Repeatability

The project distinguishes data quality failures from technical execution errors.

### 11.1 Critical Validation Failures

Critical RAW rules act as execution gates. If a critical rule fails, the validation script raises an error and prevents downstream processing from proceeding normally.

This prevents invalid or incomplete source data from being treated as a successful pipeline input.

### 11.2 Technical Errors

A previous PREPARED validation test returned:

- `execution_status = ERROR`
- `global_success = false`
- `OperationalError`
- Connection refused on `localhost:59999`

This was a technical connectivity failure. No quality rules were evaluated because the database connection could not be established.

The error illustrates why operational failures must be distinguished from logical data quality failures.

### 11.3 Repeatability and Safe Reruns

The load implementation uses a transaction for the RAW truncate-and-load operation and passes the same database connection to the insertion process.

The dimensional loading process uses the defined keys and conflict-handling logic to manage existing records during repeated executions.

Repeatability should be verified by running the pipeline again and comparing row counts and key-level checks, rather than assuming that a successful run alone proves idempotency.

### 11.4 Successful Execution Evidence

The October 16, 2026 execution reported:

- Successful artist integration.
- Successful PREPARED validation.
- All seven PREPARED validation rules passing.
- All six DW load rules passing.
- No reported load exceptions.
- No duplicate fact track-genre keys.
- No orphan track-artist or track-Grammy bridge rows.
- `global_success = true`.

Evidence files available in the project include:

- `docs/evidence/artist_integration_latest.json`
- `docs/evidence/load_dw_latest.json`
- `docs/evidence/prepared_validation_latest.json`
- `docs/evidence/transform_integrate_latest.json`
- `docs/evidence/analytics_results.txt`

RAW validation evidence includes:

- `docs/evidence/raw_validation_20261014_120856_556312.json`
- `docs/evidence/raw_validation_20261014_120903_105677.json`

The repository should also include the actual evidence for the controlled critical-failure test, repeatability test, and dashboard execution where available.

## 12. Analytical Queries and Dashboard

The analytical SQL is maintained in:

**`sql/analytics_queries.sql`**

The queries support the three business requirements.

### RQ-01 — Average Popularity by Genre

The query calculates average Spotify popularity by genre for tracks associated with Grammy records.

### RQ-02 — Musical Characteristics by Genre

The query compares selected Spotify audio features by genre to support analysis of musical characteristics.

### RQ-03 — Popularity by Grammy Year and Category

The query groups current Spotify popularity by Grammy year and category using the available track-Grammy associations.

The dashboard consumes the dimensional data warehouse and presents the results in a form suitable for exploring these requirements.

### Interpretation and Coverage Limitations

The analytical results depend on the artist-level integration and the records represented in `dw.bridge_track_grammy`.

Therefore:

- The results do not necessarily represent every track in the Spotify dataset.
- The association does not prove that each linked track was directly nominated for or won a Grammy.
- The available `winner` field does not distinguish winning from non-winning records because only `True` was observed.
- The current Spotify popularity value cannot be used to reconstruct historical popularity trends without time-stamped historical observations.
- Exact normalized-name matching can leave legitimate artist matches unresolved.

These limitations should be considered when interpreting the dashboard results.

## 13. Repository Structure

The project follows a separation of concerns between orchestration, source data, transformation scripts, validation logic, SQL, and evidence.

```text
Workshop2/
├── dags/
│   └── etl_workshop_pipeline.py
├── data/
│   ├── spotify_dataset.csv
│   └── the_grammy_awards.csv
├── src/
│   ├── extract_spotify.py
│   ├── extract_grammy.py
│   ├── transform_integrate.py
│   ├── integrate_artists.py
│   ├── prepared_validation.py
│   └── load_dw.py
├── gx/
│   └── raw_validation.py
├── sql/
│   ├── dw_schema.sql
│   └── analytics_queries.sql
├── docs/
│   └── evidence/
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```

This is a representative structure. File placement should reflect the actual repository, and additional SQL, configuration, dashboard, or test files should be documented if present.

## 14. Setup and Execution

### 14.1 Prerequisites

- Docker Desktop with Docker Compose.
- Git.
- Access to the two source CSV files.
- Sufficient local resources to run PostgreSQL and the Airflow services.

### 14.2 Start the Environment

From the project root:

```powershell
docker compose up -d --build
```

Initialize Airflow if required by the environment:

```powershell
docker compose up airflow-init
```

Then start or verify the services:

```powershell
docker compose ps
```

The exact startup sequence depends on the current Docker Compose configuration and whether initialization has already been completed.

### 14.3 Access Airflow

Open:

```text
http://localhost:8080
```

Locate the `etl_workshop_pipeline` DAG, enable it if necessary, and trigger a run.

Monitor the task statuses and inspect task logs to verify that extraction, validation, transformation, integration, and DW loading complete successfully.

### 14.4 Verify the Data Warehouse

Connect to the PostgreSQL database `etl_workshop` using the connection details configured for the environment.

For host-side access, the default configuration uses port `5433`. Inside the Docker network, use the PostgreSQL service name and port `5432`.

Inspect the row counts in the `dw` schema and compare them with the latest successful evidence.

### 14.5 Execute Analytical Queries

Run the SQL statements in:

```text
sql/analytics_queries.sql
```

Use the results to verify the analytical outputs consumed by the dashboard.

**Configuration note:** Connection credentials and environment-specific values should be supplied through the project's environment configuration rather than hard-coded into new scripts.

## 15. Main Challenges and Implemented Solutions

| Challenge | Implemented solution |
|---|---|
| Duplicate track-genre observations | Profiled repeated keys and removed 450 additional rows |
| Inconsistent audio feature scales | Normalized numeric attributes |
| Tempo values equal to zero | Converted 157 values to NULL |
| Missing Grammy nominee names | Preserved and flagged six records |
| Different artist naming conventions | Applied normalized-name matching |
| Risk of false-positive integration | Disabled fuzzy matching |
| Quality failures affecting downstream processing | Introduced critical RAW and PREPARED validation gates |
| Technical database connection failures | Recorded execution errors separately from quality rule results |
| Complex pipeline dependencies | Orchestrated extraction, validation, transformation, and loading with Airflow |
| Many-to-many artist and Grammy relationships | Represented associations through dimensional bridge tables |

## 16. Assumptions and Limitations

1. The selected Spotify analytical grain is one track-genre observation, identified by `track_id + track_genre`.
2. The Spotify source does not provide the historical popularity snapshots required to analyze changes in popularity over time.
3. Artist integration uses exact equality after normalization and does not use fuzzy matching.
4. Grammy associations are made at the artist level and do not establish direct track nomination or award status.
5. The constant `winner` source value prevents distinguishing winners from non-winners using that field.
6. Unmatched artist names are retained as unmatched rather than being assigned speculative identities.
7. The eight-row difference between prepared track-Grammy matches and the dimensional bridge requires reconciliation before claiming full match preservation.
8. The reported row counts describe the documented successful execution and may change if the source data or transformation logic changes.

## 17. Conclusion

This project establishes a reproducible ETL workflow that transforms Spotify and Grammy source datasets into a dimensional data warehouse for analytical consumption.

The implementation combines source profiling, data quality controls, normalization, artist-level integration, dimensional modeling, Airflow orchestration, and SQL-based analytics.

The resulting warehouse supports the three defined business requirements through genre-level musical analysis and Grammy-related artist associations.

The main technical outcome is a structured and validated data pipeline. The main analytical caveat is that the integration represents artist-level associations rather than verified direct relationships between individual Spotify tracks and Grammy nominations or wins.

The quality of the analytical conclusions therefore depends on understanding the selected grain, integration strategy, source limitations, and reconciliation checks documented in this README.


## 16. Conclusion

This workshop demonstrates an ETL architecture that separates orchestration from processing logic, applies data-quality checks before analytical loading, integrates sources with different structures, and exposes the resulting data through a dimensional warehouse and Power BI. The reliability of the final system is demonstrated through validation results, controlled failure behavior, repeatability tests, and reproducible setup instructions.
