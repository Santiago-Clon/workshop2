-- RQ-01: Popularidad promedio por género musical.
-- Cada track_key se cuenta una sola vez dentro de su género.

WITH eligible_tracks AS (
    SELECT DISTINCT track_key
    FROM dw.bridge_track_grammy
)
SELECT
    g.genre_name AS genero,
    COUNT(*) AS pistas_asociadas,
    COUNT(f.popularity) AS pistas_con_popularidad,
    ROUND(AVG(f.popularity)::NUMERIC, 2) AS popularidad_promedio
FROM eligible_tracks AS e
JOIN dw.fact_track AS f
    ON f.track_key = e.track_key
JOIN dw.dim_genre AS g
    ON g.genre_key = f.genre_key
GROUP BY g.genre_key, g.genre_name
ORDER BY popularidad_promedio DESC NULLS LAST;


-- RQ-02: Características musicales promedio por género.
-- Se calcula una observación por track_key.

WITH eligible_tracks AS (
    SELECT DISTINCT track_key
    FROM dw.bridge_track_grammy
)
SELECT
    g.genre_name AS genero,
    COUNT(*) AS pistas_asociadas,
    ROUND(AVG(f.danceability), 4) AS danceability_promedio,
    ROUND(AVG(f.energy), 4) AS energy_promedio,
    ROUND(AVG(f.loudness), 3) AS loudness_promedio,
    ROUND(AVG(f.speechiness), 4) AS speechiness_promedio,
    ROUND(AVG(f.acousticness), 4) AS acousticness_promedio,
    ROUND(AVG(f.instrumentalness), 4) AS instrumentalness_promedio,
    ROUND(AVG(f.liveness), 4) AS liveness_promedio,
    ROUND(AVG(f.valence), 4) AS valence_promedio,
    ROUND(AVG(f.tempo), 2) AS tempo_promedio,
    ROUND(AVG(f.duration_ms), 2) AS duracion_promedio_ms
FROM eligible_tracks AS e
JOIN dw.fact_track AS f
    ON f.track_key = e.track_key
JOIN dw.dim_genre AS g
    ON g.genre_key = f.genre_key
GROUP BY g.genre_key, g.genre_name
ORDER BY g.genre_name;


-- RQ-03: Popularidad por año y categoría Grammy.
-- Cada pista se cuenta una sola vez dentro de cada combinación
-- de año y categoría.

WITH eligible_track_records AS (
    SELECT DISTINCT
        track_key,
        grammy_record_key
    FROM dw.bridge_track_grammy
)
SELECT
    d.year AS anio,
    d.category AS categoria_grammy,
    COUNT(*) AS pistas_asociadas,
    COUNT(f.popularity) AS pistas_con_popularidad,
    ROUND(AVG(f.popularity)::NUMERIC, 2) AS popularidad_promedio,
    MIN(f.popularity) AS popularidad_minima,
    MAX(f.popularity) AS popularidad_maxima
FROM eligible_track_records AS e
JOIN dw.fact_track AS f
    ON f.track_key = e.track_key
JOIN dw.dim_grammy_record AS d
    ON d.grammy_record_key = e.grammy_record_key
GROUP BY
    d.year,
    d.grammy_record_key,
    d.category
ORDER BY
    d.year,
    d.category;