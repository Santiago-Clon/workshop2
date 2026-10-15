CREATE SCHEMA IF NOT EXISTS raw;

DROP TABLE IF EXISTS raw.grammy;

CREATE TABLE raw.grammy (
    year         TEXT,
    title        TEXT,
    published_at TEXT,
    updated_at   TEXT,
    category     TEXT,
    nominee      TEXT,
    artist       TEXT,
    workers      TEXT,
    img          TEXT,
    winner       TEXT
);

CREATE SCHEMA IF NOT EXISTS raw;


-- ---------------------------------------------------------
-- Grammy RAW
-- ---------------------------------------------------------

DROP TABLE IF EXISTS raw.grammy;

CREATE TABLE raw.grammy (
    year TEXT,
    title TEXT,
    published_at TEXT,
    updated_at TEXT,
    category TEXT,
    nominee TEXT,
    artist TEXT,
    workers TEXT,
    img TEXT,
    winner TEXT
);


-- ---------------------------------------------------------
-- Spotify RAW
-- ---------------------------------------------------------

DROP TABLE IF EXISTS raw.spotify;

CREATE TABLE raw.spotify (
    "Column1" TEXT,
    track_id TEXT,
    artists TEXT,
    album_name TEXT,
    track_name TEXT,
    popularity TEXT,
    duration_ms TEXT,
    explicit TEXT,
    danceability TEXT,
    energy TEXT,
    key TEXT,
    loudness TEXT,
    mode TEXT,
    speechiness TEXT,
    acousticness TEXT,
    instrumentalness TEXT,
    liveness TEXT,
    valence TEXT,
    tempo TEXT,
    time_signature TEXT,
    track_genre TEXT
);