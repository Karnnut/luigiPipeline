CREATE ROLE postgres WITH LOGIN SUPERUSER PASSWORD 'postgres';
-- Create a dedicated database
CREATE DATABASE weather_data;

-- Connect to it
\connect weather_data;

CREATE USER weather_user WITH PASSWORD 'weather_password';
GRANT ALL PRIVILEGES ON DATABASE weather_data TO weather_user;

CREATE TABLE IF NOT EXISTS weather_data (
    time TIME,
    isobaricInhPa DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    number INTEGER,
    step INTERVAL,
    valid_time DATE,
    d DOUBLE PRECISION,
    cc DOUBLE PRECISION,
    z DOUBLE PRECISION,
    o3 DOUBLE PRECISION,
    pv DOUBLE PRECISION,
    r DOUBLE PRECISION,
    ciwc DOUBLE PRECISION,
    clwc DOUBLE PRECISION,
    q DOUBLE PRECISION,
    crwc DOUBLE PRECISION,
    cswc DOUBLE PRECISION,
    t DOUBLE PRECISION,
    u DOUBLE PRECISION,
    v DOUBLE PRECISION,
    w DOUBLE PRECISION,
    vo DOUBLE PRECISION,
    date DATE,
    tp DOUBLE PRECISION,
    surface DOUBLE PRECISION
);
