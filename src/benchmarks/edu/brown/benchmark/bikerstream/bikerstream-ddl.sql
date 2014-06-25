-- ===========
-- BASE TABLES
-- ===========

-- Station table keeps track of each collection of docks.
CREATE TABLE stations
(
    station_id INTEGER     PRIMARY KEY
,   location   VARCHAR(64) NOT NULL
,   lat        INTEGER     NOT NULL
,   lon        INTEGER     NOT NULL
,   num_bikes  INTEGER     NOT NULL
,   num_docks  INTEGER     NOT NULL
);

-- Keep track of riders in the system.
CREATE TABLE riders (
    rider_id  Integer     PRIMARY KEY,
    f_name    VARCHAR(32) NOT NULL,
    l_name    VARCHAR(32) NOT NULL,
);

-- Track payment options for each rider.
CREATE TABLE cards (
    rider_id INTEGER     REFERENCES riders(rider_id),
    bank     VARCHAR(32) NOT NULL,
    name     VARCHAR(32) NOT NULL,
    num      VARCHAR(20) NOT NULL,
    exp      VARCHAR(32) NOT NULL,
    sec_code INTEGER     NOT NULL
);

CREATE TABLE logs
(
    rider_id INTEGER   NOT NULL REFERENCES riders(rider_id),
    time     TIMESTAMP NOT NULL,
    success  INTEGER   NOT NULL,
    action   VARCHAR(64),
);

-- =============
-- STREAM TABLES
-- =============

CREATE TABLE bikeStatus (
    rider        INTEGER REFERENCES riders(rider_id),
    last_time    TIMESTAMP,
    speed        FLOAT,
    last_lat     BIGINT,
    last_lon     BIGINT,
    tot_time     INTEGER,
    tot_Distance FLOAT
);


-- Track bike positions by point.
CREATE TABLE bikereadings_table
(
  bike_id integer        NOT NULL
, reading_time timestamp NOT NULL
, reading_lat bigint     NOT NULL
, reading_lon bigint     NOT NULL
-- CONSTRAINT PK_t_bikereadings PRIMARY KEY
-- (
--   bike_id
-- , reading_time --- not sure this syntax is correct ...
-- )
);

-- Count readings by bike.
CREATE TABLE count_bikereadings_table
(
  bike_id    integer   NOT NULL
, count_time timestamp NOT NULL
, count_val  integer   NOT NULL
---, CONSTRAINT PK_t_bikereadingsPRIMARY KEY
---  (
---    bike_id
---  , reading_time --- not sure this syntax is correct ...
---  )
);

-- Streams for bike readings.
CREATE STREAM bikereadings_stream
(
  bike_id      INTEGER   NOT NULL
, reading_time timestamp NOT NULL
, reading_lat  FLOAT     NOT NULL
, reading_lon  FLOAT     NOT NULL
);

--- Window over the bikereadings_stream.
CREATE WINDOW bikereadings_window_rows ON bikereadings_stream ROWS 100 SLIDE 10;
