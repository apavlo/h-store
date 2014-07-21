-- ===========
-- BASE TABLES
-- ===========

-- Station table keeps track of each collection of docks.
CREATE TABLE stations
(
    station_id     INTEGER      PRIMARY KEY
,   station_name   VARCHAR(64)  NOT NULL
,   street_address VARCHAR(128) NOT NULL
,   latitude       FLOAT        NOT NULL
,   longitude      FLOAT        NOT NULL
);

CREATE TABLE StationStatus
(
    station_id            INTEGER NOT NULL REFERENCES stations(station_id)
,   current_bikes         INTEGER NOT NULL
,   current_docks         INTEGER NOT NULL
,   current_discount      INTEGER NOT NULL
);

-- Keep track of riders in the system.
CREATE TABLE users (
    user_id                     Integer     PRIMARY KEY
,   user_name                   VARCHAR(32) NOT NULL
,   credit_card                 VARCHAR(32) NOT NULL
,   membership_status           INTEGER     NOT NULL -- 0=expired | 1=active
,   membership_expiration_date  TIMESTAMP   NOT NULL
);

CREATE TABLE bikes (
    bike_id        INTEGER PRIMARY KEY
,   current_status INTEGER NOT NULL -- 1=docked 2=riding
);


CREATE TABLE logs
(
    user_id  INTEGER   NOT NULL REFERENCES users(user_id)
,   time     TIMESTAMP NOT NULL
,   success  INTEGER   NOT NULL
,   action   VARCHAR(128)
);


CREATE TABLE ride
(
    user_id  INTEGER NOT NULL REFERENCES users(user_id)
,   start_station INTEGER NOT NULL REFERENCES stations(station_id)
,   pos_end_station INTEGER NOT NULL REFERENCES stations(station_id)
,   def_end_station INTEGER REFERENCES stations(station_id)
);

-- =============
-- STREAM TABLES
-- =============

CREATE STREAM bikeStatus (
    user_id   INTEGER   NOT NULL REFERENCES users(user_id)
,   latitude  FLOAT     NOT NULL
,   longitude FLOAT     NOT NULL
,   time      TIMESTAMP NOT NULL
);


-- ------------------------- ^ Locked in tables ^ ------------------------------

--- Window over the bikereadings_stream.
CREATE WINDOW bikerstream_window ON bikestatus ROWS 100 SLIDE 10;

CREATE WINDOW lastNTuples ON bikestatus ROWS 100 SLIDE 10;

CREATE TABLE discounts
(
    user_id INTEGER NOT NULL REFERENCES users(user_id)
,   station_id INTEGER NOT NULL REFERENCES stations(station_id)
);

CREATE TABLE userLocations (
    user_id   INTEGER PRIMARY KEY REFERENCES users(user_id),
    latitude  FLOAT   NOT NULL,
    longitude FLOAT   NOT NULL
    );

