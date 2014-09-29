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

CREATE TABLE stationStatus
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
,   user_id        INTEGER references users(user_id)
,   station_id     INTEGER references stations(station_id)
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


CREATE TABLE nearByStations (
    user_id     INTEGER NOT NULL REFERENCES users(user_id)
,   station_id  INTEGER NOT NULL REFERENCES stations(station_id)
);


CREATE TABLE nearByDiscounts (
    user_id     INTEGER NOT NULL REFERENCES users(user_id)
,   station_id  INTEGER NOT NULL REFERENCES stations(station_id)
);


-- to be 'insert into' by ProcessBikeStatus
CREATE TABLE riderPositions (
    user_id   INTEGER NOT NULL REFERENCES users(user_id)
,   latitude  FLOAT   NOT NULL
,   longitude FLOAT   NOT NULL
,   time      TIMESTAMP NOT NULL
);

-- to be updated by DetectAnomalies
CREATE TABLE anomalies (
    user_id   INTEGER references users(user_id)
,   status    INTEGER NOT NULL
);


-- to be updated by
CREATE TABLE recentRiderArea (
    latitude_1  FLOAT   NOT NULL
,   longitude_1 FLOAT   NOT NULL
,   latitude_2  FLOAT   NOT NULL
,   longitude_2 FLOAT   NOT NULL
,   sqr_mile    FLOAT   NOT NULL
);


-- to be updated by
CREATE TABLE recentRiderSummary (
    rider_count INTEGER NOT NULL
,   speed_max   FLOAT   NOT NULL
,   speed_min   FLOAT   NOT NULL
,   speed_avg   FLOAT   NOT NULL
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
CREATE WINDOW lastNBikeStatus ON bikeStatus ROWS 100 SLIDE 1;


-- to be fed by UpdateNearByStations
CREATE STREAM s1 (
    user_id   INTEGER   NOT NULL REFERENCES users(user_id)
);


-- to be fed by CalculateSpeed
CREATE STREAM riderSpeeds (
    user_id   INTEGER   NOT NULL REFERENCES users(user_id)
,   speed     FLOAT     NOT NULL
);
CREATE WINDOW lastNRiderSpeeds ON riderSpeeds ROWS 100 SLIDE 1;


-- to be fed by ProcessBikeStatus
CREATE STREAM s3 (
    user_id   INTEGER   NOT NULL REFERENCES users(user_id)
,   latitude  FLOAT     NOT NULL
,   longitude FLOAT     NOT NULL
);


-- ------------------------- ^ Locked in tables ^ ------------------------------

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