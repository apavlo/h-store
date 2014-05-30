-- ===========
-- BASE TABLES
-- ===========

CREATE TABLE zones (
    zone_id    INTEGER PRIMARY KEY,
    discount   FLOAT   NOT NULL
);

-- Station table keeps track of each collection of docks.
CREATE TABLE stations
(
    station_id INTEGER     PRIMARY KEY
,   zone_id    INTEGER     REFERENCES zones(zone_id)
,   location   VARCHAR(64) NOT NULL
,   lat        INTEGER     NOT NULL
,   lon        INTEGER     NOT NULL
);

-- Each dock and whether or not a bike is stationed there, denoted
-- by whether or not a bike_id is NULL or not.
CREATE TABLE docks
(
    dock_id    INTEGER PRIMARY KEY
,   station_id INTEGER NOT NULL REFERENCES stations(station_id)
,   occupied   INTEGER NOT NULL -- ( 1=Full | 0=Empty)
);

-- The following query will give us the discount for each dock
-- select d.dock_id, z.discount from docks d, stations s, zones z WHERE d.station_id = s.station_id AND s.zone_id = z.zone_id

-- Keep track of all bikes on record.
CREATE TABLE bikes
(
    bike_id    INTEGER     PRIMARY KEY,
    zone_id    INTEGER     REFERENCES zones(zone_id),
    condition  VARCHAR(32),
    state      VARCHAR(16) NOT NULL, -- (DOCKED|RESERVED|OUT|RETURNING|STOLEN)
    last_dock  INTEGER     NOT NULL,
    last_rider INTEGER     NOT NULL
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


-- ==============
-- DYNAMIC TABLES
-- ==============


CREATE TABLE bikeres
(
    bike_id  INTEGER NOT NULL REFERENCES bikes(bike_id),
    dock_id  INTEGER NOT NULL REFERENCES docks(dock_id),
    rider_id INTEGER NOT NULL REFERENCES riders(rider_id),
    valid    INTEGER NOT NULL,
    time     TIMESTAMP
);

CREATE TABLE dockres
(
    dock_id  INTEGER NOT NULL REFERENCES docks(dock_id),
    bike_id  INTEGER NOT NULL REFERENCES bikes(bike_id),
    rider_id INTEGER NOT NULL REFERENCES riders(rider_id),
    discount FLOAT   NOT NULL,
    valid    INTEGER NOT NULL,
    time     TIMESTAMP
);


-- =============
-- STATIC TABLES
-- =============

-- Track basic trip information.
CREATE TABLE trips (
    trip_id INTEGER PRIMARY KEY,
    dock_i  INTEGER REFERENCES docks(dock_id) NOT NULL,
    dock_f  INTEGER REFERENCES docks(dock_id),
    time_i  TIMESTAMP,
    time_f  TIMESTAMP
);

-- Tracks payments made by each rider for each ride.
CREATE TABLE payments (
    rider INTEGER REFERENCES riders(rider_id),
    ride  INTEGER REFERENCES trips(trip_id),
    amount FLOAT
);

-- =============
-- STREAM TABLES
-- =============

CREATE TABLE bikeStatus (
    rider        INTEGER REFERENCES riders(rider_id),
    bike_id      INTEGER REFERENCES bikes(bike_id),
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
---, CONSTRAINT PK_t_bikereadingsPRIMARY KEY
---  (
---    bike_id
---  , reading_time --- not sure this syntax is correct ...
---  )
);

-- Count readings by bike.
CREATE TABLE count_bikereadings_table
(
  bike_id integer     NOT NULL
, count_time timestamp NOT NULL
, count_val integer NOT NULL
---, CONSTRAINT PK_t_bikereadingsPRIMARY KEY
---  (
---    bike_id
---  , reading_time --- not sure this syntax is correct ...
---  )
);

-- Streams for bike readings.
CREATE STREAM bikereadings_stream
(
  bike_id integer     NOT NULL
, reading_time timestamp NOT NULL
, reading_lat bigint NOT NULL
, reading_lon bigint NOT NULL
);

--- Window over the bikereadings_stream.
CREATE WINDOW bikereadings_window_rows ON bikereadings_stream ROWS 100 SLIDE 10;


