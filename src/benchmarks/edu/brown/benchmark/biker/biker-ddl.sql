-- create a table to hold stations
CREATE TABLE stations
(
  station_id    INTEGER     NOT NULL
, location_text VARCHAR(50) NOT NULL
, lat           FLOAT      NOT NULL
, lon           FLOAT      NOT NULL
, CONSTRAINT PK_stations PRIMARY KEY
  (
    station_id
  )
);

-- create a table to hold docks and info about them
CREATE TABLE docks
(
  dock_id    INTEGER NOT NULL
, bike_id    INTEGER
, station_id INTEGER NOT NULL
, CONSTRAINT PK_docks PRIMARY KEY
  (
    dock_id
  )
);
  -- would like a constraint that said that bike_id cannot
  -- be null in the docks table if there is a bike reservation

CREATE TABLE reservations
(
  dock_id              INTEGER NOT NULL
, is_bike              INTEGER NOT NULL -- 1 if is a reservation for a bike
                                        -- 0 is if a reservation for a dock
, set_time             TIMESTAMP NOT NULL
, CONSTRAINT PK_reservations PRIMARY KEY -- only one reservation per dock at a time
 (
    dock_id
 )
);
