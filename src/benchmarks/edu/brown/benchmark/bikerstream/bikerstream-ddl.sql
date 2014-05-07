
-- contestants table holds the contestants numbers (for voting) and names
CREATE TABLE bikereadings_table
(
  bike_id integer     NOT NULL
, reading_time timestamp NOT NULL
, reading_lat bigint NOT NULL
, reading_lon bigint NOT NULL
---, CONSTRAINT PK_t_bikereadingsPRIMARY KEY
---  (
---    bike_id
---  , reading_time --- not sure this syntax is correct ...
---  )
);

-- table of count of readings by bike
CREATE TABLE cnt_bikereadings_table
(
  bike_id integer     NOT NULL
, cnt_time timestamp NOT NULL
, cnt_val integer NOT NULL
---, CONSTRAINT PK_t_bikereadingsPRIMARY KEY
---  (
---    bike_id
---  , reading_time --- not sure this syntax is correct ...
---  )
);

-- streams for bike readings ---
CREATE STREAM bikereadings_stream
(
  bike_id integer     NOT NULL
, reading_time timestamp NOT NULL
, reading_lat bigint NOT NULL
, reading_lon bigint NOT NULL
);


--- Window over the bikereadings_stream
CREATE WINDOW bikereadings_window_rows ON bikereadings_stream ROWS 100 SLIDE 10;


