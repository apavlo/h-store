
-- contestants table holds the contestants numbers (for voting) and names
CREATE TABLE bikereadings 
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

-- streams for bike readings ---
CREATE STREAM bikereadings_stream
(
  bike_id integer     NOT NULL
, reading_time timestamp NOT NULL
, reading_lat bigint NOT NULL
, reading_lon bigint NOT NULL
);


--- CREATE WINDOW W_ROWS ON S1 ROWS 100 SLIDE 10;


