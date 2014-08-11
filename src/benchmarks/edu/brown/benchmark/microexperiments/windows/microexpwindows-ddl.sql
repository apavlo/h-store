
-- contestants table holds the contestants numbers (for voting) and names
CREATE TABLE a_tbl
(
  a_id integer     NOT NULL
, a_val   integer NOT NULL
, CONSTRAINT PK_a PRIMARY KEY
  (
    a_id
  )
);

CREATE STREAM A_STREAM
(
  a_id integer     NOT NULL
, a_val   integer NOT NULL
);

CREATE WINDOW A_WIN ON A_STREAM ROWS 100 SLIDE 5;
