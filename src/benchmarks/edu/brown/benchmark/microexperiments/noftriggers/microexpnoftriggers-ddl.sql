
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

CREATE TABLE b_tbl
(
  b_id integer     NOT NULL
, b_val   integer NOT NULL
, CONSTRAINT PK_b PRIMARY KEY
  (
    b_id
  )
);

CREATE TABLE c_tbl
(
  c_id integer     NOT NULL
, c_val   integer NOT NULL
, CONSTRAINT PK_c PRIMARY KEY
  (
    c_id
  )
);

CREATE STREAM proc_one_out
(
  a_id integer     NOT NULL
, a_val   integer NOT NULL
);

CREATE STREAM proc_two_out
(
  b_id integer     NOT NULL
, b_val   integer NOT NULL
);
