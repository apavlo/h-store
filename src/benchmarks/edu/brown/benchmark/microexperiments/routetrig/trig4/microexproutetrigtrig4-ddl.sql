
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

CREATE STREAM proc_one_out
(
  a_id integer     NOT NULL
, a_val   integer NOT NULL
, CONSTRAINT PK_1 PRIMARY KEY
  (
    a_id
  )
);

CREATE STREAM proc_two_out
(
  a_id integer     NOT NULL
, a_val   integer NOT NULL
, CONSTRAINT PK_2 PRIMARY KEY
  (
    a_id
  )
);

CREATE STREAM proc_three_out
(
  a_id integer     NOT NULL
, a_val   integer NOT NULL
, CONSTRAINT PK_3 PRIMARY KEY
  (
    a_id
  )
);

CREATE STREAM proc_four_out
(
  a_id integer     NOT NULL
, a_val   integer NOT NULL
, CONSTRAINT PK_4 PRIMARY KEY
  (
    a_id
  )
);

CREATE STREAM proc_five_out
(
  a_id integer     NOT NULL
, a_val   integer NOT NULL
, CONSTRAINT PK_5 PRIMARY KEY
  (
    a_id
  )
);

CREATE STREAM proc_six_out
(
  a_id integer     NOT NULL
, a_val   integer NOT NULL
, CONSTRAINT PK_6 PRIMARY KEY
  (
    a_id
  )
);

CREATE STREAM proc_seven_out
(
  a_id integer     NOT NULL
, a_val   integer NOT NULL
, CONSTRAINT PK_7 PRIMARY KEY
  (
    a_id
  )
);

CREATE STREAM proc_eight_out
(
  a_id integer     NOT NULL
, a_val   integer NOT NULL
, CONSTRAINT PK_8 PRIMARY KEY
  (
    a_id
  )
);

CREATE STREAM proc_nine_out
(
  a_id integer     NOT NULL
, a_val   integer NOT NULL
, CONSTRAINT PK_9 PRIMARY KEY
  (
    a_id
  )
);

CREATE STREAM proc_ten_out
(
  a_id integer     NOT NULL
, a_val   integer NOT NULL
, CONSTRAINT PK_10 PRIMARY KEY
  (
    a_id
  )
);
