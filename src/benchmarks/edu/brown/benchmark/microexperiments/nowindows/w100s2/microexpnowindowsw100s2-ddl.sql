
-- contestants table holds the contestants numbers (for voting) and names
CREATE TABLE A_STAGING
(
  win_id integer    NOT NULL
,  a_id integer     NOT NULL
, a_val   integer NOT NULL
, CONSTRAINT PK_stage PRIMARY KEY
  (
    win_id
  )
);

CREATE TABLE A_WIN
(
 win_id integer    NOT NULL
, a_id integer     NOT NULL
, a_val   integer NOT NULL
, CONSTRAINT PK_win PRIMARY KEY
  (
    win_id
  )
);

CREATE TABLE state_tbl
(
  row_id  integer  NOT NULL
, cnt     integer  NOT NULL
, current_win_id integer NOT NULL
);
