-- contestants table holds the contestants numbers (for voting) and names
CREATE TABLE contestants
(
  contestant_number integer     NOT NULL
, contestant_name   varchar(50) NOT NULL
, CONSTRAINT PK_contestants PRIMARY KEY
  (
    contestant_number
  )
);

CREATE STREAM S1
(
  vote_id            bigint     NOT NULL,
  phone_number       bigint     NOT NULL
, state              varchar(2) NOT NULL -- REFERENCES area_code_state (state)
, contestant_number  integer    NOT NULL REFERENCES contestants (contestant_number)
, created            timestamp  NOT NULL
);

CREATE WINDOW w_rows ON S1 ROWS 100 SLIDE 10;

CREATE TABLE leaderboard
(
  contestant_number  integer   NOT NULL
, numvotes           integer   NOT NULL
, CONSTRAINT PK_leaderboard PRIMARY KEY
  (
    contestant_number
  )

);

