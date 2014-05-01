
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

-- Map of Area Codes and States for geolocation classification of incoming calls
CREATE TABLE area_code_state
(
  area_code smallint   NOT NULL
, state     varchar(2) NOT NULL
, CONSTRAINT PK_area_code_state PRIMARY KEY
  (
    area_code
  )
);

-- votes table holds every valid vote.
--   voterdemosstorepetrigonlys are not allowed to submit more than <x> votes, x is passed to client application
CREATE TABLE votes
(
  vote_id            bigint     NOT NULL,
  phone_number       bigint     NOT NULL
, state              varchar(2) NOT NULL -- REFERENCES area_code_state (state)
, contestant_number  integer    NOT NULL REFERENCES contestants (contestant_number)
, ts		     integer    NOT NULL
, CONSTRAINT PK_votes PRIMARY KEY
  (
    vote_id
  )
-- PARTITION BY ( phone_number )
);

CREATE TABLE voteCount
(
  row_id	     integer    NOT NULL,
  cnt		     integer    NOT NULL

, CONSTRAINT PK_voteCount PRIMARY KEY
  (
    row_id
  )
);

CREATE TABLE totalVoteCount
(
  row_id	     integer    NOT NULL,
  cnt		     integer    NOT NULL

, CONSTRAINT PK_totalVoteCount PRIMARY KEY
  (
    row_id
  )
);

CREATE TABLE totalLeaderboardCount
(
  row_id	     integer    NOT NULL,
  cnt		     integer    NOT NULL

, CONSTRAINT PK_totalLeaderboardCount PRIMARY KEY
  (
    row_id
  )
);

CREATE TABLE win_timestamp
(
   row_id            integer   NOT NULL,
   ts	     integer   NOT NULL

, CONSTRAINT PK_win_timestamp PRIMARY KEY
  (
    row_id
  )
);

CREATE TABLE stage_timestamp
(
   row_id            integer   NOT NULL,
   ts	     integer   NOT NULL

, CONSTRAINT PK_stage_timestamp PRIMARY KEY
  (
    row_id
  )
);

-- rollup of votes by phone number, used to reject excessive voting
CREATE VIEW v_votes_by_phone_number
(
  phone_number
, num_votes
)
AS
   SELECT phone_number
        , COUNT(*)
     FROM votes
 GROUP BY phone_number
;

-- rollup of votes by contestant and state for the heat map and results
CREATE VIEW v_votes_by_contestant_number_state
(
  contestant_number
, state
, num_votes
)
AS
   SELECT contestant_number
        , state
        , COUNT(*)
     FROM votes
 GROUP BY contestant_number
        , state
;

CREATE VIEW v_votes_by_contestant
(
  contestant_number
, num_votes
)
AS
   SELECT contestant_number
        , COUNT(*)
     FROM votes
 GROUP BY contestant_number
;

CREATE STREAM proc_one_out
(
  vote_id            bigint     NOT NULL,
  phone_number       bigint     NOT NULL
, state              varchar(2) NOT NULL
, contestant_number  integer    NOT NULL
, time		     integer    NOT NULL
);

CREATE INDEX idx_ts_votes ON VOTES (ts);
