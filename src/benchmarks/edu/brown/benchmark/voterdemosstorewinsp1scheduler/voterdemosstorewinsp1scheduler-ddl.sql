
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
--   voterdemosstorewinsp1s are not allowed to submit more than <x> votes, x is passed to client application
CREATE TABLE votes
(
  vote_id            bigint     NOT NULL,
  phone_number       bigint     NOT NULL
, state              varchar(2) NOT NULL -- REFERENCES area_code_state (state)
, contestant_number  integer    NOT NULL REFERENCES contestants (contestant_number)
, time		     integer    NOT NULL
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

CREATE STREAM proc_one_out
(
  vote_id            bigint     NOT NULL,
  phone_number       bigint     NOT NULL
, state              varchar(2) NOT NULL
, contestant_number  integer    NOT NULL
, time		     integer    NOT NULL
);

CREATE WINDOW trending_leaderboard ON proc_one_out RANGE 30 SLIDE 1;

CREATE TABLE top_three_last_30_sec
(
  --phone_number       bigint    NOT NULL,
  contestant_number  integer   NOT NULL
, num_votes          integer
, CONSTRAINT PK_topThree PRIMARY KEY
  (
    contestant_number
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

