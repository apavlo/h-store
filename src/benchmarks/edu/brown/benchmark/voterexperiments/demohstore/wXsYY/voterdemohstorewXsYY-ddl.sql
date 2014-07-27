
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
--   voterdemohstores are not allowed to submit more than <x> votes, x is passed to client application
CREATE TABLE votes
(
  vote_id            bigint     NOT NULL,
  phone_number       bigint     NOT NULL
, state              varchar(2) NOT NULL -- REFERENCES area_code_state (state)
, contestant_number  integer    NOT NULL REFERENCES contestants (contestant_number)
, created	     timestamp  NOT NULL
, CONSTRAINT PK_votes PRIMARY KEY
  (
    vote_id
  )
-- PARTITION BY ( phone_number )
);

CREATE TABLE proc_one_out
(
  vote_id            bigint     NOT NULL,
  phone_number       bigint     NOT NULL
, state              varchar(2) NOT NULL -- REFERENCES area_code_state (state)
, contestant_number  integer    NOT NULL REFERENCES contestants (contestant_number)
, created	     timestamp    NOT NULL
);

CREATE TABLE w_staging
(
  vote_id            bigint     NOT NULL,
  phone_number       bigint     NOT NULL
, state              varchar(2) NOT NULL -- REFERENCES area_code_state (state)
, contestant_number  integer    NOT NULL REFERENCES contestants (contestant_number)
, created            timestamp  NOT NULL
, win_id             bigint     NOT NULL
, CONSTRAINT PK_stage PRIMARY KEY
  (
    win_id
  )
-- PARTITION BY ( phone_number )
);

CREATE TABLE w_rows
(
  vote_id            bigint     NOT NULL,
  phone_number       bigint     NOT NULL
, state              varchar(2) NOT NULL -- REFERENCES area_code_state (state)
, contestant_number  integer    NOT NULL REFERENCES contestants (contestant_number)
, created            timestamp  NOT NULL
, win_id             bigint     NOT NULL
, CONSTRAINT PK_win PRIMARY KEY
  (
    win_id
  )
-- PARTITION BY ( phone_number )
);

CREATE TABLE leaderboard
(
  --phone_number       bigint    NOT NULL,
  contestant_number  integer   NOT NULL
, num_votes          integer
, CONSTRAINT PK_leaderboard PRIMARY KEY
  (
    contestant_number
  )
);

CREATE TABLE votes_count
(
  row_id	     integer    NOT NULL,
  cnt		     integer    NOT NULL
);

CREATE TABLE staging_count
(
  row_id	     integer    NOT NULL,
  cnt		     integer    NOT NULL
);

CREATE TABLE current_win_id
(
  row_id	     integer    NOT NULL,
  win_id     bigint    NOT NULL
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

--CREATE VIEW v_top_three_last_30_sec
--(
--  contestant_number, num_votes
--)
--AS
--   SELECT contestant_number
--        , count(*) 
--   FROM w_rows t
--   GROUP BY t.contestant_number
--;

