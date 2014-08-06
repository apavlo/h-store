
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
--   voterdemosstores are not allowed to submit more than <x> votes, x is passed to client application
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

CREATE STREAM s1
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

--CREATE WINDOW trending_leaderboard ON proc_one_out RANGE 30 SLIDE 2;

CREATE STREAM s1prime
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s2
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s2prime
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s3
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s3prime
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s4
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s4prime
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);
