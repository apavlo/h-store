
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

CREATE TABLE txntotal
(
  txn                bigint     NOT NULL
);

CREATE STREAM s1input
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

--CREATE WINDOW trending_leaderboard ON proc_one_out RANGE 30 SLIDE 2;

CREATE STREAM s1
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s101
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s102
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s103
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s104
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s105
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s106
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s107
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s108
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s109
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s110
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s111
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s112
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s113
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s114
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s115
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s116
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s117
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s118
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s119
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s120
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s121
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s122
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s123
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s124
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s125
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s126
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s127
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s128
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s129
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s130
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s131
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s201
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s202
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s203
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s204
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s205
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s206
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s207
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s208
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s209
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s210
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s211
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s212
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s213
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s214
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s215
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s216
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s217
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s218
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s219
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s220
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s221
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s222
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s223
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s224
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s225
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s226
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s227
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s228
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s229
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s230
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s231
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s201prime
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s202prime
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s203prime
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s204prime
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s205prime
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s206prime
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s207prime
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s208prime
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s209prime
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s210prime
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s211prime
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s212prime
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s213prime
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s214prime
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s215prime
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s216prime
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s217prime
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s218prime
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s219prime
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s220prime
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s221prime
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s222prime
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s223prime
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s224prime
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s225prime
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s226prime
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s227prime
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s228prime
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s229prime
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s230prime
(
  vote_id            bigint     NOT NULL
  , part_id          bigint     NOT NULL
-- PARTITIONED BY (part_id)
);

CREATE STREAM s231prime
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
