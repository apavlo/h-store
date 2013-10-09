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

CREATE TABLE votes_by_contestant_number_state
(
  contestant_number  int        NOT NULL
, state              varchar(2) NOT NULL
, num_votes          int
);

CREATE TABLE total_votes
(
    row_id           bigint    NOT NULL,
    num_votes        int       NOT NULL
);


-- streams for processing ---
CREATE STREAM votes_stream
(
  vote_id            bigint     NOT NULL,
  phone_number       bigint     NOT NULL
, state              varchar(2) NOT NULL
, contestant_number  integer    NOT NULL
, created            timestamp  NOT NULL
);

-- rollup of votes by phone number, used to reject excessive voting
CREATE TABLE votes_by_phone_number
(
    phone_number     bigint    NOT NULL,
    num_votes        int
);

-- result from step 1: Validate contestants
CREATE STREAM S1
(
  vote_id            bigint     NOT NULL,
  phone_number       bigint     NOT NULL
, state              varchar(2) NOT NULL
, contestant_number  integer    NOT NULL
, created            timestamp  NOT NULL
);

-- result from step2: Validate number of votes
CREATE STREAM S2
(
  vote_id            bigint     NOT NULL,
  phone_number       bigint     NOT NULL
, state              varchar(2) NOT NULL
, contestant_number  integer    NOT NULL
, created            timestamp  NOT NULL
);


CREATE STREAM S3
(
    phone_number     bigint    NOT NULL,
    num_votes        int
);

