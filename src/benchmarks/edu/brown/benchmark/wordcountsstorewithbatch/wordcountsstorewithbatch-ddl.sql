CREATE STREAM words
(
  word  varchar(20)  NOT NULL
, time  bigint     NOT NULL
);

CREATE WINDOW W_WORDS ON words RANGE 1 SLIDE 1;

CREATE STREAM midstream
(
  word  varchar(20)  NOT NULL
, time  bigint     NOT NULL
, num   bigint     NOT NULL
);

CREATE WINDOW W_RESULTS ON midstream RANGE 30 SLIDE 2;

CREATE TABLE results
(
   word  varchar(20) NOT NULL
,  time  bigint         NOT NULL
,  num   bigint         NOT NULL
);

CREATE TABLE words_full
(
  word  varchar(20)  NOT NULL
, time  bigint     NOT NULL
);
