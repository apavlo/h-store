CREATE STREAM words
(
  word  varchar(20)  NOT NULL
, time  int     NOT NULL
);

CREATE WINDOW W_WORDS ON words RANGE 1 SLIDE 1;

CREATE STREAM midstream
(
  word  varchar(20)  NOT NULL
, time  int     NOT NULL
, num   int     NOT NULL
);

CREATE WINDOW W_RESULTS ON midstream RANGE 100 SLIDE 1;

CREATE TABLE results
(
   word  varchar(20) NOT NULL
,  time  int         NOT NULL
,  num   int         NOT NULL
);

CREATE TABLE words_full
(
  word  varchar(20)  NOT NULL
, time  int     NOT NULL
);
