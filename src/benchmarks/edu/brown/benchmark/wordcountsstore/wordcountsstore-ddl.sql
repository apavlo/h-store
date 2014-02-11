CREATE STREAM words
(
  word  varchar(20)  NOT NULL
, time  int     NOT NULL
);

CREATE WINDOW W_WORDS ON words RANGE 30 SLIDE 2;

CREATE TABLE results
(
   word  varchar(20) NOT NULL
,  time  int         NOT NULL
,  num   int         NOT NULL
);
