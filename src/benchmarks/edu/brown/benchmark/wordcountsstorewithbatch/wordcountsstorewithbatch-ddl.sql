CREATE STREAM words
(
  word  varchar(50)  NOT NULL
, time  int     NOT NULL
);

CREATE WINDOW W_WORDS ON words RANGE 30 SLIDE 2;

