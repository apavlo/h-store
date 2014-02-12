CREATE TABLE counts
(
  word  varchar(20)  NOT NULL
, num   int     NOT NULL
, time  int     NOT NULL
, CONSTRAINT PK_word PRIMARY KEY
  (
    word
  )
);

