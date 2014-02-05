CREATE STREAM words
(
  word            varchar(20)     NOT NULL
);

CREATE TABLE counts
(
  word  varchar(20) NOT NULL
, num   integer     NOT NULL
, CONSTRAINT PK_word PRIMARY KEY
  (
    word
  )
);


