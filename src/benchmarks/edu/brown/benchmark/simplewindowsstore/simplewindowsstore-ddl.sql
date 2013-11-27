CREATE TABLE AVG_FROM_WIN
(
  time              integer   NOT NULL,
  valAvg            float     NOT NULL,

);

CREATE STREAM S1
(
  myvalue            integer     NOT NULL,
  time               integer     NOT NULL
);


CREATE WINDOW W_ROWS ON S1 ROWS 100 SLIDE 10;

