CREATE STREAM S1
(
  myvalue            integer     NOT NULL
);


CREATE WINDOW W_ROWS ON S1 ROWS 4 SLIDE 2;

