CREATE TABLE T1
(
  value            integer     NOT NULL
);

CREATE STREAM S1
(
  value            integer     NOT NULL
);

CREATE WINDOW W1 ON S1 ROWS 3 SLIDE 1;

CREATE STREAM S2
(
  value            integer     NOT NULL
);

CREATE TABLE T2
(
  value            integer     NOT NULL
);
