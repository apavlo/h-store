CREATE TABLE TABLEA (
   phone_number     bigint     NOT NULL, 
   cash integer default 23
);

CREATE TABLE TABLEB (
   phone_number     bigint     NOT NULL, 
   cash integer default 23
);

create stream streamA (
   phone_number     bigint     NOT NULL, 
   cash integer default 23
);

CREATE WINDOW W_ROWS ON streamA ROWS 10 SLIDE 5;

CREATE WINDOW W_TIME ON streamA RANGE 10 SLIDE 5;
