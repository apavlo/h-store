CREATE TABLE votes_by_phone_number
(
     phone_number     bigint    NOT NULL,
     num_votes        int,
     CONSTRAINT PK_votes_by_phone_number PRIMARY KEY
     (
       phone_number
     )
);

CREATE TABLE TABLEC (
   A_ID     BIGINT NOT NULL,
   A_VALUE  VARCHAR(64)
);

CREATE TABLE TABLEB (
   B_ID	    BIGINT NOT NULL,
   NUMROWS  INT    NOT NULL
);

CREATE TABLE TABLEA (
   A_ID     BIGINT NOT NULL,
   A_VALUE  VARCHAR(64)
);

create stream streamA (
   C_SINCE TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL, 
   phone_number     bigint     NOT NULL, 
   cash integer default 23
);

CREATE WINDOW W1 ON streamA ROWS 10 SLIDE 5;


