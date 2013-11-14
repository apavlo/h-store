CREATE STREAM S1
(
  value            integer     NOT NULL
);

CREATE STREAM S2
(
  value            integer     NOT NULL
);

CREATE STREAM S3
(
  value            integer     NOT NULL
);

CREATE STREAM S4
(
  value            integer     NOT NULL
);

CREATE STREAM S5
(
  value            integer     NOT NULL
);

CREATE STREAM S6
(
  value            integer     NOT NULL
);

CREATE STREAM S7
(
  value            integer     NOT NULL
);

CREATE STREAM S8
(
  value            integer     NOT NULL
);

CREATE STREAM S9
(
  value            integer     NOT NULL
);

CREATE STREAM S10
(
  value            integer     NOT NULL
);

CREATE STREAM S11
(
  value            integer     NOT NULL
);




CREATE TABLE votes_by_phone_number
(
    phone_number     bigint    NOT NULL,
    num_votes        int,
    CONSTRAINT PK_votes_by_phone_number PRIMARY KEY
    (
      phone_number
    )
);
