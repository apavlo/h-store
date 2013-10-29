CREATE TABLE votes_by_phone_number
(
    phone_number     bigint    NOT NULL,
    num_votes        int,
    CONSTRAINT PK_votes_by_phone_number PRIMARY KEY
    (
      phone_number
    )
);