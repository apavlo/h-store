CREATE TABLE SUBSCRIBER (
   s_id INTEGER NOT NULL PRIMARY KEY,
   sub_nbr VARCHAR(15) NOT NULL UNIQUE,
   bit_1 TINYINT,
   bit_2 TINYINT,
   bit_3 TINYINT,
   bit_4 TINYINT,
   bit_5 TINYINT,
   bit_6 TINYINT,
   bit_7 TINYINT,
   bit_8 TINYINT,
   bit_9 TINYINT,
   bit_10 TINYINT,
   hex_1 TINYINT,
   hex_2 TINYINT,
   hex_3 TINYINT,
   hex_4 TINYINT,
   hex_5 TINYINT,
   hex_6 TINYINT,
   hex_7 TINYINT,
   hex_8 TINYINT,
   hex_9 TINYINT,
   hex_10 TINYINT,
   byte2_1 SMALLINT,
   byte2_2 SMALLINT,
   byte2_3 SMALLINT,
   byte2_4 SMALLINT,
   byte2_5 SMALLINT,
   byte2_6 SMALLINT,
   byte2_7 SMALLINT,
   byte2_8 SMALLINT,
   byte2_9 SMALLINT,
   byte2_10 SMALLINT,
   msc_location INTEGER,
   vlr_location INTEGER
);

CREATE TABLE ACCESS_INFO (
   s_id INTEGER NOT NULL,
   ai_type TINYINT NOT NULL,
   data1 SMALLINT,
   data2 SMALLINT,
   data3 VARCHAR(3),
   data4 VARCHAR(5),
   PRIMARY KEY(s_id, ai_type),
   FOREIGN KEY (s_id) REFERENCES SUBSCRIBER (s_id)
);

CREATE TABLE SPECIAL_FACILITY (
   s_id INTEGER NOT NULL,
   sf_type TINYINT NOT NULL,
   is_active TINYINT NOT NULL,
   error_cntrl SMALLINT,
   data_a SMALLINT,
   data_b VARCHAR(5),
   PRIMARY KEY (s_id, sf_type),
   FOREIGN KEY (s_id) REFERENCES SUBSCRIBER (s_id)
);

CREATE TABLE CALL_FORWARDING (
   s_id INTEGER NOT NULL,
   sf_type TINYINT NOT NULL,
   start_time TINYINT NOT NULL,
   end_time TINYINT,
   numberx VARCHAR(15),
   PRIMARY KEY (s_id, sf_type, start_time),
   FOREIGN KEY (s_id, sf_type) REFERENCES SPECIAL_FACILITY(s_id, sf_type)
);
CREATE INDEX IDX_CF ON CALL_FORWARDING (S_ID);