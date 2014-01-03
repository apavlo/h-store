
-- TPC-E Clause 2.2.8.2
CREATE TABLE status_type (
   st_id CHAR(4) NOT NULL,
   st_name CHAR(10) NOT NULL,
   PRIMARY KEY (st_id)
);

-- =========================================
-- MARKET TABLES
-- =========================================
CREATE TABLE security (
   s_symb CHAR(15) NOT NULL,
   s_issue CHAR(6) NOT NULL,
   s_st_id CHAR(4) NOT NULL REFERENCES status_type (st_id),
   s_name VARCHAR(70) NOT NULL, 
   s_num_out BIGINT NOT NULL,
   s_start_date TIMESTAMP NOT NULL,
   s_exch_date TIMESTAMP NOT NULL,
   s_pe FLOAT NOT NULL,
   s_52wk_high FLOAT NOT NULL,
   s_52wk_high_date TIMESTAMP NOT NULL,
   s_52wk_low FLOAT NOT NULL,
   s_52wk_low_date TIMESTAMP NOT NULL,
   s_dividend FLOAT NOT NULL,
   s_yield FLOAT NOT NULL,
   PRIMARY KEY (s_symb)
);


-- TPC-E Clause 2.2.6.1
-- remove status all brokers assumed to be active
-- removed name
CREATE TABLE broker (
   b_id BIGINT NOT NULL,
   b_num_trades INTEGER NOT NULL,
   b_comm_total FLOAT NOT NULL,
   PRIMARY KEY (b_id)
);

-- =========================================
-- CUSTOMER TABLE
-- =========================================

/*Combination of customer and customer_acccount*/
CREATE TABLE customer_info (
   c_id BIGINT NOT NULL,
   ca_id BIGINT NOT NULL,
   ca_b_id BIGINT NOT NULL REFERENCES broker (b_id),
   c_tier SMALLINT NOT NULL,
   ca_bal FLOAT NOT NULL,
   PRIMARY KEY (ca_id) /*??????????????????????*/
);

-- =========================================
-- BROKER TABLES
-- =========================================

-- TPC-E Clause 2.2.6.9
CREATE TABLE trade_type (
   tt_id CHAR(3) NOT NULL,
   tt_name CHAR(12) NOT NULL,
   tt_is_sell TINYINT NOT NULL,
   tt_is_mrkt TINYINT NOT NULL,
   PRIMARY KEY (tt_id)
);

-- TPC-E Clause 2.2.6.6
CREATE TABLE trade (
   t_id BIGINT NOT NULL,
   t_dts TIMESTAMP NOT NULL,
   t_st_id CHAR(4) NOT NULL REFERENCES status_type (st_id), --complete, pending, active etc 
   t_tt_id CHAR(3) NOT NULL REFERENCES trade_type (tt_id), --buy or sell, limit buy, limit sell
   t_is_cash TINYINT NOT NULL, --likely remove bc trade is always cash
   t_s_symb CHAR(15) NOT NULL REFERENCES security (s_symb), 
   t_qty INTEGER NOT NULL CHECK (t_qty > 0),
   t_bid_price FLOAT NOT NULL CHECK (t_bid_price > 0),
   t_ca_id BIGINT NOT NULL REFERENCES customer_info (ca_id),
   --removed name here
   t_trade_price FLOAT,
   t_chrg FLOAT NOT NULL CHECK (t_chrg >= 0), -- will be a constant
   t_comm FLOAT NOT NULL CHECK (t_comm >= 0), -- will be a constant
   t_tax FLOAT NOT NULL CHECK (t_tax >= 0), -- will be a constant
   t_lifo TINYINT NOT NULL, --don't think this matters left it in
   PRIMARY KEY (t_id)
);
CREATE INDEX i_t_st_id ON trade (t_st_id);
CREATE INDEX i_t_ca_id ON trade (t_ca_id);

-- TPC-E Clause 2.2.6.7
CREATE TABLE trade_history (
   th_t_id BIGINT NOT NULL REFERENCES trade (t_id),
   th_dts TIMESTAMP NOT NULL,
   th_st_id CHAR(4) NOT NULL REFERENCES status_type (st_id), --complete, pending, active etc 
   PRIMARY KEY (th_t_id, th_st_id)
);



-- TPC-E Clause 2.2.5.5
CREATE TABLE holding (
   h_t_id BIGINT NOT NULL REFERENCES trade (t_id),
   h_ca_id BIGINT NOT NULL,
   h_s_symb CHAR(15) NOT NULL,
   h_dts TIMESTAMP NOT NULL,
   h_price FLOAT NOT NULL CHECK (h_price > 0),
   h_qty INTEGER NOT NULL,
   PRIMARY KEY (h_t_id)
);
CREATE INDEX i_holding ON holding (h_ca_id, h_s_symb);

-- TPC-E Clause 2.2.5.6
CREATE TABLE holding_history (
   hh_h_t_id BIGINT NOT NULL REFERENCES trade (t_id),
   hh_t_id BIGINT NOT NULL REFERENCES trade (t_id),
   hh_before_qty INTEGER NOT NULL,
   hh_after_qty INTEGER NOT NULL,
   PRIMARY KEY (hh_h_t_id, hh_t_id)
);

-- TPC-E Clause 2.2.6.8
CREATE TABLE trade_request (
   tr_t_id BIGINT NOT NULL REFERENCES trade (t_id),
    tr_tt_id CHAR(3) NOT NULL REFERENCES trade_type (tt_id), --remove reference to trade_type
   tr_s_symb CHAR(15) NOT NULL REFERENCES security (s_symb), --remove reference to security
   tr_qty INTEGER NOT NULL CHECK (tr_qty > 0),
   tr_bid_price FLOAT NOT NULL CHECK (tr_bid_price > 0),
   tr_ca_id BIGINT NOT NULL REFERENCES customer_info (ca_id),
   --tr_b_id BIGINT NOT NULL REFERENCES broker (b_id),
   PRIMARY KEY (tr_t_id)
);
CREATE INDEX i_tr_s_symb ON trade_request (tr_s_symb);

-- TPC-E Clause 2.2.6.2
CREATE TABLE cash_transaction (
   ct_t_id BIGINT NOT NULL REFERENCES trade (t_id),
   ct_dts TIMESTAMP NOT NULL,
   ct_amt FLOAT NOT NULL,
   ct_name VARCHAR(100),
   PRIMARY KEY (ct_t_id)
);

-- TPC-E Clause 2.2.7.7
CREATE TABLE last_trade (
   lt_s_symb CHAR(15) NOT NULL REFERENCES security (s_symb),
   lt_dts TIMESTAMP NOT NULL,
   lt_price FLOAT NOT NULL,
   lt_open_price FLOAT NOT NULL,
   lt_vol BIGINT,
   PRIMARY KEY (lt_s_symb)
);

-- TPC-E Clause 2.2.5.7
CREATE TABLE holding_summary (
   hs_ca_id BIGINT NOT NULL REFERENCES customer_info (ca_id),
   hs_s_symb CHAR(15) NOT NULL REFERENCES security (s_symb),
   hs_qty INTEGER NOT NULL,
   PRIMARY KEY (hs_ca_id, hs_s_symb)
);




