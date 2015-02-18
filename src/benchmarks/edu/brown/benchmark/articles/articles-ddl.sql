-- ================================================================ 
-- USERS
-- Represents users 
-- u_id     User ID
-- u_firstname      User's first name
-- u_lastname       User's last name
-- u_password       User's password
-- u_email      User's email
-- ================================================================
CREATE TABLE USERS (
    u_id    BIGINT NOT NULL,
    u_firstname    VARCHAR(100),
    u_lastname    VARCHAR(100),
    u_password    VARCHAR(100),
    u_email       VARCHAR(100) UNIQUE, 
    u_attr01      VARCHAR(8),
    u_attr02      VARCHAR(8),
    u_attr03      VARCHAR(8),
    u_attr04      VARCHAR(8),
    u_attr05      VARCHAR(8),
    u_attr06      VARCHAR(8),
    u_attr07      VARCHAR(8),
    u_attr08      VARCHAR(8),
    u_attr09      VARCHAR(8),
    u_attr10      VARCHAR(8),
    u_attr11      VARCHAR(8),
    u_attr12      VARCHAR(8),
    u_attr13      VARCHAR(8),
    u_attr14      VARCHAR(8),
    u_attr15      VARCHAR(8),    
    PRIMARY KEY (u_id)
);
-- ================================================================
-- ARTICLES
-- Represents articles
-- a_id      ID
-- a_title     Article's title
-- a_text     Article's content
-- a_num_comments    Number of comments on the article
-- ================================================================
CREATE TABLE ARTICLES (
    a_id    BIGINT NOT NULL,
    a_title    VARCHAR(100),
    a_text    VARCHAR(100),
    a_num_comments    BIGINT,      
    a_attr01      VARCHAR(8),
    a_attr02      VARCHAR(8),
    a_attr03      VARCHAR(8),
    a_attr04      VARCHAR(8),
    a_attr05      VARCHAR(8),
    a_attr06      VARCHAR(8),
    a_attr07      VARCHAR(8),
    a_attr08      VARCHAR(8),
    a_attr09      VARCHAR(8),
    a_attr10      VARCHAR(8),
    a_attr11      VARCHAR(8),
    a_attr12      VARCHAR(8),
    a_attr13      VARCHAR(8),
    a_attr14      VARCHAR(8),
    a_attr15      VARCHAR(8),    
    PRIMARY KEY (a_id)
);
-- ================================================================
-- COMMENTS
-- Represents comments provided by buyers
-- c_id    Comment's ID
-- a_id    Article's ID
-- u_id    User's ID
-- c_text    Actual comment text
-- ================================================================
CREATE TABLE COMMENTS (
    c_id    BIGINT NOT NULL UNIQUE,
    c_a_id    BIGINT NOT NULL REFERENCES ARTICLES (a_id),
    c_u_id    BIGINT NOT NULL REFERENCES USERS (u_id),
    c_text    VARCHAR(100),
    c_attr01      VARCHAR(8),
    c_attr02      VARCHAR(8),
    c_attr03      VARCHAR(8),
    c_attr04      VARCHAR(8),
    c_attr05      VARCHAR(8),
    c_attr06      VARCHAR(8),
    c_attr07      VARCHAR(8),
    c_attr08      VARCHAR(8),
    c_attr09      VARCHAR(8),
    c_attr10      VARCHAR(8),
    PRIMARY KEY (c_a_id, c_id)
);
CREATE INDEX idx_comments_article_id ON COMMENTS (c_a_id);