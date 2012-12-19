CREATE TABLE ipblocks (
  ipb_id int, -- Serial
  ipb_address varchar(255) NOT NULL,
  ipb_user int DEFAULT '0' NOT NULL,
  ipb_by int DEFAULT '0' NOT NULL,
  ipb_by_text varchar(255) DEFAULT '' NOT NULL,
  ipb_reason varchar(255) NOT NULL,
  ipb_timestamp timestamp NOT NULL,
  ipb_auto smallint DEFAULT '0' NOT NULL,
  ipb_anon_only smallint DEFAULT '0' NOT NULL ,
  ipb_create_account smallint DEFAULT '1' NOT NULL ,
  ipb_enable_autoblock smallint DEFAULT '1' NOT NULL ,
  ipb_expiry timestamp NOT NULL ,
  ipb_range_start varchar(255) NOT NULL,
  ipb_range_end varchar(255) NOT NULL,
  ipb_deleted smallint DEFAULT '0' NOT NULL,
  ipb_block_email smallint DEFAULT '0' NOT NULL ,
  ipb_allow_usertalk smallint DEFAULT '0' NOT NULL ,
  PRIMARY KEY (ipb_id),
  UNIQUE (ipb_address,ipb_user,ipb_auto,ipb_anon_only)
);
CREATE INDEX IDX_IPB_USER ON ipblocks (ipb_user);
CREATE INDEX IDX_IPB_RANGE ON ipblocks (ipb_range_start,ipb_range_end);
CREATE INDEX IDX_IPB_TIMESTAMP ON ipblocks (ipb_timestamp);
CREATE INDEX IDX_IPB_EXPIRY ON ipblocks (ipb_expiry);

CREATE TABLE useracct (
  user_id int NOT NULL,
  user_name varchar(255) DEFAULT '' NOT NULL,
  user_real_name varchar(255) DEFAULT '' NOT NULL,
  user_password varchar(255) NOT NULL,
  user_newpassword varchar(255) NOT NULL,
  user_newpass_time timestamp DEFAULT NULL,
  user_email varchar(255) NOT NULL,
  user_options varchar(255) NOT NULL,
  user_touched timestamp NOT NULL ,
  user_token varchar(32) DEFAULT '' NOT NULL,
  user_email_authenticated timestamp DEFAULT NULL,
  user_email_token varchar(32) DEFAULT NULL,
  user_email_token_expires timestamp DEFAULT NULL,
  user_registration timestamp DEFAULT NULL,
  user_editcount int DEFAULT NULL,
  PRIMARY KEY (user_id),
  UNIQUE (user_name)
);
CREATE INDEX IDX_USER_EMAIL_TOKEN ON useracct (user_email_token);

CREATE TABLE user_groups (
  ug_user int DEFAULT '0' NOT NULL,
  ug_group varchar(16) DEFAULT '' NOT NULL,
  UNIQUE (ug_user,ug_group)
);
CREATE INDEX IDX_UG_GROUP ON user_groups (ug_group);

CREATE TABLE page (
  page_id bigint NOT NULL,
  page_namespace int NOT NULL,
  page_title varchar(255) NOT NULL,
  page_restrictions varchar(255) NOT NULL,
  page_counter bigint DEFAULT '0' NOT NULL,
  page_is_redirect smallint DEFAULT '0' NOT NULL,
  page_is_new smallint DEFAULT '0' NOT NULL,
  page_random float precision NOT NULL,
  page_touched timestamp NOT NULL,
  page_latest int NOT NULL,
  page_len int NOT NULL,
  PRIMARY KEY (page_id),
  UNIQUE (page_namespace,page_title)
);
CREATE INDEX IDX_PAGE_RANDOM ON page (page_random);
CREATE INDEX IDX_PAGE_LEN ON page (page_len);

CREATE TABLE page_restrictions (
  pr_page bigint NOT NULL REFERENCES page (page_id),
  pr_type varchar(60) NOT NULL,
  pr_level varchar(60) NOT NULL,
  pr_cascade smallint NOT NULL,
  pr_user int DEFAULT NULL,
  pr_expiry timestamp DEFAULT NULL,
  pr_id int NOT NULL,
  PRIMARY KEY (pr_id),
  UNIQUE (pr_page,pr_type)
);
CREATE INDEX IDX_PR_TYPELEVEL ON page_restrictions (pr_type,pr_level);
CREATE INDEX IDX_PR_LEVEL ON page_restrictions (pr_level);
CREATE INDEX IDX_PR_CASCADE ON page_restrictions (pr_cascade);

CREATE TABLE recentchanges (
  rc_id int NOT NULL,
  rc_timestamp timestamp NOT NULL,
  rc_cur_time timestamp NOT NULL,
  rc_user int DEFAULT '0' NOT NULL,
  rc_user_text varchar(255) NOT NULL,
  rc_namespace int DEFAULT '0' NOT NULL,
  rc_page bigint NOT NULL REFERENCES page (page_id),
  rc_comment varchar(255) DEFAULT '' NOT NULL,
  rc_minor smallint DEFAULT '0' NOT NULL,
  rc_bot smallint DEFAULT '0' NOT NULL,
  rc_new smallint DEFAULT '0' NOT NULL,
  rc_cur_id int DEFAULT '0' NOT NULL,
  rc_this_oldid int DEFAULT '0' NOT NULL,
  rc_last_oldid int DEFAULT '0' NOT NULL,
  rc_type smallint DEFAULT '0' NOT NULL,
  rc_moved_to_ns smallint DEFAULT '0' NOT NULL,
  rc_moved_to_title varchar(255) DEFAULT '' NOT NULL,
  rc_patrolled smallint DEFAULT '0' NOT NULL,
  rc_ip varchar(40) DEFAULT '' NOT NULL,
  rc_old_len int DEFAULT NULL,
  rc_new_len int DEFAULT NULL,
  rc_deleted smallint DEFAULT '0' NOT NULL,
  rc_logid int DEFAULT '0' NOT NULL,
  rc_log_type varchar(255) DEFAULT NULL,
  rc_log_action varchar(255) DEFAULT NULL,
  rc_params varchar(255),
  PRIMARY KEY (rc_id, rc_page)
);
CREATE INDEX IDX_RC_TIMESTAMP ON recentchanges (rc_timestamp);
CREATE INDEX IDX_RC_NAMESPACE_TITLE ON recentchanges (rc_namespace,rc_page);
CREATE INDEX IDX_RC_CUR_ID ON recentchanges (rc_cur_id);
CREATE INDEX IDX_NEW_NAME_TIMESTAMP ON recentchanges (rc_new,rc_namespace,rc_timestamp);
CREATE INDEX IDX_RC_IP ON recentchanges (rc_ip);
CREATE INDEX IDX_RC_NS_USERTEXT ON recentchanges (rc_namespace,rc_user_text);
CREATE INDEX IDX_RC_USER_TEXT ON recentchanges (rc_user_text,rc_timestamp);

CREATE TABLE text (
  old_id bigint NOT NULL,
  old_text varchar(1000000) NOT NULL,
  old_flags varchar(255) NOT NULL,
  old_page bigint NOT NULL REFERENCES page (page_id),
  PRIMARY KEY (old_id, old_page)
);

CREATE TABLE revision (
  rev_id bigint NOT NULL,
  rev_page bigint NOT NULL,
  rev_text_id bigint NOT NULL,
  rev_comment varchar(255) NOT NULL,
  rev_user int DEFAULT '0' NOT NULL REFERENCES useracct (user_id),
  rev_user_text varchar(255) DEFAULT '' NOT NULL,
  rev_timestamp timestamp NOT NULL ,
  rev_minor_edit smallint DEFAULT '0' NOT NULL,
  rev_deleted smallint DEFAULT '0' NOT NULL,
  rev_len int DEFAULT NULL,
  rev_parent_id int DEFAULT NULL,
  FOREIGN KEY (rev_text_id, rev_page) REFERENCES text (old_id, old_page),
  PRIMARY KEY (rev_id, rev_page),
);
CREATE INDEX IDX_REV_TIMESTAMP ON revision (rev_timestamp);
CREATE INDEX IDX_PAGE_TIMESTAMP ON revision (rev_page,rev_timestamp);
CREATE INDEX IDX_USER_TIMESTAMP ON revision (rev_user,rev_timestamp);
-- CREATE INDEX IDX_USERTEXT_TIMESTAMP ON revision (rev_user_text,rev_timestamp);

CREATE TABLE logging (
  log_id bigint NOT NULL, -- serial
  log_type varchar(32) NOT NULL,
  log_action varchar(32) NOT NULL,
  log_timestamp timestamp NOT NULL,
  log_user int DEFAULT '0' NOT NULL,
  log_namespace int DEFAULT '0' NOT NULL,
  log_page bigint DEFAULT 0 NOT NULL,
  log_comment varchar(255) DEFAULT '' NOT NULL,
  log_params varchar(255) NOT NULL,
  log_deleted smallint DEFAULT '0' NOT NULL,
  log_user_text varchar(255) DEFAULT '' NOT NULL,
  PRIMARY KEY (log_id)
);
CREATE INDEX IDX_LOG_TYPE_TIME ON logging (log_type,log_timestamp);
CREATE INDEX IDX_LOG_USER_TIME ON logging (log_user,log_timestamp);
CREATE INDEX IDX_LOG_PAGE_TIME ON logging (log_namespace,log_page,log_timestamp);
CREATE INDEX IDX_LOG_TIMES ON logging (log_timestamp);
CREATE INDEX IDX_LOG_USER_TYPE_TIME ON logging (log_user,log_type,log_timestamp);
CREATE INDEX IDX_LOG_PAGE_ID_TIME ON logging (log_page,log_timestamp);

CREATE TABLE watchlist (
  wl_user int NOT NULL,
  wl_namespace int DEFAULT '0' NOT NULL,
  wl_page bigint NOT NULL REFERENCES page (page_id),
  wl_notificationtimestamp timestamp DEFAULT NULL,
  UNIQUE (wl_user,wl_namespace,wl_page)
);
CREATE INDEX IDX_WL_NAMESPACE_TITLE ON watchlist (wl_namespace, wl_page);
