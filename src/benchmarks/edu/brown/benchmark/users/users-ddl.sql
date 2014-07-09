-- ================================================================ 
-- USERS
-- Represents users 
-- u_id             User ID
-- u_firstname      User's first name
-- u_lastname       User's last name
-- u_password       User's password
-- u_email          User's email
-- ================================================================
CREATE TABLE USERS (
	u_id	BIGINT NOT NULL,
		u_firstname	VARCHAR(100),
			u_lastname	VARCHAR(100),
				u_password	VARCHAR(100),
					u_email	VARCHAR(100), 
					u_attr01	BIGINT,
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
