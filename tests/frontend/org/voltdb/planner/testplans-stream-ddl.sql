           
CREATE STREAM ticker (
	t_id INTEGER default '0' NOT NULL,
	t_name varchar(10) NOT NULL,
	t_stamp INTEGER NOT NULL,
	PRIMARY KEY (t_stamp)
);

