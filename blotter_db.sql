/*
Version: PostgreSQL 13.1, compiled by Visual C++ build 1914, 64-bit
Developed By: John Falconi on 2/18/21
Last Updated: 2/18/21
*/


----------------------------------------------------------------------------------------
---------------------------------- ***** TABLES ***** ----------------------------------
----------------------------------------------------------------------------------------

----    this is the main table that contains all the blotter's activities
--DROP TABLE pittsburghpd_blotter;

CREATE TABLE pittsburghpd_blotter (
	report_name VARCHAR,
	ccr VARCHAR,
	section VARCHAR,
	description VARCHAR,
	arrest_date DATE,
	arrest_time TIME,
	address VARCHAR,
	neighborhood VARCHAR,
	zone VARCHAR,
	age VARCHAR,
	gender VARCHAR
);


----    this table is where I store all the activity so I can track when something fails or when the process runs
--DROP TABLE blotter_system_activity;

CREATE TABLE blotter_system_activity (
	id SERIAL PRIMARY KEY,
	action_name VARCHAR,
	run_date TIMESTAMP,
	run_source VARCHAR,
	parameter VARCHAR,
	status VARCHAR,
	description VARCHAR
);


----    an import table is also created and dropped at each run.  The reason for this is due to formatting and import
----    issues that run into when using pandas to import the csv data into the pg database.  Headers and data types are
----    fixed in the transfer and the scripts checks to see if the table exists at the beginning of each run.
--DROP TABLE pittsburghpd_blotter_import;

----    not a CREATE TABLE statement but it shows the changes made to the data from table to table
INSERT INTO pittsburghpd_blotter
SELECT "REPORT_NAME"
	, "CCR"
	, "SECTION"
	, "DESCRIPTION"
	, TO_DATE("ARREST_DATE", 'MM/DD/YYYY')
	, TO_TIMESTAMP("ARREST_TIME", 'HH24:MI')
	, "ADDRESS"
	, "NEIGHBORHOOD"
	, "ZONE"
	, CAST(NULLIF("AGE", '') AS INTEGER)
	, CAST(NULLIF("GENDER", '') AS VARCHAR)
FROM pittsburghpd_blotter_import;

---------------------------------------------------------------------------------------
-------------------------------- ***** FUNCTIONS ***** --------------------------------
---------------------------------------------------------------------------------------

----    function to automate the transfer using the previous query
CREATE OR REPLACE FUNCTION transfer() RETURNS void AS $$
	BEGIN
			INSERT INTO pittsburghpd_blotter
			SELECT "REPORT_NAME"
				, "CCR"
				, "SECTION"
				, "DESCRIPTION"
				, TO_DATE("ARREST_DATE", 'MM/DD/YYYY')
				, TO_TIMESTAMP("ARREST_TIME", 'HH24:MI')
				, "ADDRESS"
				, "NEIGHBORHOOD"
				, "ZONE"
				, CAST(NULLIF("AGE", '') AS INTEGER)
				, CAST(NULLIF("GENDER", '') AS VARCHAR)
			FROM pittsburghpd_blotter_import;
		
			DROP TABLE pittsburghpd_blotter_import;
		
	END;
$$ LANGUAGE plpgsql
;

----    this function helps me check to see if I already pulled records for a specific date
CREATE OR REPLACE FUNCTION date_check(day_in_question DATE) RETURNS INT AS $$
	DECLARE
		the_count INT;
	BEGIN
			
			the_count:= (SELECT COUNT(DISTINCT(ccr))
			FROM pittsburghpd_blotter
			WHERE arrest_date = day_in_question);
		
			RETURN the_count;
		
	END;
$$ LANGUAGE plpgsql


----    this function helps me log activity to the blotter_system_activity table
CREATE OR REPLACE FUNCTION log_activity(the_action VARCHAR, the_source VARCHAR, the_params VARCHAR, the_status VARCHAR, the_desc VARCHAR) RETURNS void AS $$
	BEGIN
			
			INSERT INTO blotter_system_activity(action_name, run_date, run_source, parameter, status, description) 
			VALUES (the_action, NOW(), the_source, the_params, the_status, the_desc);
		
	END;
$$ LANGUAGE plpgsql


----    this function checks if a specified table exists so the script won't fail out
CREATE OR REPLACE FUNCTION table_status(the_table VARCHAR) RETURNS INT AS $$
	DECLARE 
		status INT;
	BEGIN
			
			status:=(SELECT exists
					(
						SELECT 1
						FROM information_schema.tables
						WHERE table_schema = 'public'
						AND table_name = the_table
					)::INT);
			
			RETURN STATUS;
		
	END;
$$ LANGUAGE plpgsql