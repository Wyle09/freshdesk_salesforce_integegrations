DELIMITER $$

DROP PROCEDURE IF EXISTS customer_success_freshdesk.create_calendar_table;

CREATE PROCEDURE customer_success_freshdesk.create_calendar_table (start_date DATE, end_date DATE)
	BEGIN 
        DECLARE sd DATE DEFAULT start_date;
        DECLARE ed DATE DEFAULT end_date;

		DROP TABLE IF EXISTS customer_success_freshdesk.calendar_table;
		
		CREATE TABLE IF NOT EXISTS customer_success_freshdesk.calendar_table (
			  calendar_date DATE UNIQUE -- 1
            , calendar_day_name VARCHAR(64) -- 2
            , calendar_day_of_week TINYINT -- 3
            , calendar_day_of_month TINYINT -- 4 
            , calendar_day_of_year SMALLINT -- 5
            , calendar_week_day TINYINT -- 6
            , calendar_week_of_year TINYINT -- 7
            , calendar_week_first_day DATE -- 8
            , calendar_week_last_day DATE -- 9
            , calendar_month TINYINT NOT NULL -- 10
			, calendar_month_name VARCHAR(64) -- 11
            , calendar_month_first_day DATE -- 12
            , calendar_month_last_day DATE -- 13
            , calendar_quarter TINYINT NOT NULL -- 14
            , calendar_quarter_first_day DATE -- 15
            , calendar_quarter_last_day DATE -- 16
            , calendar_year SMALLINT NOT NULL -- 17
            , calendar_year_first_day DATE -- 18
            , calendar_year_last_day DATE -- 19
            , calendar_unix_timestamp INT UNSIGNED -- 20
            , PRIMARY KEY (calendar_date)
		);

		WHILE (sd <= ed) DO
			INSERT INTO customer_success_freshdesk.calendar_table VALUES (
				  sd -- 1
				, DAYNAME(sd) -- 2
                , DAYOFWEEK(sd) -- 3
                , DAYOFMONTH(sd) -- 4
                , DAYOFYEAR(sd) -- 5
                , WEEKDAY(sd) -- 6
                , WEEKOFYEAR(sd) -- 7
                , DATE_SUB(sd, INTERVAL DAYOFWEEK(sd) -1 DAY) -- 8
                , DATE_ADD(DATE_SUB(sd, INTERVAL DAYOFWEEK(sd) -1 DAY), INTERVAL 6 DAY) -- 9
                , MONTH(sd) -- 10
                , MONTHNAME(sd) -- 11
                , DATE_SUB(sd, INTERVAL DAYOFMONTH(sd) -1 DAY) -- 12
                , LAST_DAY(sd) -- 13
                , QUARTER(sd) -- 14
                , MAKEDATE(YEAR(sd), 1) + INTERVAL QUARTER(sd) QUARTER - INTERVAL 1 QUARTER -- 15
                , MAKEDATE(YEAR(sd), 1) + INTERVAL QUARTER(sd) QUARTER - INTERVAL 1 DAY -- 16
                , YEAR(sd) -- 17
                , DATE(CONCAT(YEAR(sd),"-01-01")) -- 18
                , DATE(CONCAT(YEAR(sd),"-12-31")) -- 19
                , UNIX_TIMESTAMP(sd) -- 20
            );
            
            SET sd = sd + INTERVAL 1 DAY;
		END WHILE;
        
	END$$

DELIMITER ;

CALL customer_success_freshdesk.create_calendar_table('2022-01-01', '2030-12-31');
