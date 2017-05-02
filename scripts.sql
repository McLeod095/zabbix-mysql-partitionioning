DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `table_prepare`(p_SCHEMANAME VARCHAR(32))
BEGIN
	CALL prepare_partition(p_SCHEMANAME, 'history', 90, 24);
	CALL prepare_partition(p_SCHEMANAME, 'history_log', 90, 24);
	CALL prepare_partition(p_SCHEMANAME, 'history_str', 90, 24);
	CALL prepare_partition(p_SCHEMANAME, 'history_text', 90, 24);
	CALL prepare_partition(p_SCHEMANAME, 'history_uint', 90, 24);
	CALL prepare_partition(p_SCHEMANAME, 'trends', 365, 24);
	CALL prepare_partition(p_SCHEMANAME, 'trends_uint', 365, 24);
END$$
DELIMITER ;


DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `prepare_partition`(p_SCHEMANAME VARCHAR(32), p_TABLENAME VARCHAR(32), p_KEEP_DATA_DAYS INT, p_HOURLY_INTERVAL INT)
BEGIN
        DECLARE v_PARTITIONNAME VARCHAR(16);
        DECLARE v_LESS_THAN_TIMESTAMP INT;
        DECLARE v_CURTIME INT;
		DECLARE v_KEEP_DATA_HOURS INT;
		DECLARE v_RETROWS INT;
		DECLARE v_CHECKTABLE INT;

		SELECT COUNT(1) INTO v_CHECKTABLE
        FROM information_schema.partitions
        WHERE table_schema = p_SCHEMANAME AND TABLE_NAME = p_TABLENAME AND partition_name IS NULL;

		IF v_CHECKTABLE = 1 THEN
			
			SET v_KEEP_DATA_HOURS = p_KEEP_DATA_DAYS * 24 / p_HOURLY_INTERVAL;
			SET v_CURTIME = UNIX_TIMESTAMP(DATE_FORMAT(NOW() + INTERVAL 1 DAY, '%Y-%m-%d 00:00:00'));

			SET @__SQL = CONCAT("ALTER TABLE ", p_SCHEMANAME, ".", p_TABLENAME, " PARTITION BY RANGE(`clock`) (");

			SET @_COMMA = "";
			SET @__interval = v_KEEP_DATA_HOURS;
			create_loop: LOOP
                IF @__interval < 0 THEN
					LEAVE create_loop;
                END IF;
 
                SET v_LESS_THAN_TIMESTAMP = v_CURTIME - (p_HOURLY_INTERVAL * @__interval * 3600);
                SET v_PARTITIONNAME = FROM_UNIXTIME(v_CURTIME - p_HOURLY_INTERVAL * (@__interval + 1) * 3600, 'p%Y%m%d%H00');
                SET @__interval=@__interval-1;

				SELECT COUNT(1) INTO v_RETROWS FROM information_schema.partitions WHERE table_schema = p_SCHEMANAME AND table_name = p_TABLENAME AND (partition_name = v_PARTITIONNAME OR partition_description = v_LESS_THAN_TIMESTAMP);
				
				IF v_RETROWS = 0 THEN

					SET @__SQL = CONCAT( @__SQL, @_COMMA, 'PARTITION ', v_PARTITIONNAME, ' VALUES LESS THAN (', v_LESS_THAN_TIMESTAMP, ')' );
					SET @_COMMA = ", ";
				END IF;
			END LOOP; 
		
			SET @__SQL = CONCAT(@__SQL, ");");
			PREPARE STMT FROM @__SQL;
			EXECUTE STMT;
			DEALLOCATE PREPARE STMT;
		END IF;
END$$
DELIMITER ;


DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `partition_verify`(SCHEMANAME VARCHAR(64), TABLENAME VARCHAR(64), HOURLYINTERVAL INT(11))
BEGIN
        DECLARE PARTITION_NAME VARCHAR(16);
        DECLARE RETROWS INT(11);
        DECLARE FUTURE_TIMESTAMP TIMESTAMP;
 
        
        SELECT COUNT(1) INTO RETROWS
        FROM information_schema.partitions
        WHERE table_schema = SCHEMANAME AND TABLE_NAME = TABLENAME AND partition_name IS NULL;
 
        
        IF RETROWS = 1 THEN
                
                SET FUTURE_TIMESTAMP = TIMESTAMPADD(HOUR, HOURLYINTERVAL, CONCAT(CURDATE(), " ", '00:00:00'));
                SET PARTITION_NAME = DATE_FORMAT(CURDATE(), 'p%Y%m%d%H00');
 
                
                SET @__PARTITION_SQL = CONCAT("ALTER TABLE ", SCHEMANAME, ".", TABLENAME, " PARTITION BY RANGE(`clock`)");
                SET @__PARTITION_SQL = CONCAT(@__PARTITION_SQL, "(PARTITION ", PARTITION_NAME, " VALUES LESS THAN (", UNIX_TIMESTAMP(FUTURE_TIMESTAMP), "));");
 
                
                PREPARE STMT FROM @__PARTITION_SQL;
                EXECUTE STMT;
                DEALLOCATE PREPARE STMT;
        END IF;
END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `partition_create`(SCHEMANAME VARCHAR(64), TABLENAME VARCHAR(64), PARTITIONNAME VARCHAR(64), CLOCK INT)
BEGIN
        DECLARE RETROWS INT;
        SELECT COUNT(1) INTO RETROWS
        FROM information_schema.partitions
        WHERE table_schema = SCHEMANAME AND TABLE_NAME = TABLENAME AND partition_description >= CLOCK;
 
		SELECT CONCAT("RETROWS ", RETROWS);
	
        IF RETROWS = 0 THEN
                
                SELECT CONCAT( "partition_create(", SCHEMANAME, ",", TABLENAME, ",", PARTITIONNAME, ",", CLOCK, ")" ) AS msg;
                SET @SQL = CONCAT( 'ALTER TABLE ', SCHEMANAME, '.', TABLENAME, ' ADD PARTITION (PARTITION ', PARTITIONNAME, ' VALUES LESS THAN (', CLOCK, '));' );
                PREPARE STMT FROM @SQL;
                EXECUTE STMT;
                DEALLOCATE PREPARE STMT;
        END IF;
END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `partition_drop`(SCHEMANAME VARCHAR(64), TABLENAME VARCHAR(64), DELETE_BELOW_PARTITION_DATE BIGINT)
BEGIN
        
        DECLARE done INT DEFAULT FALSE;
        DECLARE drop_part_name VARCHAR(16);
 
        
        DECLARE myCursor CURSOR FOR
                SELECT partition_name
                FROM information_schema.partitions
                WHERE table_schema = SCHEMANAME AND TABLE_NAME = TABLENAME AND CAST(SUBSTRING(partition_name FROM 2) AS UNSIGNED) < DELETE_BELOW_PARTITION_DATE;
        DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
 
        
        SET @alter_header = CONCAT("ALTER TABLE ", SCHEMANAME, ".", TABLENAME, " DROP PARTITION ");
        SET @drop_partitions = "";
 
        
        OPEN myCursor;
        read_loop: LOOP
                FETCH myCursor INTO drop_part_name;
                IF done THEN
                        LEAVE read_loop;
                END IF;
                SET @drop_partitions = IF(@drop_partitions = "", drop_part_name, CONCAT(@drop_partitions, ",", drop_part_name));
        END LOOP;
        IF @drop_partitions != "" THEN
                
                SET @full_sql = CONCAT(@alter_header, @drop_partitions, ";");
                PREPARE STMT FROM @full_sql;
                EXECUTE STMT;
                DEALLOCATE PREPARE STMT;
 
                SELECT CONCAT(SCHEMANAME, ".", TABLENAME) AS `table`, @drop_partitions AS `partitions_deleted`;
        ELSE
                
                SELECT CONCAT(SCHEMANAME, ".", TABLENAME) AS `table`, "N/A" AS `partitions_deleted`;
        END IF;
END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `partition_maintenance`(SCHEMA_NAME VARCHAR(32), TABLE_NAME VARCHAR(32), KEEP_DATA_DAYS INT, HOURLY_INTERVAL INT, CREATE_NEXT_INTERVALS INT)
BEGIN
        DECLARE OLDER_THAN_PARTITION_DATE VARCHAR(16);
        DECLARE PARTITION_NAME VARCHAR(16);
        DECLARE LESS_THAN_TIMESTAMP INT;
        DECLARE CUR_TIME INT;
 
        CALL partition_verify(SCHEMA_NAME, TABLE_NAME, HOURLY_INTERVAL);
        SET CUR_TIME = UNIX_TIMESTAMP(DATE_FORMAT(NOW(), '%Y-%m-%d 00:00:00'));
 
        SET @__interval = 1;
        create_loop: LOOP
                IF @__interval > CREATE_NEXT_INTERVALS THEN
                        LEAVE create_loop;
                END IF;
 
                SET LESS_THAN_TIMESTAMP = CUR_TIME + (HOURLY_INTERVAL * @__interval * 3600);
                SET PARTITION_NAME = FROM_UNIXTIME(CUR_TIME + HOURLY_INTERVAL * (@__interval - 1) * 3600, 'p%Y%m%d%H00');
                CALL partition_create(SCHEMA_NAME, TABLE_NAME, PARTITION_NAME, LESS_THAN_TIMESTAMP);
                SET @__interval=@__interval+1;
        END LOOP;
 
        SET OLDER_THAN_PARTITION_DATE=DATE_FORMAT(DATE_SUB(NOW(), INTERVAL KEEP_DATA_DAYS DAY), '%Y%m%d0000');
        CALL partition_drop(SCHEMA_NAME, TABLE_NAME, OLDER_THAN_PARTITION_DATE);
 
END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `partition_maintenance_all`(SCHEMA_NAME VARCHAR(32))
BEGIN
                CALL partition_maintenance(SCHEMA_NAME, 'history', 90, 24, 14);
                CALL partition_maintenance(SCHEMA_NAME, 'history_log', 90, 24, 14);
                CALL partition_maintenance(SCHEMA_NAME, 'history_str', 90, 24, 14);
                CALL partition_maintenance(SCHEMA_NAME, 'history_text', 90, 24, 14);
                CALL partition_maintenance(SCHEMA_NAME, 'history_uint', 90, 24, 14);
                CALL partition_maintenance(SCHEMA_NAME, 'trends', 365, 24, 14);
                CALL partition_maintenance(SCHEMA_NAME, 'trends_uint', 365, 24, 14);
		UPDATE zabbix_partition_monitoring set time=CURRENT_TIMESTAMP WHERE id='1';
END$$
DELIMITER ;


CREATE DEFINER=`root`@`localhost` EVENT `zabbix_partition` ON SCHEDULE EVERY 1 DAY STARTS '2017-01-09 23:50:00' ON COMPLETION NOT PRESERVE ENABLE COMMENT 'zabbix partitioning' DO call partition_maintenance_all('zabbix')
