CREATE TABLE IF NOT EXISTS `cc_log` (
  `id` BIGINT(20) unsigned NOT NULL AUTO_INCREMENT,
  `partition_key` VARCHAR(255) NOT NULL,
  `change_value` TEXT NOT NULL,
  `is_partitioning` BIT(1) NOT NULL,
  PRIMARY KEY (`id`)
);
||||
CREATE TABLE IF NOT EXISTS `cc_log_meta` (
  `partition_key` VARCHAR(255) NOT NULL,
  `num_jobs` INT(11) NOT NULL,
  `lock_id` VARCHAR(255) DEFAULT NULL,
  `locked_at` INT(11) DEFAULT NULL,
  PRIMARY KEY (`partition_key`)
);
||||
CREATE TABLE IF NOT EXISTS `cc_log_settings` (
  `id` BIT NOT NULL,
  `is_partitioning` BIT NOT NULL,
  `num_partitions` INT UNSIGNED NOT NULL,
  `cdc_lambda` VARCHAR(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
);
||||
CREATE TABLE IF NOT EXISTS `cc_log_dev_jobs` (
  `id` BIGINT(20) unsigned NOT NULL AUTO_INCREMENT,
  `partition_key` VARCHAR(255) NOT NULL,
  `lock_id` VARCHAR(255) NOT NULL,
  PRIMARY KEY (`id`)
);
||||
INSERT IGNORE INTO cc_log_settings (id, is_partitioning, num_partitions) VALUES (1, 0, 4);
||||
DROP FUNCTION IF EXISTS BASE64_ESCAPE_TEXT;
||||
CREATE FUNCTION BASE64_ESCAPE_TEXT(v TEXT)
RETURNS TEXT
DETERMINISTIC
BEGIN
    RETURN (
		CASE WHEN v IS NULL THEN
			'null'
        ELSE
			CONCAT('"', TO_BASE64(v), '"')
        END
	);
END
||||
DROP FUNCTION IF EXISTS JSON_KEY_VALUE_OBJECT;
||||
CREATE FUNCTION JSON_KEY_VALUE_OBJECT(k VARCHAR(255), v TEXT, dateType VARCHAR(255))
RETURNS TEXT
DETERMINISTIC
BEGIN
    RETURN (
        CASE WHEN v IS NULL THEN
            CONCAT('{ "key": ', BASE64_ESCAPE_TEXT(k), ', "dataType": "', dateType, '" }')
        ELSE
            CONCAT('{ "key": ', BASE64_ESCAPE_TEXT(k), ', "value": ', v, ', "dataType": "', dateType, '" }')
        END
    );
END
||||
DROP FUNCTION IF EXISTS JSON_KEY_VALUE_STRING;
||||
CREATE FUNCTION JSON_KEY_VALUE_STRING(k VARCHAR(255), v TEXT)
RETURNS TEXT
DETERMINISTIC
BEGIN
    DECLARE rs TEXT;
	RETURN (
		CASE WHEN v IS NULL THEN
			JSON_KEY_VALUE_OBJECT(k, NULL, 'string')

		ELSE
            JSON_KEY_VALUE_OBJECT(k, BASE64_ESCAPE_TEXT(v), 'string')
		END
    );
END
||||
DROP FUNCTION IF EXISTS JSON_KEY_VALUE_INTEGER;
||||
CREATE FUNCTION JSON_KEY_VALUE_INTEGER(k VARCHAR(255), v BIGINT)
RETURNS TEXT
DETERMINISTIC
BEGIN
    DECLARE rs TEXT;
    RETURN (
        CASE WHEN v IS NULL THEN
            JSON_KEY_VALUE_OBJECT(k, NULL, 'integer')
        ELSE
            JSON_KEY_VALUE_OBJECT(k, CONCAT('"', v, '"'), 'integer')
        END
    );
END
||||
DROP FUNCTION IF EXISTS JSON_KEY_VALUE_DOUBLE;
||||
CREATE FUNCTION JSON_KEY_VALUE_DOUBLE(k VARCHAR(255), v DOUBLE)
RETURNS TEXT
DETERMINISTIC
BEGIN
    DECLARE rs TEXT;
    RETURN (
        CASE WHEN v IS NULL THEN
            JSON_KEY_VALUE_OBJECT(k, NULL, 'double')
        ELSE
            JSON_KEY_VALUE_OBJECT(k, CONCAT('"', v, '"'), 'double')
        END
    );
END
||||
DROP FUNCTION IF EXISTS JSON_KEY_VALUE_TIME;
||||
CREATE FUNCTION JSON_KEY_VALUE_TIME(k VARCHAR(255), v TIME)
RETURNS TEXT
DETERMINISTIC
BEGIN
    DECLARE rs TEXT;
    RETURN (
        CASE WHEN v IS NULL THEN
            JSON_KEY_VALUE_OBJECT(k, NULL, 'time')
        ELSE
            JSON_KEY_VALUE_OBJECT(k, CONCAT('"', TIME_TO_SEC(v), '"'), 'time')
        END
    );
END
||||
DROP FUNCTION IF EXISTS JSON_KEY_VALUE_BINARY;
||||
CREATE FUNCTION JSON_KEY_VALUE_BINARY(k VARCHAR(255), v LONGBLOB)
RETURNS TEXT
DETERMINISTIC
BEGIN
    DECLARE rs TEXT;
    RETURN (
        CASE WHEN v IS NULL THEN
            JSON_KEY_VALUE_OBJECT(k, NULL, 'binary')
        ELSE
            JSON_KEY_VALUE_OBJECT(k, CONCAT('"', TO_BASE64(v), '"'), 'binary')
        END
    );
END
||||
DROP FUNCTION IF EXISTS JSON_KEY_VALUE_TIMESTAMP;
||||
CREATE FUNCTION JSON_KEY_VALUE_TIMESTAMP(k VARCHAR(255), v TIMESTAMP)
RETURNS TEXT
DETERMINISTIC
BEGIN
    DECLARE rs TEXT;
	RETURN (
		CASE WHEN v IS NULL THEN
			JSON_KEY_VALUE_OBJECT(k, NULL, 'date')

		ELSE
            JSON_KEY_VALUE_OBJECT(k, CONCAT('"', DATE_FORMAT(CONVERT_TZ(v, @@session.time_zone, '+00:00'), '%Y-%m-%dT%TZ'), '"'), 'date')
		END
    );
END
||||
DROP FUNCTION IF EXISTS JSON_KEY_VALUE_DATETIME;
||||
CREATE FUNCTION JSON_KEY_VALUE_DATETIME(k VARCHAR(255), v DATETIME)
RETURNS TEXT
DETERMINISTIC
BEGIN
    DECLARE rs TEXT;
	RETURN (
		CASE WHEN v IS NULL THEN
			JSON_KEY_VALUE_OBJECT(k, NULL, 'date')

		ELSE
            JSON_KEY_VALUE_OBJECT(k, CONCAT('"', DATE_FORMAT(v, '%Y-%m-%dT%T'), '"'), 'date')
		END
    );
END
||||
DROP FUNCTION IF EXISTS JSON_KEY_VALUE_DECIMAL;
||||
CREATE FUNCTION JSON_KEY_VALUE_DECIMAL(k VARCHAR(255), v DECIMAL(60, 30))
RETURNS TEXT
DETERMINISTIC
BEGIN
    DECLARE rs TEXT;
	RETURN (
		CASE WHEN v IS NULL THEN
			JSON_KEY_VALUE_OBJECT(k, NULL, 'decimal')

		ELSE
            JSON_KEY_VALUE_OBJECT(k, CONCAT('"', v, '"'), 'decimal')
		END
    );
END
||||
DROP FUNCTION IF EXISTS JSON_KEY_VALUE_BOOLEAN;
||||
CREATE FUNCTION JSON_KEY_VALUE_BOOLEAN(k VARCHAR(255), v BIT)
RETURNS TEXT
DETERMINISTIC
BEGIN
    DECLARE rs TEXT;
	RETURN (
		CASE WHEN v IS NULL THEN
			JSON_KEY_VALUE_OBJECT(k, NULL, 'boolean')
		WHEN v = 1 THEN
            JSON_KEY_VALUE_OBJECT(k, '"true"', 'boolean')
        ELSE
            JSON_KEY_VALUE_OBJECT(k, '"false"', 'boolean')
		END
    );
END
||||
DROP FUNCTION IF EXISTS JSON_CHANGE_CAPTURE;
||||
CREATE FUNCTION JSON_CHANGE_CAPTURE(action VARCHAR(255), tableName VARCHAR(255), newValue TEXT, oldValue TEXT)
RETURNS TEXT
DETERMINISTIC
BEGIN
	RETURN CONCAT('{ "action": "', action, '", "tableName": "', tableName, '", "newValue": ', newValue, ', "oldValue": ', oldValue, ' }');
END
||||
DROP FUNCTION IF EXISTS CC_HASH_CODE;
||||
CREATE FUNCTION CC_HASH_CODE (v TEXT) RETURNS INT(11)
    DETERMINISTIC
BEGIN
    DECLARE hashVal VARCHAR(255);
    DECLARE rs INTEGER;
    DECLARE increment INTEGER;
    DECLARE currentChar VARCHAR(1);
    DECLARE currentIndex INTEGER;
    SET currentIndex = 1;
    SET rs = 0;
    SET hashVal = LOWER(MD5(v));
    iterator:
    LOOP
		IF currentIndex > LENGTH(hashVal) THEN
			LEAVE iterator;
		END IF;
		SET currentChar = SUBSTRING(hashVal, currentIndex, 1);
        SET increment = (
            CASE WHEN currentChar = 'a' THEN 10
            WHEN currentChar = 'b' THEN 11
            WHEN currentChar = 'c' THEN 12
            WHEN currentChar = 'd' THEN 13
            WHEN currentChar = 'e' THEN 14
            WHEN currentChar = 'f' THEN 15
            WHEN currentChar = 'g' THEN 16
            WHEN currentChar = 'h' THEN 17
            WHEN currentChar = 'i' THEN 18
            WHEN currentChar = 'j' THEN 19
            WHEN currentChar = 'k' THEN 20
            WHEN currentChar = 'l' THEN 21
            WHEN currentChar = 'm' THEN 22
            WHEN currentChar = 'n' THEN 23
            WHEN currentChar = 'o' THEN 24
            WHEN currentChar = 'p' THEN 25
            WHEN currentChar = 'q' THEN 26
            WHEN currentChar = 'r' THEN 27
            WHEN currentChar = 's' THEN 28
            WHEN currentChar = 't' THEN 29
            WHEN currentChar = 'u' THEN 30
            WHEN currentChar = 'v' THEN 31
            WHEN currentChar = 'w' THEN 32
            WHEN currentChar = 'x' THEN 33
            WHEN currentChar = 'y' THEN 34
            WHEN currentChar = 'z' THEN 35
            ELSE CAST(currentChar AS UNSIGNED)
            END
        );
        SET rs = rs + increment;
        SET currentIndex = currentIndex + 1;
	END LOOP;
    RETURN rs;
END
||||
DROP FUNCTION IF EXISTS CC_PARTITION_VALUE;
||||
CREATE FUNCTION CC_PARTITION_VALUE (v TEXT, numPartitions INT(11)) RETURNS INT(11)
    DETERMINISTIC
BEGIN
	RETURN CC_HASH_CODE(v) % numPartitions;
END
||||
DROP TRIGGER IF EXISTS `CC_CC_LOG_ON_INSERT`;
||||
CREATE TRIGGER `CC_CC_LOG_ON_INSERT` AFTER INSERT ON `cc_log` FOR EACH ROW
    BEGIN
    IF NEW.`is_partitioning` = 0 THEN
        INSERT INTO cc_log_meta (partition_key, num_jobs) VALUES (NEW.`partition_key`, 1) ON DUPLICATE KEY UPDATE num_jobs = num_jobs + 1;
    END IF;
END
||||
DROP TRIGGER IF EXISTS `CC_CC_LOG_ON_DELETE`;
||||
CREATE TRIGGER `CC_CC_LOG_ON_DELETE` AFTER DELETE ON `cc_log` FOR EACH ROW
    BEGIN
    IF OLD.`is_partitioning` = 0 THEN
        INSERT INTO cc_log_meta (partition_key, num_jobs) VALUES (OLD.`partition_key`, 0) ON DUPLICATE KEY UPDATE num_jobs = num_jobs - 1;
    END IF;
END
||||
DROP TRIGGER IF EXISTS `CC_CC_LOG_ON_UPDATE`;
||||
CREATE TRIGGER `CC_CC_LOG_ON_UPDATE` AFTER UPDATE ON `cc_log` FOR EACH ROW
BEGIN
    IF OLD.`is_partitioning` = 1 AND NEW.`is_partitioning` = 0 THEN
        INSERT INTO cc_log_meta (partition_key, num_jobs) VALUES (NEW.`partition_key`, 1) ON DUPLICATE KEY UPDATE num_jobs = num_jobs + 1;
    END IF;
END
||||
DROP PROCEDURE IF EXISTS CC_TRIGGER_PARTITION_JOB;
||||
CREATE PROCEDURE CC_TRIGGER_PARTITION_JOB (
    IN partitionKey VARCHAR(255),
	IN lockId VARCHAR(255)
) LANGUAGE SQL
BEGIN
    DECLARE cdcLambda VARCHAR(255);
    SET cdcLambda = (SELECT cdc_lambda FROM cc_log_settings WHERE id = 1);
    IF cdcLambda IS NOT NULL THEN
        CALL mysql.lambda_async(
            cdcLambda,
            CONCAT('{ "partitionKey": "', partitionKey, '", "lockId": "', lockId, '" }')
        );
    ELSE
        INSERT INTO cc_log_dev_jobs(lock_id, partition_key) VALUES (lockId, partitionKey);
    END IF;
END
||||
DROP TRIGGER IF EXISTS `CC_CC_LOG_META_BEFORE_INSERT`;
||||
CREATE TRIGGER `CC_CC_LOG_META_BEFORE_INSERT` BEFORE INSERT ON `cc_log_meta` FOR EACH ROW
BEGIN
    IF NEW.`num_jobs` = 1 THEN
        SET NEW.`lock_id` = UUID();
        SET NEW.`locked_at` = UNIX_TIMESTAMP();
    END IF;
END
||||
DROP TRIGGER IF EXISTS `CC_CC_LOG_META_BEFORE_UPDATE`;
||||
CREATE TRIGGER `CC_CC_LOG_META_BEFORE_UPDATE` BEFORE UPDATE ON `cc_log_meta` FOR EACH ROW
BEGIN
    IF NEW.`num_jobs` = 1 AND OLD.`num_jobs` = 0 THEN
        SET NEW.`lock_id` = UUID();
        SET NEW.`locked_at` = UNIX_TIMESTAMP();
    ELSEIF NEW.`num_jobs` = 0 AND OLD.`num_jobs` = 1 THEN
        SET NEW.`lock_id` = NULL;
        SET NEW.`locked_at` = NULL;
    END IF;
END
||||
DROP TRIGGER IF EXISTS `CC_CC_LOG_META_AFTER_INSERT`;
||||
CREATE TRIGGER `CC_CC_LOG_META_AFTER_INSERT` AFTER INSERT ON `cc_log_meta` FOR EACH ROW
BEGIN
    IF NEW.`num_jobs` = 1 AND NEW.`lock_id` IS NOT NULL THEN
        CALL CC_TRIGGER_PARTITION_JOB(NEW.`partition_key`, NEW.`lock_id`);
    END IF;
END
||||
DROP TRIGGER IF EXISTS `CC_CC_LOG_META_AFTER_UPDATE`;
||||
CREATE TRIGGER `CC_CC_LOG_META_AFTER_UPDATE` AFTER UPDATE ON `cc_log_meta` FOR EACH ROW
BEGIN
    IF NEW.`num_jobs` = 1 AND OLD.`num_jobs` = 0 AND NEW.`lock_id` IS NOT NULL THEN
        CALL CC_TRIGGER_PARTITION_JOB(NEW.`partition_key`, NEW.`lock_id`);
    END IF;
END