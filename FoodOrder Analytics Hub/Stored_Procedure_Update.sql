CALL execute_update_statement(
    'UPDATE BUSINESS.fact_order_patterns SET TOTAL_SPENT=30 WHERE DATE_KEY=20240115','fact_order_patterns'
);

DROP PROCEDURE execute_update_statement;


DELIMITER $$

CREATE PROCEDURE execute_update_statement (
    IN p_sql TEXT,IN Table_Names TEXT
)
BEGIN
	declare v_load_start_time DATETIME;
	declare p_rows_updated INT;
    -- Move local variable into session variable
    SET @sql_text = p_sql;

    -- Prepare from session variable
    PREPARE stmt FROM @sql_text;
	set v_load_start_time=NOW();
    -- Execute dynamic SQL
    EXECUTE stmt;

    SET p_rows_updated = ROW_COUNT();
    -- Cleanup
    DEALLOCATE PREPARE stmt;
    
       INSERT INTO Stage.etl_load_audit (
        job_name,
        source_schema,
        source_table,
        target_schema,
        target_table,
        load_start_time,
        load_end_time,
        records_updated,
        load_status,
        executed_by,
        execution_date
    )
    VALUES (
        'UPDATION_LOAD',
        'Stage',
        'NA',
        'BUSINESS',
        Table_Names,
        v_load_start_time,
        NOW(),
        p_rows_updated,
        'SUCCESS',
        'ETL_LOAD',
        CURDATE()
    );
END$$

DELIMITER ;


