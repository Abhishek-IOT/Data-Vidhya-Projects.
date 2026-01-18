-- WE need to set the parameter in the pipeline and then we will be able to do it dynamically.




-- Safety check: prevent accidental full-table updat
IF '@{pipeline().parameters.p_set}' = ''
BEGIN
    THROW 50001, 'SET clause cannot be empty', 1;
END;

IF '@{pipeline().parameters.p_where}' = ''
BEGIN
    THROW 50002, 'WHERE clause is mandatory in Fabric', 1;
END;

-- Execute UPDATE (Fabric-supported)
UPDATE @{pipeline().parameters.p_target_schema}.@{pipeline().parameters.p_target_table}
SET @{pipeline().parameters.p_set}
WHERE @{pipeline().parameters.p_where};
