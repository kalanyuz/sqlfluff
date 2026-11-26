USE MODULES hive, core;

LOAD MODULE hive WITH ('hive-version' = '3.1.3');

LOAD MODULE core;

UNLOAD MODULE hive;

RESET;

RESET 'table.planner';

RESET table.local-time-zone;

SET table.local-time-zone = UTC;

SET execution.checkpointing.interval = 5 s;

SET table.planner = blink;

ADD JAR '/path/to/my-udf.jar';

REMOVE JAR '/path/to/my-udf.jar';

STOP JOB '228d70913eab60dda85c5e7f78b5782c';

STOP JOB '228d70913eab60dda85c5e7f78b5782c' WITH SAVEPOINT;

STOP JOB '228d70913eab60dda85c5e7f78b5782c' WITH SAVEPOINT WITH DRAIN;

CALL sys.generate_n('my_table', 5);

ANALYZE TABLE my_table COMPUTE STATISTICS;

ANALYZE TABLE my_table PARTITION (dt = '2026-01-01', region)
COMPUTE STATISTICS FOR COLUMNS col1, col2;

ANALYZE TABLE my_table COMPUTE STATISTICS FOR ALL COLUMNS;

CREATE CATALOG IF NOT EXISTS `my-cat` COMMENT 'catalog description'
WITH (
    'type' = 'generic_in_memory'
);

COMPILE PLAN '/tmp/plan.json' IF NOT EXISTS
FOR INSERT INTO sink_table
SELECT *
FROM source_table;

COMPILE AND EXECUTE PLAN '/tmp/plan.json'
FOR STATEMENT SET
BEGIN
    INSERT INTO sink_a SELECT * FROM source_a;
    INSERT INTO sink_b SELECT * FROM source_b;
END;

EXECUTE PLAN '/tmp/plan.json';
