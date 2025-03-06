USE DATABASE PRISOFT_DB;
USE SCHEMA HR_STUDENT;

----------------------------------------------------------- SAMPLE VIEWS --------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW PRISOFT_DB.HR_STUDENT.COUNTRIES_EUROPE_D AS (
SELECT * FROM PRISOFT_DB.HR_STUDENT.COUNTRIES
WHERE REGION_ID = (select 1 from PRISOFT_DB.HR_STUDENT.EMP_SALARY_V)
);
CREATE OR REPLACE VIEW HR_STUDENT.EMPLOYEE_DEPT_V AS 
(
SELECT 
E.FIRST_NAME||' '||E.LAST_NAME AS FULL_NAME,
D.DEPARTMENT_NAME,
E.EMAIL,
E.PHONE_NUMBER
FROM PRISOFT_DB.HR_STUDENT.EMPLOYEES E,
PRISOFT_DB.HR_STUDENT.DEPARTMENTS D,
PRISOFT_DB.HR_STUDENT.LOCATIONS L
WHERE 1=1
AND E.DEPARTMENT_ID = D.DEPARTMENT_ID
AND D.LOCATION_ID = L.LOCATION_ID
AND L.COUNTRY_ID IN (SELECT COUNTRY_ID FROM HR_STUDENT.COUNTRIES_EUROPE_D)
);
CREATE OR REPLACE VIEW HR_STUDENT.JOB_HIST_EMP_V AS 
(
SELECT 
E.EMPLOYEE_ID,
E.FIRST_NAME||' '||E.LAST_NAME AS FULL_NAME,
JH.START_DATE,
JH.END_DATE
FROM PRISOFT_DB.HR_STUDENT.EMPLOYEES E,
PRISOFT_DB.HR_STUDENT.JOB_HISTORY JH
WHERE 1=1
AND E.EMPLOYEE_ID = JH.EMPLOYEE_ID
);
CREATE OR REPLACE VIEW HR_STUDENT.EMP_SALARY_V AS 
(
SELECT
E.EMPLOYEE_ID,
E.FIRST_NAME||' '||E.LAST_NAME AS FULL_NAME,
E.SALARY,
J.JOB_TITLE,
J.MIN_SALARY,
J.MAX_SALARY
FROM PRISOFT_DB.HR_STUDENT.EMPLOYEES E,
PRISOFT_DB.HR_STUDENT.JOBS J
WHERE
E.JOB_ID = J.JOB_ID
);
---------------------------------------------------------------------------------------------------------------------------------------
SELECT TABLE_NAME,* FROM INFORMATION_SCHEMA.TABLES
WHERE 1=1
AND TABLE_SCHEMA = 'HR_STUDENT'
AND TABLE_TYPE = 'VIEW';

SELECT TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE 1=1
AND TABLE_SCHEMA = 'HR_STUDENT'
AND TABLE_TYPE = 'BASE TABLE';

------------------------------------------------ FIND TABLE SELECT STATEMENT ----------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------
USE DATABASE PRISOFT_DB;
SET DATABASE_NAME = 'PRISOFT_DB';
SET SCHEMA_NAME = 'HR_STUDENT';
SET VIEW_NAME = 'EMPLOYEE_DEPT_V';
SET VIEW_DDL_SEARCH = $DATABASE_NAME||'.'||$SCHEMA_NAME||'.'||$VIEW_NAME;

SELECT 
$VIEW_DDL_SEARCH AS VIEW_TO_SEARCH,
--IFF(TABLE_TYPE='BASE TABLE',TABLE_NAME, NULL) AS BASE_TABLE,
TABLE_CATALOG||'.'||TABLE_SCHEMA||'.'||TABLE_NAME SOURCE_TABLE,
TABLE_TYPE SOURCE_TABLE_TYPE,
-- REGEXP_COUNT(GET_DDL('VIEW',$VIEW_DDL_SEARCH),'.*'||TABLE_NAME||'.*') AS OCCURANCE
-- REGEXP_COUNT(GET_DDL('VIEW',$VIEW_DDL_SEARCH),'(^|[^a-zA-Z0-9_])'||TABLE_NAME||'([ \n)])') AS OCCURANCE
REGEXP_COUNT(GET_DDL('VIEW',$VIEW_DDL_SEARCH),'(^|[^a-zA-Z0-9_])'||TABLE_SCHEMA||'.'||TABLE_NAME||'([ \n)])') AS OCCURANCE
FROM INFORMATION_SCHEMA.TABLES
WHERE 1=1
AND TABLE_SCHEMA||'.'||TABLE_NAME != $SCHEMA_NAME||'.'||$VIEW_NAME
HAVING OCCURANCE > 0
;
SELECT * FROM INFORMATION_SCHEMA.TABLES;

SELECT GET_DDL('VIEW','PRISOFT_DB.HR_STUDENT.EMPLOYEE_DEPT_V');
SELECT REGEXP_COUNT(GET_DDL('VIEW', 'PRISOFT_DB.HR_STUDENT.EMPLOYEE_DEPT_V'), '.*DEPARTMENTS.*');

------------------------------------------------ CREATE TABLE TO STORE BACKTRACK ------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------
-- DROP TABLE PUBLIC.SOURCE_BACKTRACK;
CREATE OR REPLACE TABLE HR_STUDENT.SOURCE_BACKTRACK (
VIEW_TO_SEARCH VARCHAR(100),
VIEW_SQL VARCHAR,
LEVEL_1_TABLE VARCHAR(100),
LEVEL_1_TABLE_TYPE VARCHAR(100),
LEVEL_2_TABLE VARCHAR(100),
LEVEL_2_TABLE_TYPE VARCHAR(100),
LEVEL_3_TABLE VARCHAR(100),
LEVEL_3_TABLE_TYPE VARCHAR(100),
LEVEL_4_TABLE VARCHAR(100),
LEVEL_4_TABLE_TYPE VARCHAR(100),
LEVEL_5_TABLE VARCHAR(100),
LEVEL_5_TABLE_TYPE VARCHAR(100),
SOURCE_NAME VARCHAR(100)
);

------------------------------------- GET_DEPENDENT_TABLES FROM INDIVIDUAL NAMES ------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE HR_STUDENT.GET_DEPENDENT_TABLES(
    DATABASE_NAME STRING, 
    SCHEMA_NAME STRING, 
    VIEW_NAME STRING
)
-- RETURNS TABLE(TABLE_NAME STRING, TABLE_TYPE STRING, OCCURANCE NUMBER)
RETURNS TABLE()
LANGUAGE SQL
AS
BEGIN
    LET VIEW_DDL_SEARCH STRING := DATABASE_NAME || '.' || SCHEMA_NAME || '.' || VIEW_NAME;
    LET RESULTS RESULTSET := (
        SELECT VIEW_TO_SEARCH,SOURCE_TABLE,SOURCE_TABLE_TYPE
        FROM (
            SELECT 
            :VIEW_DDL_SEARCH AS VIEW_TO_SEARCH,
            TABLE_CATALOG||'.'||TABLE_SCHEMA||'.'||TABLE_NAME SOURCE_TABLE,
            TABLE_TYPE SOURCE_TABLE_TYPE,
            REGEXP_COUNT(GET_DDL('VIEW',:VIEW_DDL_SEARCH),'(^|[^a-zA-Z0-9_])'||TABLE_SCHEMA||'.'||TABLE_NAME||'([ \n)])') AS OCCURANCE
            FROM INFORMATION_SCHEMA.TABLES
            WHERE 1=1
            AND TABLE_SCHEMA||'.'||TABLE_NAME != :SCHEMA_NAME||'.'||:VIEW_NAME
            HAVING OCCURANCE > 0
            )
        );
    RETURN TABLE(RESULTS);
END;
CALL HR_STUDENT.GET_DEPENDENT_TABLES('PRISOFT_DB', 'HR_STUDENT', 'EMPLOYEE_DEPT_V');
------------------------------------- GET_DEPENDENT_TABLES FROM CONCATENATED NAMES ----------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE HR_STUDENT.GET2_DEPENDENT_TABLES(
    VIEW_TO_SEARCH STRING
)
RETURNS TABLE()
LANGUAGE SQL
AS
BEGIN
    LET VIEW_DDL_SEARCH STRING := :VIEW_TO_SEARCH;
    LET DATABASE_NAME STRING := SPLIT_PART(:VIEW_DDL_SEARCH, '.', 1);
    LET SCHEMA_NAME STRING := SPLIT_PART(:VIEW_TO_SEARCH, '.', 2);
    LET VIEW_NAME STRING := SPLIT_PART(:VIEW_TO_SEARCH, '.', 3);
    LET RESULTS RESULTSET := (
        SELECT VIEW_TO_SEARCH,SOURCE_TABLE,SOURCE_TABLE_TYPE
        FROM (
            SELECT 
            :VIEW_DDL_SEARCH AS VIEW_TO_SEARCH,
            TABLE_CATALOG||'.'||TABLE_SCHEMA||'.'||TABLE_NAME SOURCE_TABLE,
            TABLE_TYPE SOURCE_TABLE_TYPE,
            REGEXP_COUNT(GET_DDL('VIEW',:VIEW_DDL_SEARCH),'(^|[^a-zA-Z0-9_])'||TABLE_SCHEMA||'.'||TABLE_NAME||'([ \n)])') AS OCCURANCE
            FROM INFORMATION_SCHEMA.TABLES
            WHERE 1=1
            AND TABLE_SCHEMA||'.'||TABLE_NAME != :SCHEMA_NAME||'.'||:VIEW_NAME
            HAVING OCCURANCE > 0
            )
        );
    RETURN TABLE(RESULTS);
END;
CALL HR_STUDENT.GET2_DEPENDENT_TABLES('PRISOFT_DB.HR_STUDENT.COUNTRIES_EUROPE_D');
------------------------------------- GET 5 LEVEL TABLE NAMES ----------------------------------------------------
------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE HR_STUDENT.GET_ALL_DEPENDENT_TABLES(
    VIEW_TO_SEARCH STRING,
    INSERT_MODE STRING
)
RETURNS VARCHAR(10000)
LANGUAGE SQL
AS
DECLARE 
records RESULTSET;
max_level INTEGER;

BEGIN
    IF(:INSERT_MODE='CLEAN') THEN
        TRUNCATE TABLE HR_STUDENT.SOURCE_BACKTRACK;
    END IF;
    LET VIEW_DDL_SEARCH STRING := :VIEW_TO_SEARCH;
    LET DATABASE_NAME STRING := SPLIT_PART(:VIEW_DDL_SEARCH, '.', 1);
    LET SCHEMA_NAME STRING := SPLIT_PART(:VIEW_TO_SEARCH, '.', 2);
    LET VIEW_NAME STRING := SPLIT_PART(:VIEW_TO_SEARCH, '.', 3);
    /* Level_1 */ max_level := 1;
    LET l1_records RESULTSET:= (CALL HR_STUDENT.GET2_DEPENDENT_TABLES(:VIEW_TO_SEARCH));
    FOR record IN l1_records DO
        LET v1_view_to_search STRING := record.VIEW_TO_SEARCH;
        LET v1_source_table STRING := record.SOURCE_TABLE;
        LET v1_source_table_type STRING := record.SOURCE_TABLE_TYPE;

        IF(:v1_source_table_type='VIEW') THEN
            /* Level_2 */ max_level := 2;
            LET l2_records RESULTSET := (CALL HR_STUDENT.GET2_DEPENDENT_TABLES(:v1_source_table));
            
            FOR record IN l2_records DO
                LET v2_view_to_search STRING:= record.VIEW_TO_SEARCH;
                LET v2_source_table STRING:= record.SOURCE_TABLE;
                LET v2_source_table_type STRING:= record.SOURCE_TABLE_TYPE;

                IF(:v2_source_table_type='VIEW') THEN
                    /* Level_3 */  max_level := 3;
                    LET l3_records RESULTSET := (CALL HR_STUDENT.GET2_DEPENDENT_TABLES(:v2_source_table));
                    FOR record IN l3_records DO
                        LET v3_view_to_search STRING:= record.VIEW_TO_SEARCH;
                        LET v3_source_table STRING:= record.SOURCE_TABLE;
                        LET v3_source_table_type STRING:= record.SOURCE_TABLE_TYPE;
        
                        IF(:v3_source_table_type='VIEW') THEN
                            /* Level_4 */  max_level := 4;
                            LET l4_records RESULTSET := (CALL HR_STUDENT.GET2_DEPENDENT_TABLES(:v3_source_table));
                            FOR record IN l4_records DO
                                LET v4_view_to_search STRING:= record.VIEW_TO_SEARCH;
                                LET v4_source_table STRING:= record.SOURCE_TABLE;
                                LET v4_source_table_type STRING:= record.SOURCE_TABLE_TYPE;
                
                                IF(:v4_source_table_type='VIEW') THEN
                                    /* Level_5 */  max_level := 5;
                                    LET l5_records RESULTSET := (CALL HR_STUDENT.GET2_DEPENDENT_TABLES(:v4_source_table));
                                    FOR record IN l5_records DO
                                        LET v5_view_to_search STRING:= record.VIEW_TO_SEARCH;
                                        LET v5_source_table STRING:= record.SOURCE_TABLE;
                                        LET v5_source_table_type STRING:= record.SOURCE_TABLE_TYPE;
                        
                                        IF(:v5_source_table_type='VIEW') THEN
                                            /* Level_6 */
                                            -- LET l6_records RESULTSET := (CALL HR_STUDENT.GET2_DEPENDENT_TABLES(:v5_source_table));
                                            CONTINUE;
                                        ELSE 
                                            --Insert Level_5
                                            INSERT INTO HR_STUDENT.SOURCE_BACKTRACK (VIEW_TO_SEARCH,LEVEL_1_TABLE,LEVEL_1_TABLE_TYPE,LEVEL_2_TABLE,LEVEL_2_TABLE_TYPE,
                                            LEVEL_3_TABLE,LEVEL_3_TABLE_TYPE,LEVEL_4_TABLE,LEVEL_4_TABLE_TYPE,LEVEL_5_TABLE,LEVEL_5_TABLE_TYPE)
                                            VALUES (:v1_view_to_search,:v1_source_table,:v1_source_table_type,:v2_source_table,:v2_source_table_type,
                                            :v3_source_table,:v3_source_table_type,:v4_source_table,:v4_source_table_type,:v5_source_table,:v5_source_table_type);
                                        END IF;
                                    END FOR;
                                    /* Level_5 END */ 
                                ELSE 
                                    --Insert Level_4
                                    INSERT INTO HR_STUDENT.SOURCE_BACKTRACK (VIEW_TO_SEARCH,LEVEL_1_TABLE,LEVEL_1_TABLE_TYPE,LEVEL_2_TABLE,LEVEL_2_TABLE_TYPE,
                                    LEVEL_3_TABLE,LEVEL_3_TABLE_TYPE,LEVEL_4_TABLE,LEVEL_4_TABLE_TYPE)
                                    VALUES (:v1_view_to_search,:v1_source_table,:v1_source_table_type,:v2_source_table,:v2_source_table_type,
                                    :v3_source_table,:v3_source_table_type,:v4_source_table,:v4_source_table_type);
                                END IF;
                            END FOR;
                            /* Level_4_END */ 
                        ELSE 
                            --Insert Level_3
                            INSERT INTO HR_STUDENT.SOURCE_BACKTRACK (VIEW_TO_SEARCH,LEVEL_1_TABLE,LEVEL_1_TABLE_TYPE,LEVEL_2_TABLE,LEVEL_2_TABLE_TYPE,LEVEL_3_TABLE,LEVEL_3_TABLE_TYPE)
                            VALUES (:v1_view_to_search,:v1_source_table,:v1_source_table_type,:v2_source_table,:v2_source_table_type,:v3_source_table,:v3_source_table_type);
                        END IF;  
                    END FOR;
                    /* Level_3 END */ 
                ELSE 
                    --Insert Level_2
                    INSERT INTO HR_STUDENT.SOURCE_BACKTRACK (VIEW_TO_SEARCH,LEVEL_1_TABLE,LEVEL_1_TABLE_TYPE,LEVEL_2_TABLE,LEVEL_2_TABLE_TYPE)
                    VALUES (:v1_view_to_search,:v1_source_table,:v1_source_table_type,:v2_source_table,:v2_source_table_type);
                END IF;  
            END FOR;
            /* Level_2 END */ 
        ELSE
            --Insert level_1
            INSERT INTO HR_STUDENT.SOURCE_BACKTRACK (VIEW_TO_SEARCH, LEVEL_1_TABLE,LEVEL_1_TABLE_TYPE) 
            VALUES (:v1_view_to_search, :v1_source_table, :v1_source_table_type);
        END IF;
    END FOR;
    /* Level_1_END */
    
    COMMIT;
    RETURN 'SUCCESS | MAX LEVEL= '||max_level;
END;

CALL HR_STUDENT.GET_ALL_DEPENDENT_TABLES('PRISOFT_DB.HR_STUDENT.EMPLOYEE_DEPT_V','CLEAN');

-- TRUNCATE TABLE HR_STUDENT.SOURCE_BACKTRACK;
SELECT * FROM HR_STUDENT.SOURCE_BACKTRACK;




