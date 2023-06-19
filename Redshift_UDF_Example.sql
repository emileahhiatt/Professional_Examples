

CREATE OR REPLACE PROCEDURE final_schema.final_table_f(p_edl_runid int8)
LANGUAGE plpgsql
AS $$

/* *************************************************************************************************************************************************************************

Name: final_table_f()
Description: This UDF joins data from multiple sources and writes to a new table. It also includes logging and error handling.
Target table : final_table

----

Change History:

Date       Name             IR     Modification           Author     

-------- -----------------  ---   ------------        ------------------------

7/20/21   final_table_f()   1.0    Created               Emileah Hiatt
9/14/21   final_table_f()   1.1    First change          Emileah Hiatt
9/29/22   final_table_f()   1.2    Second change         Emileah Hiatt
5/27/23   final_table_f()   1.3    Third change          Emileah Hiatt

************************************************************************************************************************************************************************ */

DECLARE

exec_end_datetime timestamp without time zone;
p_STARTTIME TIMESTAMP := getdate();
p_DELETED NUMERIC := 0;
p_INSERTED NUMERIC := 0;
p_TABLE_NAME TEXT := 'final_table';
p_FUNCTION_NAME TEXT := 'final_schema.'||p_TABLE_NAME||'_f()';
p_SYSTEM_NAME TEXT ;

BEGIN

/*******************LOGGING*******************/

SELECT DISTINCT solution_name INTO p_SYSTEM_NAME FROM log_schema.log_table1 WHERE object_name = p_FUNCTION_NAME;
RAISE NOTICE 'System Name captured %', p_SYSTEM_NAME;
RAISE NOTICE 'Function Name captured %', p_FUNCTION_NAME;
EXECUTE 'call log_schema.log_table2('||p_EDL_RUNID||',''Sourcing'','''||p_SYSTEM_NAME||''','''||p_FUNCTION_NAME||''','''',''Start'',''Prod2Analytic'',''SQL'',0,0,0,0,'''||p_FUNCTION_NAME||' has been started'',0,0,NULL)';
commit;

/*********************************************/

---------------------------------------------------------------------------------------------------------------------

drop table if exists Temp1;
CREATE TEMPORARY TABLE Temp1 AS
(SELECT T1.column1
		, T1.column2
		, T2.column3
		, COALESCE(T2.column4, 'TEXT') AS column4
		, DATE_ADD('DAYS', 2, DATE_TRUNC('week', T1.date1))::date AS date1
		, SUM(T1.column5) AS column5
		, MAX(T2.date2) AS date2
		, MAX(T2.date3) AS date3
FROM schema1.table1 T1
LEFT JOIN schema1.table2 T2
ON T1.unique_id = T2.unique_id
AND T1.date1 BETWEEN T2.date2 AND T2.date3
GROUP BY T1.column1, T1.column2, T2.column3, COALESCE(T2.column4, 'TEXT'), DATE_ADD('DAYS', 2, DATE_TRUNC('week', T1.date1))::date);

RAISE NOTICE 'Temp1 Created';

drop table if exists Temp2;
CREATE TEMPORARY TABLE Temp2 AS
(SELECT P.unique_id
	, CASE WHEN (T1.preferredname IS NULL OR T1.preferredname = '') THEN T1.lastname || ', ' || T1.firstname
		ELSE T1.lastname || ', ' || T1.preferredname END AS fullname
	, T1.country
	, T1.city
	, T1.manager_id
	, T1.managerfirstname
	, T1.managerlastname
	, T1.organization
FROM schema2.people P
LEFT JOIN schema2.location L
ON P.unique_id = L.unique_id
AND P.status = 'Active'
UNION
SELECT P.unique_id
	, CASE WHEN (T1.preferredname IS NULL OR T1.preferredname = '') THEN T1.lastname || ', ' || T1.firstname
		ELSE T1.lastname || ', ' || T1.preferredname END AS fullname
	, T1.country
	, T1.city
	, T1.manager_id
	, T1.managerfirstname
	, T1.managerlastname
	, T1.organization
FROM schema2.ex_people P
LEFT JOIN schema2.location L
ON P.unique_id = L.unique_id
AND P.status = 'Inactive');

RAISE NOTICE 'Temp2 Created';

drop table if exists Final;
CREATE TEMPORARY TABLE Final AS 
(SELECT T2.unique_id
	, T2.fullname
	, T2.country
	, T2.city
	, T2.manager_id
	, T2.managerfirstname
	, T2.managerlastname
	, T2.organization
	, T1.column2
	, T1.column3
	, T1.column4
	, T1.date1
	, T1.column5
	, T1.date2
	, T1.date3
FROM Temp2 T2
INNER JOIN Temp1 T1
ON T2.unique_id = T1.column1
AND T1.date1 >= GETDATE());

RAISE NOTICE 'Final Created';

IF (SELECT COUNT(*) FROM Final) = 0 THEN
RAISE EXCEPTION 'No rows to insert.';
END IF;

TRUNCATE TABLE final_schema.final_table;

INSERT INTO final_schema.final_table

SELECT unique_id
	, fullname
	, country
	, city
	, manager_id
	, managerfirstname
	, managerlastname
	, organization
	, column2
	, column3
	, column4
	, date1
	, column5
	, date2
	, date3 
FROM Final;

GET DIAGNOSTICS p_INSERTED = ROW_COUNT;

-----------------------------------------------------------------------------

RAISE notice 'Loaded rowcount: %', p_INSERTED;
RAISE notice 'Deleted rowcount: %', p_DELETED;
commit;

/*******************LOGGING*******************/

EXECUTE 'call log_schema.log_table2('||p_EDL_RUNID||',''Sourcing'','''||p_SYSTEM_NAME||''','''||p_FUNCTION_NAME||''','''',''Finish'',''Prod2Analytic'',''SQL'','||p_INSERTED||',0,'||p_DELETED||',0,'''||p_FUNCTION_NAME||' has been finished'',0,0,NULL)';

/*********************************************/

EXCEPTION WHEN OTHERS THEN
RAISE WARNING 'final_table_f has error as % Error Message: %', SQLSTATE,SQLERRM; -- AWSMIGR;

--

END

$$

;

