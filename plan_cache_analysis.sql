select service_category, service_schema_name, service_name, sql_object_type, example, earliest_possible_release from QSYS2.SERVICES_INFO where service_category in ('DATABASE-PERFORMANCE','DATABASE-PLAN CACHE');

set current_schema dbmon;
set path dbmon;

-- https://www.ibm.com/developerworks/community/wikis/home?lang=en#!/wiki/IBM%20i%20Technology%20Updates/page/QSYS2.EXTRACT_STATEMENTS()%20procedure
create or replace variable gv_snapshot_schema char(10) default('DBMON');
create or replace variable gv_snapshot_name char(10) default('M'||to_char(current_date, 'YYMMDD'));

CALL QSYS2.DUMP_PLAN_CACHE(gv_snapshot_schema, gv_snapshot_name);
-- Same as above, jsut a different stored proc.   There are more parms, but they're named filter" 1-36, so who knows what is which :-(
--CALL QSYS2.ANALYZE_PLAN_CACHE( '01         10', gv_snapshot_schema, gv_snapshot_name, X'', 'RE');

-- I found extract_statements to be too incredibly slow.. cancelled after hours of running.  
--  Leaving here in case that changes, just commented out.
-- Most recent 100 statements
--CALL QSYS2.EXTRACT_STATEMENTS(gv_snapshot_schema, gv_snapshot_name, '*AUDIT', 'AND QQC21 NOT IN (''CH'', ''CL'', ''CN'', ''DE'', ''DI'', ''DM'', ''HC'', ''HH'', ''JR'', ''FE'', ''PD'', ''PR'', ''PD'')', ' ORDER BY QQSTIM DESC FETCH FIRST 100 ROWS ONLY ');
-- Everything over 1 second.
--CALL QSYS2.EXTRACT_STATEMENTS(gv_snapshot_schema, gv_snapshot_name, ADDITIONAL_SELECT_COLUMNS => ' DEC(QQI6)/1000000.0 as Total_time, QVC102 as Current_User_Profile ', ADDITIONAL_PREDICATES => ' AND QQI6 > 1000000 ', ORDER_BY => ' ORDER BY QQI6 DESC ');
-- Everything over 1 second for current_user
--CALL QSYS2.EXTRACT_STATEMENTS(gv_snapshot_schema, gv_snapshot_name, ADDITIONAL_SELECT_COLUMNS => ' DEC(QQI6)/1000000.0 as Total_time, QVC102 as Current_User_Profile ', ADDITIONAL_PREDICATES => ' AND QVC102 = ''''current_user'''' AND QQI6 > 1000000 ', ORDER_BY => ' ORDER BY QQI6 DESC ');
