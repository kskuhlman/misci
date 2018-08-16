-- Plan cache-related services:
select service_category, service_schema_name, service_name, sql_object_type, example, earliest_possible_release 
from QSYS2.SERVICES_INFO where service_category in ('DATABASE-PERFORMANCE','DATABASE-PLAN CACHE');
-- DUMP_PLAN_CACHE
-- PERFORMANCE_LIST_EXPLAINABLE
-- LIST_EXPLAINABLE_DETAILED
-- ANALYZE_PLAN_CACHE 
-- EXTRACT_STATEMENTS

set current_schema dbmon;
set path dbmon;

--create or replace table dbmon.plan_raw as (select current_date capture_date, t.* from dbmon.plan_latest t where 1=2) with no data;
--create unique index dbmon.plan_raw1 on dbmon.plan_raw (capture_date, statement_number);

create or replace variable gv_snapshot_schema varchar(10) default('DBMON');
create or replace variable gv_snapshot_name varchar(10) default('M'||to_char(current_date, 'YYMMDD'));

CALL QSYS2.DUMP_PLAN_CACHE(gv_snapshot_schema, gv_snapshot_name);
-- Same as above, jsut a different stored proc.   There are more parms, but they're named filter" 1-36, so who knows what is which :-(
--CALL QSYS2.ANALYZE_PLAN_CACHE( '01         10', gv_snapshot_schema, gv_snapshot_name, X'', 'RE');

-- I found extract_statements to be too incredibly slow.. cancelled after hours of running.  
--  Leaving here in case that changes, just commented out.
-- https://www.ibm.com/developerworks/community/wikis/home?lang=en#!/wiki/IBM%20i%20Technology%20Updates/page/QSYS2.EXTRACT_STATEMENTS()%20procedure
-- Most recent 100 statements
--CALL QSYS2.EXTRACT_STATEMENTS(gv_snapshot_schema, gv_snapshot_name, '*AUDIT', 'AND QQC21 NOT IN (''CH'', ''CL'', ''CN'', ''DE'', ''DI'', ''DM'', ''HC'', ''HH'', ''JR'', ''FE'', ''PD'', ''PR'', ''PD'')', ' ORDER BY QQSTIM DESC FETCH FIRST 100 ROWS ONLY ');
-- Everything over 1 second.
--CALL QSYS2.EXTRACT_STATEMENTS(gv_snapshot_schema, gv_snapshot_name, ADDITIONAL_SELECT_COLUMNS => ' DEC(QQI6)/1000000.0 as Total_time, QVC102 as Current_User_Profile ', ADDITIONAL_PREDICATES => ' AND QQI6 > 1000000 ', ORDER_BY => ' ORDER BY QQI6 DESC ');
-- Everything over 1 second for current_user
--CALL QSYS2.EXTRACT_STATEMENTS(gv_snapshot_schema, gv_snapshot_name, ADDITIONAL_SELECT_COLUMNS => ' DEC(QQI6)/1000000.0 as Total_time, QVC102 as Current_User_Profile ', ADDITIONAL_PREDICATES => ' AND QVC102 = ''''current_user'''' AND QQI6 > 1000000 ', ORDER_BY => ' ORDER BY QQI6 DESC ');

--create or replace variable current_user.gv_statement_text varchar(4096); 
--set current_user.gv_statement_text = 'select a.bar, a.* from sysibm.sysdummy1';
--select * from TABLE(qsys2.parse_statement(current_user.gv_statement_text, '*SYS', '*PERIOD', '*APOSTSQL')) c    ;

----------------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------------

-- This view is straight from the docs:
-- https://www.ibm.com/developerworks/community/wikis/home?lang=en#!/wiki/IBM%20i%20Technology%20Updates/page/Automated%20DBE%20tasks%20for%20Navigator

--set gv_snapshot_name = 'm170718';

begin 
  declare stmt varchar(4096);
  execute immediate ('create or replace alias dbmon.plan_latest for dbmon.'||gv_snapshot_name||'_QQQ1000');
  set stmt = 'create or replace view '||gv_snapshot_name||'_QQQ1000 as (
SELECT QQRID as Row_ID, QQTIME as Time_Created, QQJFLD as Join_Column, QQRDBN as Relational_Database_Name, QQSYS as System_Name, QQJOB as Job_Name, 
  QQUSER as Job_User, QQJNUM as Job_Number, QQI9 as Thread_ID, QQUCNT as Unique_Count, QQI5 as Unique_Refresh_Counter, QQUDEF as User_Defined,
  QQSTN as Statement_Number, QQC11 as Statement_Function, QQC21 as Statement_Operation, QQC12 as Statement_Type, QQC13 as Parse_Required, 
  QQC103 as Package_Name, QQC104 as Package_Library, QQC181 as Cursor_Name, QQC182 as Statement_Name, QQSTIM as Start_Timestamp, 
  QQ1000 as Statement_Text, QQC14 as Statement_Outcome, QQI2 as Result_Rows, QQC22 as Dynamic_Replan_Reason_Code, QQC16 as Data_Conversion_Reason_Code, 
  QQI4 as Total_Time_Milliseconds, QQI3 as Rows_Fetched, QQETIM as End_Timestamp, QQI6 as Total_Time_Microseconds, QQI7 as SQL_Statement_Length, 
  QQI1 as Insert_Unique_Count, QQI8 as SQLCode, QQC81 as SQLState, QVC101 as Close_Cursor_Mode, QVC11 as Allow_Copy_Data_Value, QVC12 as PseudoOpen, 
  QVC13 as PseudoClose, QVC14 as ODP_Implementation, QVC21 as Dynamic_Replan_SubCode, QVC41 as Commitment_Control_Level, QWC1B as Concurrent_Access_Resolution, 
  QVC15 as Blocking_Type, QVC16 as Delay_Prepare, QVC1C as Explainable, QVC17 as Naming_Convention, QVC18 as Dynamic_Processing_Type, 
  QVC19 as LOB_Data_Optimized, QVC1A as Program_User_Profile_Used, QVC1B as Dynamic_User_Profile_Used, QVC1281 as Default_Collection, 
  QVC1282 as Procedure_Name, QVC1283 as Procedure_Library, QQCLOB2 as SQL_Path, QVC1284 as Current_Schema, QQC18 as Binding_Type, 
  QQC61 as Cursor_Type, QVC1D as Statement_Originator, QQC15 as Hard_Close_Reason_Code, QQC23 as Hard_Close_Subcode, QVC42 as Date_Format, 
  QWC11 as Date_Separator, QVC43 as Time_Format, QWC12 as Time_Separator, QWC13 as Decimal_Point, QVC104 as Sort_Sequence_Table ,
  QVC105 as Sort_Sequence_Library, QVC44 as Language_ID, QVC23 as Country_ID, QQIA as First_N_Rows_Value, QQF1 as Optimize_For_N_Rows_Value, 
  QVC22 as SQL_Access_Plan_Reason_Code, QVC24 as Access_Plan_Not_Saved_Reason_Code, QVC81 as Transaction_Context_ID, QVP152 as Activation_Group_Mark, 
  QVP153 as Open_Cursor_Threshold, QVP154 as Open_Cursor_Close_Count, QVP155 as Commitment_Control_Lock_Limit, QWC15 as Allow_SQL_Mixed_Constants, 
  QWC16 as Suppress_SQL_Warnings, QWC17 as Translate_ASCII, QWC18 as System_Wide_Statement_Cache, QVP159 as LOB_Locator_Threshold, 
  QVP156 as Max_Decimal_Precision, QVP157 as Max_Decimal_Scale, QVP158 as Min_Decimal_Divide_Scale, QWC19 as Unicode_Normalization, 
  QQ1000L as Statement_Text_Long, QVP15B as Old_Access_Plan_Length, QVP15C as New_Access_Plan_Length, QVP151 as Fast_Delete_Count, 
  QQF2 as Statement_Max_Compression, QVC102 as Current_User_Profile, QVC1E as Expression_Evaluator_Used, QVP15A as Host_Server_Delta, 
  QQC301 as NTS_Lock_Space_Id, QQC183 as IP_Address, QFC11 as IP_Type, QQSMINT2 as IP_Port_Number, QVC3004 as NTS_Transaction_Id,  
  QQSMINT3 as NTS_Format_Id_Length, QQSMINT4 as NTS_Transatction_ID_SubLength, QVRCNT as Unique_Refresh_Counter2, QVP15F as Times_Run, 
  QVP15E as FullOpens, QVC1F as Proc_In_Cache, QWC1A as Combined_Operation, QVC3001 as Client_Applname, QVC3002 as Client_Userid, 
  QVC3003 as Client_Wrkstnname, QVC3005 as Client_Acctng, QVC3006 as Client_Progamid, QVC5001 as Interface_Information, 
  QVC82 as Open_Options, QWC1D as Extended_Indicators, QWC1C as DECFLOAT_Rounding_Mode, QWC1E as SQL_DECFLOAT_Warnings, 
  QVP15D as Worst_Time_Micro, QQINT05 as SQ_Unique_Count, QFC13 as Concurrent_Access_Res_Used, QQSMINT8 as SQL_Scalar_UDFs_Not_Inlined, 
  QVC3007 as Result_Set_Cursor, QFC12 as Implicit_XMLPARSE_Option, QQSMINT7 as SQL_XML_Data_CCSID, QQSMINT5 as OPTIMIZER_USE, 
  QFC14 as XML_Schema_In_Cache 
FROM '||gv_snapshot_name||'
WHERE QQRID=1000)';
 execute immediate stmt;
end;

insert into dbmon.plan_raw select current_date capture_date, t.* from dbmon.plan_latest t;

select count(*) from dbmon.plan_raw;

select count(*) from (
select distinct system_name, current_date extract_date, -- job_name, --job_user, job_number, current_user_profile, Thread_ID, 
  -- statement_number, 
  cursor_name, statement_name, 
  package_library, Parse_Required, Procedure_Name, procedure_library, statement_function, sqlcode, sqlstate, 
  --total_time_microseconds, total_time_milliseconds, worst_time_micro, 
  statement_text, statement_text_long,  
  --start_timestamp, end_timestamp, fullopens, 
  Allow_Copy_Data_Value, first_n_rows_value, 
  --ip_address, --times_run,  
  commitment_control_level, statement_operation, statement_outcome, --insert_unique_count, 
  sql_statement_length, 
  data_conversion_reason_code, --result_rows, rows_fetched, 
  optimize_for_n_rows_value, pseudoopen, odp_implementation, 
  sql_access_plan_reason_code, access_plan_not_saved_reason_code, hard_close_reason_code, Hard_Close_Subcode, 
  dynamic_replan_reason_code, dynamic_replan_subcode, 
  open_options,  old_access_plan_length, new_access_plan_length, system_wide_statement_cache,
   1
from dbmon.m170718_QQQ1000 
) a;

select count(*) from (
select distinct system_name, current_date extract_date, -- job_name, --job_user, job_number, current_user_profile, Thread_ID, 
  -- statement_number, 
--  cursor_name, statement_name, 
  --package_library ,
   Parse_Required, Procedure_Name, procedure_library, statement_function, 
  sqlcode, sqlstate, 
  --total_time_microseconds, total_time_milliseconds, worst_time_micro, 
  statement_text, statement_text_long,  
  --start_timestamp, end_timestamp, fullopens, 
--  Allow_Copy_Data_Value, first_n_rows_value, 
  --ip_address, --times_run,  
--  commitment_control_level, statement_operation, statement_outcome, --insert_unique_count, 
--  sql_statement_length, --
--  data_conversion_reason_code, --result_rows, rows_fetched, 
--  optimize_for_n_rows_value, pseudoopen, odp_implementation, 
--  sql_access_plan_reason_code, access_plan_not_saved_reason_code, hard_close_reason_code, Hard_Close_Subcode, 
--  dynamic_replan_reason_code, dynamic_replan_subcode, 
--  open_options,  old_access_plan_length, new_access_plan_length, system_wide_statement_cache ,
   1
from dbmon.m170718_QQQ1000 
) a;

order by statement_text;

--29,054 
--4,161

select count(*) from (
select distinct system_name, current_date extract_date, -- job_name, --job_user, job_number, current_user_profile, Thread_ID, 
  -- statement_number, 
--  cursor_name, statement_name, 
  --package_library ,
   Parse_Required, Procedure_Name, procedure_library, statement_function, 
  sqlcode, sqlstate, 
  --total_time_microseconds, total_time_milliseconds, worst_time_micro, 
  statement_text, statement_text_long,  
  --start_timestamp, end_timestamp, fullopens, 
--  Allow_Copy_Data_Value, first_n_rows_value, 
  --ip_address, --times_run,  
--  commitment_control_level, statement_operation, statement_outcome, --insert_unique_count, 
--  sql_statement_length, --
--  data_conversion_reason_code, --result_rows, rows_fetched, 
--  optimize_for_n_rows_value, pseudoopen, odp_implementation, 
--  sql_access_plan_reason_code, access_plan_not_saved_reason_code, hard_close_reason_code, Hard_Close_Subcode, 
--  dynamic_replan_reason_code, dynamic_replan_subcode, 
--  open_options,  old_access_plan_length, new_access_plan_length, system_wide_statement_cache ,
   1
from dbmon.m170718_QQQ1000 
) a;
-- 2518 distinct statements
-- 2582 with program & result
-- 90% compression this way, so we'll go with it! 

  
drop table qtemp.foo;

select * from dbmon.m170718_QQQ1000 s where statement_number in (10002808, 9441561);


--create table qtemp.foo as (
WITH 
plan_statements (naming_mode, dec_point, string_delim, statement_number, statement_text) AS (
  select '*SYS' naming_mode, '*PERIOD' dec_point, '*APOSTSQL' string_delim, statement_number, statement_text_long 
  from dbmon.m170718_QQQ1000 s 
  --where statement_number in (10002808, 9441561) --statement_text like ' SELECT XSOUSI%' and statement_number = 9441561      
  ),
parsed_statements (seq, name_type, name, schema, column_name, name_start_position, statement_number, statement_text) AS (
  SELECT row_number() over(partition by statement_number order by name_start_position) seq, name_type, c.name, c.schema, c.column_name, c.name_start_position, statement_number,
     statement_text
  FROM plan_statements, TABLE(qsys2.parse_statement(statement_text, naming_mode, dec_point, string_delim)) c
  WHERE c.name_type = 'TABLE'  --- TABLE, FUNCTION, COLUMN
  ),
stripped_statements (seq, name_type, name, schema, column_name, name_start_position, statement_number, statement_text) as (
  select seq, name_type, name, schema, column_name, name_start_position, statement_number,
    regexp_replace(a.statement_text, a.schema||'.','',1,1,'i') statement_text 
  from parsed_statements a where seq = 1
  union all
  select b.seq, b.name_type, b.name, b.schema, b.column_name, b.name_start_position, b.statement_number,
    regexp_replace(a.statement_text, b.schema||'.','',1,1,'i') statement_text 
  from stripped_statements a, parsed_statements b
  where a.statement_number = b.statement_number and b.seq = a.seq + 1 --and b.seq = 2
  ),
final_statement (statement_number, statement_text_parsed) as (
  select statement_number, statement_text from stripped_statements a
  where (a.statement_number, a.seq) in (select statement_number, max(b.seq) from stripped_statements b group by statement_number)) 
select *  
from final_statement --stripped_statements --parsed_statements --plan_statements --final_statement
ORDER BY statement_number;

select * from qtemp.foo a where seq = 1; 
select * from qtemp.foo b where seq = 2;
with
stripped_statements (seq, name_type, name, schema, column_name, name_start_position, statement_text) as (
  select seq, name_type, name, schema, column_name, name_start_position, 
    regexp_replace(a.statement_text, a.schema||'.','',1,1,'i') statement_text 
  from qtemp.foo a where seq = 1
  union all
  select b.seq, a.name_type, a.name, a.schema, a.column_name, a.name_start_position, 
    regexp_replace(a.statement_text, b.schema||'.','',1,1,'i') statement_text 
  from stripped_statements a, qtemp.foo b
  where b.seq = a.seq + 1  --and b.seq = 2    
) 
select * 
from stripped_statements
ORDER BY 1, schema, name
;
