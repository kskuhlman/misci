drop table ip_client;
create or replace table ip_client (profile char(10) not null, ip_address char(15), allowed_query_min decimal(5, 0), hard_limit char(1) default 'N', primary key(profile, ip_address)); 

-- Add a list of known clients & their allowed_query_min.
include 'long_runners_data.sql'


create or replace view longruns as (
WITH ACTIVE_USER_JOBS (Q_JOB_NAME, CPU_TIME, RUN_PRIORITY, profile) AS (
 SELECT JOB_NAME, CPU_TIME, RUN_PRIORITY, AUTHORIZATION_NAME FROM TABLE (QSYS2.ACTIVE_JOB_INFO('NO','','','')) x 
 WHERE JOB_TYPE <> 'SYS' )
select
  char(case 
    when allowed_min is null then 
      case when run_mins < 5 then 'No' else 'Ask DBA' end 
    when run_mins >= allowed_min then
      case when hard_limit = 'Y' then 'YES' else 'Probably' end
    else 'NO' end,  10) kill_safe,
  char(case when allowed_min is null then 'N' else 'Y' end, 1) known_address,
  v_client_ip_address ip_address, a.* from ( 
SELECT 
  timestampdiff(4,char(current_timestamp - b.V_SQL_STMT_START_TIMESTAMP )) run_mins,
  allowed_query_min allowed_min, a.profile, c.hard_limit,
  char('CALL QSYS2.CANCEL_SQL('''||Q_JOB_NAME||''');  /* '||a.profile || ' */', 128) as kill_stmt,
  char('CL: ENDJOB ('||Q_JOB_NAME||');', 40) end_stmt, 
  CPU_TIME JOB_CPU_MS, RUN_PRIORITY, V_SQL_STATEMENT_TEXT, V_SQL_STMT_START_TIMESTAMP,
  V_CLIENT_WRKSTNNAME, V_CLIENT_IP_ADDRESS, V_CLIENT_APPLNAME, V_CLIENT_PROGRAMID, 
  V_CLIENT_USERID, V_QUERY_OPTIONS_LIB_NAME, char(q_job_name, 32) job_name
FROM ACTIVE_USER_JOBS A
JOIN TABLE(QSYS2.GET_JOB_INFO(A.Q_JOB_NAME)) B 
 on b.V_SQL_STMT_STATUS = 'ACTIVE'
left outer JOIN ip_client C
 on a.profile = c.profile and b.v_client_ip_address = c.ip_address 
WHERE V_SQL_STMT_START_TIMESTAMP < current_timestamp - 2 minutes
) a
);
DROP TABLE LONGRUNH; 
create table longrunh as (select current_timestamp capture_time, a.* from longruns a) with no data;


select a.* 
from longruns a 
where kill_safe = 'YES'
order by run_mins desc;

-- From results of above.. 
-- CALL QSYS2.CANCEL_SQL('207308/QUSER/QZDASOINIT');  /* BADUSER1 */
