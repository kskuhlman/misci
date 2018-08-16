drop table dbmon.ip_client;
create or replace table dbmon.ip_client (profile char(10) not null, ip_address char(15), allowed_query_min decimal(5, 0), hard_limit char(1) default 'N', primary key(profile, ip_address)); 

-- Add a list of known clients & their allowed_query_min.
include 'long_runners_clients.sql'


create or replace view dbmon.longruns as (
WITH ACTIVE_USER_JOBS (Q_JOB_NAME, CPU_TIME, RUN_PRIORITY, profile) AS (
 SELECT JOB_NAME, CPU_TIME, RUN_PRIORITY, AUTHORIZATION_NAME FROM TABLE (QSYS2.ACTIVE_JOB_INFO('NO','','','')) x 
 WHERE JOB_TYPE <> 'SYS' )
select
  case 
    when allowed_query_min is null then 
      case when run_minutes < 5 then 'No' else 'Ask DBA' end 
    when run_minutes >= allowed_query_min then
      case when hard_limit = 'Y' then 'YES' else 'Probably' end
    else 'NO' end kill_safe,
  case when allowed_query_min is null then 'N' else 'Y' end known_address,
  v_client_ip_address ip_address, a.* from ( 
SELECT 
  timestampdiff(4,char(current_timestamp - b.V_SQL_STMT_START_TIMESTAMP )) run_minutes,
  allowed_query_min, a.profile, c.hard_limit,
  'CALL QSYS2.CANCEL_SQL('''||Q_JOB_NAME||''');  /* '||a.profile || ' */' as cancel_stmt,
  'CL: ENDJOB ('||Q_JOB_NAME||');' end_stmt, 
  CPU_TIME JOB_CPU_MS, RUN_PRIORITY, V_SQL_STATEMENT_TEXT, V_SQL_STMT_START_TIMESTAMP,
  V_CLIENT_WRKSTNNAME, V_CLIENT_IP_ADDRESS, V_CLIENT_APPLNAME, V_CLIENT_PROGRAMID, 
  V_CLIENT_USERID, V_QUERY_OPTIONS_LIB_NAME, q_job_name
FROM ACTIVE_USER_JOBS A
JOIN TABLE(QSYS2.GET_JOB_INFO(A.Q_JOB_NAME)) B 
 on b.V_SQL_STMT_STATUS = 'ACTIVE'
left outer JOIN dbmon.ip_client C
 on a.profile = c.profile and b.v_client_ip_address = c.ip_address 
WHERE V_SQL_STMT_START_TIMESTAMP < current_timestamp - 5 minutes
) a
);

select * from dbmon.longruns 
--where kill_safe not in ('Ask DBA') 
order by kill_safe desc, run_minutes desc;


-- 20180206 dpitest 
CALL QSYS2.CANCEL_SQL('335958/QUSER/QZDASOINIT');
