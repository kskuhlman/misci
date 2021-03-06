-- Create two "monthly files" and populate each with 10 millions rows.  
-- Then try different approaches to building a poor-man's local range partitioning scheme on them.
--    (Which is just a fancy way of saying build a UNION ALL view to join them).
--
-- A successful partitioning scheme will avoid lookups against tables (partitions) that aren't being 
--   "needed" by the query.
-- An earlier iteration of this script left off indexes on one of the partitions to attempt to highlight this.
--  But extra highlighting shouldn't be necessary, and may not even be possible (given MTIs), so now things are uniform.

-- Create tables 
-----------------
execute immediate('set path =' || current_schema); 

drop index jan01; 
drop index jan02;
drop index feb01; 
drop index feb02;

--repeated this until we have 8 million records. (proc below creates 1m
insert into jan (mo, dtanbr, dtachr) select mo, dtanbr, dtachr from jan order by rid;
insert into feb (mo, dtanbr, dtachr) select mo, dtanbr, dtachr from feb order by rid;
commit;    

begin
  declare exit handler for sqlstate '42710' /* already exists, sql0601 */ 
  begin
    execute immediate('set gv_rebuild_tables = ''N''');
  end;  
  execute immediate('create variable gv_rebuild_tables char(1) default(''Y'')');
end;

values gv_rebuild_tables; 
stop; 

begin
  if gv_rebuild_tables = 'Y' then
    drop table jan;
    drop table feb;
  end if;
end;

begin  /* build tables if needed */
  declare create_rows integer default 1000000;
  declare cur_row integer;

  if gv_rebuild_tables = 'Y' then
    /* generate test data */
    
    begin 
      declare continue handler for sqlstate'42704' /* SQL0204 */  begin end;
      drop index jan01;
      drop index jan02;
      drop index feb01;
      drop index feb02;
    end;

    create or replace table jan (
      rid bigint generated always as identity primary key, 
      mo smallint not null, 
      dtanbr smallint not null, 
      dtachr char(10) not null,
      chgtsp timestamp not null generated always for each row on update as row change timestamp
      );
    create or replace table feb like jan including identity column attributes including row change timestamp column attributes;
    
    set cur_row = 1;
      while (cur_row <= create_rows) do
        insert into jan (mo, dtanbr, dtachr) values(01, int(rand() * 255), left(TRANSLATE(CHAR(BIGINT(RAND() * 10000000000)), 'kenkuhlman', '1234567890'),10));
        insert into feb (mo, dtanbr, dtachr) values(02, int(rand() * 255), left(TRANSLATE(CHAR(BIGINT(RAND() * 10000000000)), 'kenkuhlman', '1234567890'),10));
        set cur_row = cur_row + 1;
      end while;
  end if;  
end; 
commit;    

begin
    declare continue handler for sqlstate '42710' /* SQL0601
 */  begin end;
    create index jan01 on jan (mo); 
    create index jan02 on jan (dtanbr);
    create index feb01 on feb (mo); 
    create index feb02 on feb (dtanbr);
end; 


create or replace view months_data_only as (
select dtanbr, dtachr from jan
 union all
select dtanbr, dtachr from feb
);

create or replace view months_data_and_month_nbr as
 (select mo, dtanbr, dtachr from jan
  union all
  select mo, dtanbr, dtachr from feb
);

-- this is identical to above, but won't be used until after we alter the tables to have constraints.
--  having a distinct name to query makes it easier to check costs by query
create or replace view months_data_and_month_nbr_constrained as
 (select mo, dtanbr, dtachr from jan
  union all
  select mo, dtanbr, dtachr from feb
);

-- Adding a derived field to the view is logically equivalent to having the field in the table..
--   but does IBM i handle the predicate pushdown & branch elimination?
-- Ref: Old LUW article: Partitioning in DB2 Using the UNION ALL View
-- https://www.ibm.com/developerworks/data/library/techarticle/0202zuzarte/0202zuzarte.pdf
create or replace view months_data_month_nbr_and_forced_month_name as
 (select char('JANUARY', 10) month_name, x.* from jan x
  union all
  select char('FEBRUARY', 10) month_name, x.* from feb x
);

-- Run queries once to get stats requests in & buffer whatever we can't avoid buffering when we purge purging pools later.
-------------------------------------------------------------
select count(*) from months_data_only where dtanbr = 5;
select count(*) from months_data_and_month_nbr where mo = 2 and dtanbr = 5;
select count(*) from months_data_and_month_nbr_constrained where mo = 2 and dtanbr = 5;
select count(*) from months_data_month_nbr_and_forced_month_name where month_name = 'FEBRUARY' and mo = 2 and dtanbr = 5;
-- Wait a bit to let system auto-gather stats.  Wish I could do a runstats here to force them to create.
--call qsys2.qcmdexc('dlyjob 120'); 


call qsys2.qcmdexc('SETOBJACC OBJ('||current_schema||'/JAN) OBJTYPE(*FILE) POOL(*PURGE)');
call qsys2.qcmdexc('SETOBJACC OBJ('||current_schema||'/FEB) OBJTYPE(*FILE) POOL(*PURGE)');

release mydb;
disconnect mydb; 
values x;
connect to mydb; 


---------------------------------
CALL QSYS2.SET_MONITOR_OPTION(1);
-- CALL QSYS2.SET_MONITOR_OPTION(3);
CALL QSYS2.QCMDEXC('QSYS/STRDBG UPDPROD(*YES)');
CALL QSYS2.OVERRIDE_QAQQINI(2, 'OPEN_CURSOR_THRESHOLD', '-1');

--CALL qsys2.qcmdexc('STRDBMON OUTFILE(dbmon/tmpmon) JOB(*) TYPE(*DETAIL)  COMMENT(DONT_REGISTER_MONITOR)');
CALL qsys2.qcmdexc('STRDBMON OUTFILE(dbmon/tmpmon) JOB(*) TYPE(*DETAIL)');
commit;

begin /* clean up constraints from previous runs */
  declare continue handler for sqlstate '42704' /* SQL0204 */  begin end;
  alter table jan drop constraint only_jan;
  alter table feb drop constraint only_feb;
end; 

begin  /* run perf tests without constraints */
   declare rtv_rows integer default 10000;
   declare cur_row integer;
   
   declare cur_dta smallint;    
   declare junk integer;
      
   set cur_row = 0;      
   while (cur_row <= rtv_rows) do
     set cur_dta = mod(cur_row, 255);
     select count(*) into junk from months_data_only where dtanbr = cur_dta;
     set cur_row = cur_row + 1;
   end while; 
   
   set cur_row = 0;
   while (cur_row <= rtv_rows) do
     set cur_dta = mod(cur_row, 255);
     select count(*) into junk from months_data_and_month_nbr where mo = 2 and dtanbr = cur_dta;
     set cur_row = cur_row + 1;
   end while;

   set cur_row = 0;
   while (cur_row <= rtv_rows) do
     set cur_dta = mod(cur_row, 255);
     select count(*) into junk from months_data_month_nbr_and_forced_month_name where month_name = 'FEBRUARY' and mo = 2 and dtanbr = cur_dta;
     set cur_row = cur_row + 1;
   end while;
end; 


commit;
call qsys2.qcmdexc('ALCOBJ OBJ(('||current_schema||'/JAN *FILE *EXCL)) WAIT(5) CONFLICT(*RQSRLS)'); 
call qsys2.qcmdexc('ALCOBJ OBJ(('||current_schema||'/FEB *FILE *EXCL)) WAIT(5) CONFLICT(*RQSRLS)'); 
call qsys2.qcmdexc('DLCOBJ OBJ(('||current_schema||'/JAN *FILE *EXCL))'); 
call qsys2.qcmdexc('DLCOBJ OBJ(('||current_schema||'/FEB *FILE *EXCL))'); 
call qsys2.qcmdexc('RCLRSC'); 
call qsys2.qcmdexc('RCLACTGRP *ELIGIBLE'); 
commit;rollback;


alter table jan add constraint only_jan check (mo = 1);
alter table feb add constraint only_feb check (mo = 2);
   
-- Run again with check constraint enabled. 
-------------------------------------------
begin
   declare rtv_rows integer default 10000;
   declare cur_row integer;
   
   declare cur_dta smallint; 
   declare junk integer;
   
   set cur_row = 1;
   while (cur_row <= rtv_rows) do
     set cur_dta = mod(cur_row, 255);
     select count(*) into junk from months_data_and_month_nbr_constrained where mo = 2 and dtanbr = cur_dta;
     set cur_row = cur_row + 1;
   end while;
end; 
commit;    

-- commit;  call qsys2.qcmdexc('RCLRSC *'); call qsys2.qcmdexc('dlyjob 120'); 

CALL QSYS2.SET_MONITOR_OPTION(3);
--CALL qsys2.qcmdexc('ENDDBMON JOB(*) COMMENT(DONT_REGISTER_MONITOR)');
CALL qsys2.qcmdexc('ENDDBMON JOB(*)');
CALL QSYS2.QCMDEXC('QSYS/ENDDBG');



drop table qtemp.performance_list_explainable; 
create table qtemp.PERFORMANCE_LIST_EXPLAINABLE as (
  WITH F AS (SELECT QQJFLD FROM dbmon.tmpmon F
               WHERE F.QQRID = 1000 AND QQ1000L IS NOT NULL AND QQUCNT > 0 AND (QVC1C = 'Y' OR QQI8 >= 0)), 
  R AS (SELECT QQJFLD, QQUCNT, QVRCNT AS REFRESH_COUNT,
    CASE
      WHEN QXC16 = '1'
        THEN QQSMINTF
        ELSE NULL
    END AS QQSMINTF, QVP15D, QVP15F, QQI3,
      CASE
        WHEN X.QQI2 < 0
          THEN 0
          ELSE X.QQI2
      END AS QQI2, QQI6, QQ1000L,
        CASE
          WHEN (QQC21 NOT IN ('FE','CL','HC','UP','DL') OR (QQC21 IN ('UP','DL') AND (QQC181 = ' ' OR QVP15D IS NOT NULL))) AND QQ1000L <> ''
            THEN 1
            ELSE 0
        END AS REAL_QUERY, QQSTIM, QVC102, QQJOB, QQUSER, QQJNUM, QQSYS, QQRDBN, QQUDEF
            FROM dbmon.tmpmon X
            WHERE X.QQRID = 1000 AND QQJFLD IN (SELECT QQJFLD
                                                  FROM F) AND X.QQSTIM IS NOT NULL AND X.QQETIM IS NOT NULL AND X.QQ1000 IS NOT NULL AND (X.QVC1C =
                                                  'Y' OR X.QQC21 IN('DC','OP','FE','CL','HC', 'UP','DL','IN','SI','SK','SV','VI',
                                                  'QF','QM','QQ','QR'))), 
    TT AS (SELECT X.QQJFLD, MAX(QQUCNT) AS QQUCNT, MIN(REFRESH_COUNT) AS REFRESH_COUNT, DECIMAL(DECIMAL(MAX(COALESCE(X.QVP15D, X.QQI6)), 28, 6)
              / 1000000, 15, 6) AS QQI6_WORST, DECIMAL(SUM(DECIMAL(X.QQI6, 28, 6)) / 1000000, 15, 6) AS QQI6_TOTAL, DECIMAL(SUM(DECIMAL(X.QQI6, 28,
              6)) / 1000000 / CASE WHEN SUM(COALESCE(X.QVP15F, REAL_QUERY)) = 0 THEN NULL ELSE SUM(COALESCE(X.QVP15F, REAL_QUERY)) END, 15, 6) AS
              QQI6_AVERAGE, DECIMAL(SUM(DECIMAL((COALESCE(X.QQI3, X.QQI2)), 57, 6)) / CASE WHEN SUM(COALESCE(X.QVP15F, REAL_QUERY)) = 0 THEN NULL
              ELSE SUM(COALESCE(X.QVP15F, REAL_QUERY)) END, 17, 6) AS RESULT_ROWS, DECIMAL(CASE WHEN SUM(COALESCE(X.QVP15F, REAL_QUERY)) = 0 THEN
              NULL ELSE SUM(COALESCE(X.QVP15F, REAL_QUERY)) END, 15, 0) AS TOTAL_TIMES_RUN, MAX(CASE WHEN REAL_QUERY = 1 THEN X.QQ1000L ELSE NULL
              END) AS QQ1000L, MIN(X.QQSTIM) QQSTIM, MAX(X.QVC102) AS CURRENT_USER, MAX(X.QQJOB) AS QQJOB, MAX(X.QQUSER) AS QQUSER, MAX(X.QQJNUM)
              AS QQJNUM, MAX(X.QQSYS) AS QQSYS, MAX(X.QQRDBN) AS QQRDBN, MAX(X.QQUDEF) AS QQUDEF
             FROM R X
             GROUP BY X.QQJFLD, QQSMINTF
             HAVING SUM(COALESCE(X.QVP15F, REAL_QUERY)) > 0)
  , ST AS (SELECT QQJFLD, MAX(X.QQINT03) AS TEMP_STORAGE, SUM(CASE WHEN X.QQRID = 3020 THEN 1 ELSE NULL END) AS INDEX_ADVISED, SUM(CASE WHEN
              X.QQRID = 3015 THEN 1 ELSE NULL END) AS STATISTICS_ADVISED, DECIMAL(SUM(CASE WHEN X.QQRID = 3014 THEN DECIMAL(X.QQI5 / 1000, 19, 6)
              ELSE NULL END), 19, 6) AS OPTIMIZATION_TIME, DECIMAL(SUM(CASE WHEN X.QQRID = 3014 THEN DECIMAL(X.QQF1 / 1000000, 19, 6) ELSE NULL
              END), 19, 6) AS ADJUSTED_AVERAGE_TIME, DECIMAL(SUM(CASE WHEN X.QQRID = 3019 THEN DECIMAL(X.QQi1 / 1000, 19, 6) ELSE NULL END), 19, 6)
              AS ROW_RETRIEVAL_CPU_TIME, DECIMAL(SUM(CASE WHEN X.QQRID = 3019 THEN DECIMAL(X.QQI2 / 1000, 19, 6) ELSE NULL END), 19, 6) AS
              ROW_RETRIEVAL_CLOCK_TIME, SUM(CASE WHEN X.QQRID = 3019 THEN DECIMAL(X.QQI3, 19, 6) ELSE NULL END) AS SYNCHRONOUS_DB_READS, SUM(CASE
              WHEN X.QQRID = 3019 THEN DECIMAL(X.QQI4, 19, 6) ELSE NULL END) AS SYNCHRONOUS_DB_WRITES, SUM(CASE WHEN X.QQRID = 3019 THEN
              DECIMAL(X.QQI5, 19, 6) ELSE NULL END) AS ASYNCHRONOUS_DB_READS, SUM(CASE WHEN X.QQRID = 3019 THEN DECIMAL(X.QQI6, 19, 6) ELSE NULL
              END) AS ASYNCHRONOUS_DB_WRITES, SUM(CASE WHEN X.QQRID = 3019 THEN DECIMAL(X.QQi7, 19, 6) ELSE NULL END) AS ROWS_RETURNED_3019,
              SUM(CASE WHEN X.QQRID = 3019 THEN DECIMAL(X.QVP151, 19, 6) ELSE NULL END) AS PAGE_FAULTS, SUM(CASE WHEN X.QQRID = 3019 THEN
              DECIMAL(X.QQI8, 19, 6) ELSE NULL END) AS CALLS_TO_RETRIEVE_ROWS, MAX(CASE WHEN X.QQRID = 3014 THEN QQC83 ELSE NULL END) AS QRO_HASH
             FROM dbmon.tmpmon X
             WHERE X.QQRID IN (3019,3020,3015,3014) AND (QQJFLD,QVRCNT) IN (SELECT QQJFLD,REFRESH_COUNT
                                                  FROM TT)
             GROUP BY X.QQJFLD)
  SELECT X.QQSTIM, X.QQI6_WORST, X.QQI6_TOTAL, X.TOTAL_TIMES_RUN, X.QQI6_AVERAGE,
    CASE
      WHEN X.QQ1000L IS NULL
        THEN 'UNKNOWN' 
        ELSE CAST(X.QQ1000L AS CLOB(2M))  
     END SQL_STATEMENT, X.CURRENT_USER, X.QQJOB, X.QQUSER, X.QQJNUM, Y.OPTIMIZATION_TIME, Y.ADJUSTED_AVERAGE_TIME, COALESCE(Y.INDEX_ADVISED, 0) AS INDEX_ADVISED,
       COALESCE(Y.STATISTICS_ADVISED, 0) AS STATISTICS_ADVISED, COALESCE(Y.TEMP_STORAGE, 0) AS TEMP_STORAGE, X.RESULT_ROWS,
       DECIMAL(ROWS_RETURNED_3019 / X.TOTAL_TIMES_RUN, 19, 6) AS AVG_ROWS_RETURNED_TO_DB, 
       DECIMAL(ROW_RETRIEVAL_CPU_TIME / X.TOTAL_TIMES_RUN, 19, 6) AS AVERAGE_ROW_RETRIEVAL_CPU_TIME, 
       DECIMAL(ROW_RETRIEVAL_CLOCK_TIME / X.TOTAL_TIMES_RUN, 19, 6) AS AVERAGE_ROW_RETRIEVAL_CLOCK_TIME,
       DECIMAL(SYNCHRONOUS_DB_READS / X.TOTAL_TIMES_RUN, 19, 6) AS AVERAGE_SYNCHRONOUS_DB_READS, 
       DECIMAL(SYNCHRONOUS_DB_WRITES / X.TOTAL_TIMES_RUN, 19, 6) AS AVERAGE_SYNCHRONOUS_DB_WRITES, 
       DECIMAL(ASYNCHRONOUS_DB_READS / X.TOTAL_TIMES_RUN, 19, 6) AS AVERAGE_ASYNCHRONOUS_DB_READS,
       DECIMAL(ASYNCHRONOUS_DB_WRITES / X.TOTAL_TIMES_RUN, 19, 6) AS AVERAGE_ASYNCHRONOUS_DB_WRITES, 
       DECIMAL(PAGE_FAULTS / X.TOTAL_TIMES_RUN, 19, 6) AS AVERAGE_PAGE_FAULTS, 
       DECIMAL(CALLS_TO_RETRIEVE_ROWS / X.TOTAL_TIMES_RUN, 19, 6) AS AVERAGE_CALLS_TO_RETRIEVE_ROWS, 
       X.QQSYS, X.QQRDBN, X.QQUDEF, y.QRO_HASH, X.REFRESH_COUNT, X.QQUCNT, X.QQJFLD
    FROM TT X LEFT OUTER
    JOIN ST Y ON X.QQJFLD = Y.QQJFLD
    ORDER BY 1 DESC
  WITH NC
) with data;

create table performance_list_explainable_16million as (select * from qtemp.performance_list_explainable) with data; 
-- alter table performance_list_explain

select * from qtemp.performance_list_explainable order by qqi6_total desc;


stop; 


--select * from qtemp.performance_list_explainable;
--select * from qtemp.monfile; 

-- SELECT SUM(CASE WHEN QVC11 = 'Z' THEN 1 ELSE 0 END) , MAX(QQC102)
--  --  INTO : H : H , : H : H
--   FROM QTEMP.MONFILE
--   WHERE QQRID = 3018
--   WITH NC;
-- 
SELECT 1 -- INTO : H : H
 FROM QTEMP.TMPMONX A
 WHERE QQRID = 3010 AND EXISTS (SELECT 1
                                    FROM QTEMP.TMPMONX B
                                    WHERE QQRID = 1000 AND QQDBCLOB1 IS NULL AND QQC21 NOT IN ('FE' , 'CL' , 'CH' , 'HC') AND NOT (QQC21 IN ('DL' ,
                                      'UP') AND QQC181 > ' ') AND A . QQJFLD = B . QQJFLD AND A . QQI5 = B . QQI5)
FETCH FIRST 1 ROW ONLY;
--   WITH NC;
 -- 0 rows
  
-- SELECT SUM(CASE WHEN QVC13 = 'Z' THEN 1 ELSE 0 END) , MAX(QQC11)
-- -- INTO : H : H , : H : H
--   FROM QTEMP.MONFILE
--   WHERE QQRID = 3018
--   WITH NC;

--call qsys2.qcmdexc('dsplib qtemp *print');

select qqrid, qqtfn, qvqtbl, qvptbl, qqptfn, x.* from qtemp.TMPMONX x 
where -- qqrid = 3020  -- 3000  3015
 qqtfn is not null
order by qqrid, qqtfn;


SELECT * -- 1 -- INTO : H : H
 FROM QTEMP.TMPMONX A
 WHERE QQRID = 3010 AND EXISTS (SELECT 1
                                    FROM QTEMP.TMPMONX B
                                    WHERE QQRID = 1000 AND QQDBCLOB1 IS NULL AND QQC21 NOT IN ('FE' , 'CL' , 'CH' , 'HC') AND NOT (QQC21 IN ('DL' ,
                                      'UP') AND QQC181 > ' ') AND A . QQJFLD = B . QQJFLD AND A . QQI5 = B . QQI5)
--FETCH FIRST 1 ROW ONLY
;





WITH TT
  AS (SELECT A.*,
      (CASE
         WHEN a.qqrid = 1000
           THEN 9000
         WHEN a.qqrid = 3014
             THEN 2914
         WHEN a.qqrid = 5002
             THEN 2902
         WHEN a.qqrid = 3018
             THEN 1 ELSE QQRID
       END) AS SortQQRID
        FROM qtemp.tmpmonx A)
  SELECT TT.*
    FROM TT
    WHERE QQRID = 3018 OR (QQJOB = 'QZDASOINIT' AND QQUSER = 'QUSER     ' AND QQJNUM = '400223' AND QQUCNT = 294 AND (QQRID
      = 3004 OR QQRID = 3007 OR QQRID = 3010 OR QQRID = 3023 OR QQRID = 3014 OR QQRID = 3015 OR QQRID = 5002 OR QQRID = 3019 OR QQRID = 1000) AND
      QVRCNT =
      (SELECT MAX(QVRCNT)
         FROM qtemp.tmpmonx C
         WHERE TT.QQJFLD = C.QQJFLD AND C.QQRID = 3014 AND C.QVRCNT <= 0) AND (QQSMINTF IS NULL OR QQSMINTF =
           (SELECT MAX(QQSMINTF)
              FROM qtemp.tmpmonx B
              WHERE TT.QQJFLD = B.QQJFLD AND TT.QVRCNT = B.QVRCNT AND TT.QQRID = B.QQRID)))
    ORDER BY SortQQRID;
    
create or replace function timeit(SQL_STRING varchar(4096))
returns integer

LANGUAGE SQL                                                    
DETERMINISTIC                                                   
MODIFIES SQL DATA                                               
SPECIFIC timeit
                                                                
SET OPTION DBGVIEW = *SOURCE, COMMIT = *NONE                    
                                                                
BEGIN                                                           
  DECLARE str_ts timestamp;                             
  DECLARE end_ts timestamp;
  DECLARE wk_SQL_STRING varchar(4096);
  
  set wk_SQL_STRING = SQL_STRING;
  set str_ts = current_timestamp; 
  PREPARE S1 FROM wk_SQL_STRING;                                   
  EXECUTE S1;
  set end_ts = current_timestamp;
  return (qsys2.timestampdiff(1, char(end_ts - str_ts)));
END;

--values timeit('select count(*) from sysibm.sysdummy1');


-- 8 million rows:
-- 308.146448	9999	0.030817	SELECT COUNT ( * ) INTO : H : H FROM MONTHS_DATA_AND_MONTH_NBR_CONSTRAINED WHERE MO = 2 AND DTANBR = : H : H  
-- 291.363208	10000	0.029136	SELECT COUNT ( * ) INTO : H : H FROM MONTHS_DATA_AND_MONTH_NBR WHERE MO = 2 AND DTANBR = : H : H  
-- 282.014120	10000	0.028201	SELECT COUNT ( * ) INTO : H : H FROM MONTHS_DATA_MONTH_NBR_AND_FORCED_MONTH_NAME WHERE MONTH_NAME = 'FEBRUARY' AND MO = 2 AND DTANBR = : H : H  
-- 241.584952	10000	0.024158	SELECT COUNT ( * ) INTO : H : H FROM MONTHS_DATA_ONLY WHERE DTANBR = : H : H  

-- 16 million rows 
--595.864552	10000	0.059586	SELECT COUNT ( * ) INTO : H : H FROM MONTHS_DATA_MONTH_NBR_AND_FORCED_MONTH_NAME WHERE MONTH_NAME = 'FEBRUARY' AND MO = 2 AND DTANBR = : H : H
--590.270872	10000	0.059027	SELECT COUNT ( * ) INTO : H : H FROM MONTHS_DATA_AND_MONTH_NBR WHERE MO = 2 AND DTANBR = : H : H
--579.876168	9999	0.057993	SELECT COUNT ( * ) INTO : H : H FROM MONTHS_DATA_AND_MONTH_NBR_CONSTRAINED WHERE MO = 2 AND DTANBR = : H : H
--400.902056	10000	0.040090	SELECT COUNT ( * ) INTO : H : H FROM MONTHS_DATA_ONLY WHERE DTANBR = : H : H

-- 32 million rows 
-- 1125.710576	10000	0.112571	SELECT COUNT ( * ) INTO : H : H FROM MONTHS_DATA_AND_MONTH_NBR WHERE MO = 2 AND DTANBR = : H : H  
-- 1016.307464	10000	0.101630	SELECT COUNT ( * ) INTO : H : H FROM MONTHS_DATA_MONTH_NBR_AND_FORCED_MONTH_NAME WHERE MONTH_NAME = 'FEBRUARY' AND MO = 2 AND DTANBR = : H : H  
-- 1010.622904	9999	0.101072	SELECT COUNT ( * ) INTO : H : H FROM MONTHS_DATA_AND_MONTH_NBR_CONSTRAINED WHERE MO = 2 AND DTANBR = : H : H  
-- 718.970904	10000	0.071897	SELECT COUNT ( * ) INTO : H : H FROM MONTHS_DATA_ONLY WHERE DTANBR = : H : H  


select count(*) from jan;
