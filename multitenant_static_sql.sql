/* Show access plan rebuild reasons for static embedded SQL in an RPG pgm.  */
/* Simulate production use in a multi-tenancy arrangement with tennant data */ 
/*   in different schemas and one object library.  */ 

/* cleanup previous runs */
BEGIN
  declare monid varchar(10);
  DECLARE continue handler FOR SQLEXCEPTION BEGIN END;
  CREATE schema spcloclck;
  CREATE schema spcloclck1;
  CREATE schema spcloclck2;
  call qcmdexc('RMVLIBLE spcloclck1');
  call qcmdexc('RMVLIBLE spcloclck2');
  DROP TABLE spcloclck.invisilock;
  DROP SEQUENCE spcloclck.sq_srcseq;
  call qcmdexc('dltpgm spcloclck/invisilock');
  drop table spcloclck1.mydummy;
  drop table spcloclck2.mydummy;
  set monid = (SELECT QQC101 FROM spcloclck.dbmon WHERE QQRID = 3018);
  call qcmdexc('ENDDBMON MONID('||monid||')');
END;

-- Ensure we're starting with clean stats.
select case when count(*) > 0 then RAISE_ERROR('38E00','Expected 0 rows') else 'ok' end 
from QSYS2.sysprogramstmtstat s 
WHERE system_program_schema = 'SPCLOCLCK' AND program_name = 'INVISILOCK';
select case when count(*) > 0 then RAISE_ERROR('38E00','Expected 0 rows') else 'ok' end 
from QSYS2.SYSPROGRAMSTAT 
WHERE system_program_schema = 'SPCLOCLCK' AND program_name = 'INVISILOCK';
 
 
SET SCHEMA spcloclck;
CL: CHGCURLIB spcloclck;
 
-- Create a dummy table in two schemas.  
-- Yes, the indexes are real dummies but they help us create a variety of plans in an otherwise simple testcase.
CREATE TABLE spcloclck1.mydummy (myid int not null generated always as identity primary key, mycol int);
create index spcloclck1.mydummy1 on spcloclck1.mydummy (mycol) where mycol = 1;
create index spcloclck1.mydummy2 on spcloclck1.mydummy (mycol) where mycol = 2;
create encoded vector index spcloclck1.mydummy3 on spcloclck1.mydummy (mycol) where mycol = 1;
create encoded vector index spcloclck1.mydummy4 on spcloclck1.mydummy (mycol) where mycol = 2;
CREATE TABLE spcloclck2.mydummy (myid int not null generated always as identity primary key, mycol int);
create index spcloclck2.mydummy1 on spcloclck2.mydummy (mycol) where mycol = 1;
create index spcloclck2.mydummy2 on spcloclck2.mydummy (mycol) where mycol = 2;
create encoded vector index spcloclck2.mydummy3 on spcloclck2.mydummy (mycol) where mycol = 1;
create encoded vector index spcloclck2.mydummy4 on spcloclck2.mydummy (mycol) where mycol = 2;

-- Populate them with data that's skewed differently.
--   Schema 1 has tons of rows of 1s and once each with a 2 & a 3.  The 3s are unindexed.
--   For schema 2 it's almost all 2s & one 1 & one 3.
-- This encourages different plans for different libraries.
insert into spcloclck1.mydummy (mycol)
with md (mycol) AS (VALUES(1) UNION ALL SELECT mycol+1 FROM md WHERE mycol < 1000000)
select 1 from md;
INSERT INTO spcloclck1.mydummy (mycol) VALUES(2);
INSERT INTO spcloclck1.mydummy (mycol) VALUES(3);

insert into spcloclck2.mydummy (mycol)
with md (mycol) AS (VALUES(1) UNION ALL SELECT mycol+1 FROM md WHERE mycol < 1000000)
select 2 from md;
INSERT INTO spcloclck2.mydummy (mycol) VALUES(1);
INSERT INTO spcloclck2.mydummy (mycol) VALUES(3);
commit;


-- Create a trivial RPG program to access our dummy using system naming (i.e. *libl).
CREATE OR REPLACE SEQUENCE sq_srcseq;
CL: CRTSRCPF FILE(spcloclck/INVISILOCK) RCDLEN(112) MBR(INVISILOCK) TEXT('demo sql pgm lock');
set current schema spcloclck;
delete from invisilock;
INSERT INTO invisilock (srcseq, srcdta) VALUES(NEXTVAL FOR sq_srcseq, '**FREE');
INSERT INTO invisilock (srcseq, srcdta) VALUES(NEXTVAL FOR sq_srcseq, 'dcl-ds foo ExtName(''MYDUMMY'') qualified end-ds;');
INSERT INTO invisilock (srcseq, srcdta) VALUES(NEXTVAL FOR sq_srcseq, 'exec sql values round(rand()*3,0) + 1 into :foo.mycol;');
INSERT INTO invisilock (srcseq, srcdta) VALUES(NEXTVAL FOR sq_srcseq, 'exec sql SELECT myid into :foo.myid FROM mydummy');
INSERT INTO invisilock (srcseq, srcdta) VALUES(NEXTVAL FOR sq_srcseq, '  where mycol=:foo.mycol fetch first 1 row only;');
INSERT INTO invisilock (srcseq, srcdta) VALUES(NEXTVAL FOR sq_srcseq, '*inlr = *on;');
commit;

CL: ADDLIBLE spcloclck1 *first;
CL: CRTSQLRPGI OBJ(spcloclck/INVISILOCK) SRCFILE(INVISILOCK) COMMIT(*NONE) OPTION(*SYS) ALWCPYDTA(*OPTIMIZE) CLOSQLCSR(*ENDACTGRP) DBGVIEW(*SOURCE);

-- Call once just to get initial plan created.
CALL invisilock;
CL: RMVLIBLE spcloclck1;
 
-- Find the initial access plan lengths
SELECT statement_text, access_plan_length 
FROM qsys2.sysprogramstmtstat s 
WHERE program_schema = 'SPCLOCLCK' AND program_name = 'INVISILOCK';
-- Sample result: 
-- VALUES ROUND ( RAND ( ) * 3 , 0 ) + 1 INTO : H  	12736
-- SELECT MYID INTO : H FROM MYDUMMY WHERE MYCOL = : H FETCH FIRST 1 ROW ONLY  	13344

-- and used size
SELECT program_used_size, number_statements, isolation, default_schema, naming 
FROM QSYS2.SYSPROGRAMSTAT WHERE system_program_schema = 'SPCLOCLCK' AND program_name = 'INVISILOCK';

-- Start a monitor 
CL: STRDBMON OUTFILE(DBMON) JOB(GROWPLAN) TYPE(*DETAIL);

-- A dumb, CPU-burning, but trivial way to sleep for durations in microseconds
CREATE OR REPLACE PROCEDURE spcloclck.DUMB_SLEEP (ms INTEGER)
BEGIN
  DECLARE endts TIMESTAMP;
  SET endts = CURRENT TIMESTAMP + MS microseconds;
  while (CURRENT TIMESTAMP <= endts) do
   begin end;
  END while;
END;

-- Call the program numerous times, alternating schemas every 10th call.
begin 
  declare cnt int;
  declare lib char(10);  
  declare sqlcode int;
  declare stmt varchar(4096);
  declare env char(1);
  declare other_env char(1);
  
  set cnt = 1;  
  while (cnt <= 100) do
    
    set env = cast(mod(cnt / 10, 2) + 1 as char(1));
    if (env = '1') then 
      set other_env = '2';
    else
      set other_env = '1';
    end if;
    
    begin
      DECLARE continue handler FOR SQLSTATE '38501' BEGIN END;    
      call qcmdexc('ADDLIBLE spcloclck'||env);
      call qcmdexc('RMVLIBLE spcloclck'||other_env);
    end;
    
    -- Sleep for a fraction of a second so jobs have a chance at a lock.
    call spcloclck.dumb_sleep(int(rand() * 100000)); 
    
    -- Submit half to interactive, half to batch.
    if ((rand() * 4) < 2.0) then
      call qcmdexc('SBMJOB CMD(CALL spcloclck/invisilock) JOB(GROWPLAN) JOBQ(QINTER) INLLIBL(*CURRENT) LOG(0 40 *NOLIST)');
    else
      call qcmdexc('SBMJOB CMD(CALL spcloclck/invisilock) JOB(GROWPLAN) JOBQ(QUSRNOMAX) INLLIBL(*CURRENT) LOG(0 40 *NOLIST)');
    end if;
    
    set cnt = cnt +1;
  end while;

  -- Wait for those jobs to complete.  
  set cnt = -1; 
  while (cnt <> 0) do
    set cnt = (SELECT count(*) FROM TABLE(QSYS2.JOB_INFO(JOB_SUBMITTER_FILTER => '*JOB',JOB_USER_FILTER => '*ALL')) X 
      where not (COMPLETION_STATUS	= 'NORMAL'));
    call spcloclck.dumb_sleep(100000); 
  end while;  
end;


-- End monitor
call qcmdexc('ENDDBMON MONID('||(SELECT distinct QQC101 FROM spcloclck.dbmon WHERE QQRID = 3018)||')'); 
  

-- Check the access path length after calling.
SELECT statement_text, access_plan_length, number_rebuilds 
FROM qsys2.sysprogramstmtstat s 
WHERE program_schema = 'SPCLOCLCK' AND program_name = 'INVISILOCK';
-- Example data:
-- VALUES ROUND ( RAND ( ) * 3 , 0 ) + 1 INTO : H  	12736	1
-- SELECT MYID INTO : H FROM MYDUMMY WHERE MYCOL = : H FETCH FIRST 1 ROW ONLY  	13344	0

SELECT program_used_size, number_statements, isolation, default_schema, naming 
FROM QSYS2.SYSPROGRAMSTAT 
WHERE system_program_schema = 'SPCLOCLCK' AND program_name = 'INVISILOCK';
-- 43376

-- Review access plan rebuild stats from monitor.   (http://www-01.ibm.com/support/docview.wss?uid=nas8N1010982)
SELECT QQRCOD rebuild_rsn, count(*) cnt
FROM dbmon
where qqrid = 3006    
GROUP BY QQRCOD
ORDER BY cnt desc;
-- Example data:
-- AB	6
-- A1	2
-- A4	2
-- B6	2
-- AF	1


SELECT qvc24 save_reason, count(*) cnt
FROM dbmon
where qqrid = 1000 and substr(qvc24, 1, 1) = 'A'                                                          
group by qvc24                                       
order by cnt desc;
-- Example data:
-- AB	31


-- Compressing the program would fail at this point with CPF9898/SQL0913 since we called & locked it above. 
--   So reconnect to DB a couple of times to let go.
disconnect mydb;
connect to mydb;
 
CL: DLYJOB 3;

disconnect mydb;
connect to mydb;

CL: CALL QSYS/QSQCMPGM PARM('SPCLOCLCK' 'INVISILOCK' '*PGM');               
-- MODULE: INVISILOCK                     SIZE BEFORE COMPRESSION: 00043376   
--   BYTES. SIZE AFTER COMPRESSION:  00004864 BYTES.                          
