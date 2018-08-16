-- Semi-Automated version of the "SQL General" MustGather document:
-- www-01.ibm.com/support/docview.wss?uid=nas8N1012188
-- Script designed to work with jobs that respawn when killed but is easily adapted to other scenarios.
-- "IBM i" is aka Db2/i aka iSeries aka i5 aka as/400 aka as500 aka silverlake.

-- Overview of steps.
-- Collection steps for a batch job
-- 1.	Issue CRTLIB QIBMDATA
-- 2.	Submit the job with HOLD(*YES) on the SBMJOB command.
-- 3.	Find the job; for example, use the WRKSBMJOB command.
-- 4.	Note the full job name (number/user/name). It will be used in the following steps.
-- 5.	Issue STRSRVJOB JOB(NUMBER/USER/NAME) (Note: Replace the job with the job that was submitted.)
-- 6.	Issue STRDBG UPDPROD(*YES)
-- 7.	Issue RLSJOB JOB(NUMBER/USER/NAME) (Note: Replace the job with the job that was submitted.)
-- 8.	Press F10 for a command line.
-- 9.	Issue CHGJOB JOB(NUMBER/USER/NAME) LOG(4 00 *SECLVL) LOGCLPGM(*YES) (Note: Replace the job with the job that was submitted.)
-- 10.	Issue STRDBMON OUTFILE(QIBMDATA/DBMON1) JOB(NUMBER/USER/JOBNAMANE) TYPE(*DETAIL) (Note: Replace the job with the job that was submitted.)
-- 11.	Issue STRTRC SSNID(TRACE1) JOB(NUMBER/USER/NAME) MAXSTG(4000000) (Note: Set the MAXSTG as large as you can and replace the job with the job that was submitted.)
-- 12.	Press F3 to Exit.
-- 13.	Press the Enter key to let the job run.
-- 14.	ENDDBMON JOB(NUMBER/USER/JOBNAMANE) (Note: Replace the job with the job that was submitted.)
-- 15.	ENDTRC SSNID(TRACE1) DTALIB(QIBMDATA) PRTTRC(*YES)

-- Then send the info to IBM.

set current schema mylib; 
set path mylib;
create or replace variable gv_jobid   varchar(28) default null;
create or replace variable gv_PMRlib  varchar(10) default null; 
create or replace variable gv_jobname varchar(10) default null;
create or replace variable gv_jobuser varchar(10) default null;

set gv_jobname = 'THEBADJOB'; 
set gv_jobuser = 'BADJOBUSR'; 
set gv_PMRlib = 'PMR14092';

-- 1 Collection steps for a batch job
--CRTLIB QIBMDATA
begin
  declare continue handler for sqlstate '38501' begin end;  
  declare continue handler for sqlstate '42704' begin end;  
    
  call qsys2.qcmdexc('CRTLIB '||gv_PMRlib);
  execute immediate('drop table '||gv_PMRlib||'.DBMON1');
end;

-- 2 Hold jobq & submit job here.
--CL: HLDJOBQ SERVERJOBQ; 

-- 3 Find the job; for example, use the WRKSBMJOB command.
-- 4 Note the full job name (number/user/name). It will be used in the following steps.
set gv_jobid = (select job_name FROM TABLE(QSYS2.JOB_INFO(JOB_STATUS_FILTER => '*ACTIVE' , JOB_TYPE_FILTER => '*BATCH', JOB_USER_FILTER => gv_jobuser, JOB_SUBSYSTEM_FILTER => 'SERVERSBS')) a where job_name like '%'||gv_jobname||'%');
values gv_jobid;
stop;  -- double check that we want to kill this job before resuming.
call qsys2.qcmdexc('ENDJOB '||gv_jobid);
stop;  -- wait for it to die.

-- wait for new one to respawn.
set gv_jobid = null;
select job_name FROM TABLE(QSYS2.JOB_INFO(JOB_STATUS_FILTER => '*JOBQ'  , JOB_TYPE_FILTER => '*BATCH', JOB_USER_FILTER => gv_jobuser)) a where job_name like '%'||gv_jobname||'%';
begin
  while (gv_jobid is null) do
   set gv_jobid = (select job_name FROM TABLE(QSYS2.JOB_INFO(JOB_STATUS_FILTER => '*JOBQ' , JOB_TYPE_FILTER => '*BATCH', JOB_USER_FILTER => gv_jobuser, JOB_SUBSYSTEM_FILTER => 'SERVERSBS')) a where job_name like '%'||gv_jobname||'%'); 
   call qsys2.qcmdexc('dlyjob 2');
  end while;
end;
values gv_jobid;

-- 5  Issue STRSRVJOB JOB(NUMBER/USER/NAME) (Note: Replace the job with the job that was submitted.)
call qsys2.qcmdexc('STRSRVJOB JOB('||gv_jobid||')');
-- 6.	Issue STRDBG UPDPROD(*YES)
CL: STRDBG UPDPROD(*YES);
-- 7.	Issue RLSJOB JOB(NUMBER/USER/NAME) (Note: Replace the job with the job that was submitted.)  8.	Press F10 for a command line.
CL: RLSJOBQ SERVERJOBQ;
-- 9.	Issue CHGJOB JOB(NUMBER/USER/NAME) LOG(4 00 *SECLVL) LOGCLPGM(*YES) (Note: Replace the job with the job that was submitted.)
call qsys2.qcmdexc('CHGJOB JOB('||gv_jobid||') LOG(4 00 *SECLVL) LOGCLPGM(*YES)');
-- 10.	Issue STRDBMON OUTFILE(QIBMDATA/DBMON1) JOB(NUMBER/USER/JOBNAMANE) TYPE(*DETAIL) (Note: Replace the job with the job that was submitted.)
call qsys2.qcmdexc('STRDBMON OUTFILE('||gv_PMRlib||'/DBMON1) JOB('||gv_jobid||') TYPE(*DETAIL)');
-- 11.	Issue STRTRC SSNID(TRACE1) JOB(NUMBER/USER/NAME) MAXSTG(4000000) (Note: Set the MAXSTG as large as you can and replace the job with the job that was submitted.) --12.	Press F3 to Exit. 13.	Press the Enter key to let the job run.
call qsys2.qcmdexc('STRTRC SSNID(TRACE1) JOB('||gv_jobid||') MAXSTG(4000000)');

-- Wait for error to occur..
stop; 


-- 14.	ENDDBMON JOB(NUMBER/USER/JOBNAMANE) (Note: Replace the job with the job that was submitted.)
call qsys2.qcmdexc('ENDDBMON JOB('||gv_jobid||')');
-- 15.	QIBMDATA) PRTTRC(*YES)
call qsys2.qcmdexc('ENDTRC SSNID(TRACE1) DTALIB('||gv_PMRlib||')');
-- Added.  End debug.
CL: ENDDBG;
-- Added.  Stop servicing job.
CL: ENDSRVJOB;
