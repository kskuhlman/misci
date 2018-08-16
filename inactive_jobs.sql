-- Roll-our-own inactive timeout:  
-- IBM's inactivity timer has gaps, including when the jobs are watching a queue.
--  This CPU check is simular to the pre-6.1 method IBM used. 
--
--  Using CSI data, find jobs that started X hours ago, haven’t ended, 
--   and have used less than Y ms of CPU time in the last Z hours? 


call dbmon.set_csi_collection();

with job_info as (
SELECT JBNAME job_name, JBUSER job_user, JBNBR job_number, 
 case when max(JBSTSF) = 0 then 'rollover'
      when max(JBSTSF) = 1 then 'started'                          
      when max(JBSTSF) = 2 then 'ended'                            
      when max(JBSTSF) = 3 then 'start & end' end sts,             
 time(to_date(min(dtetim),'YYMMDDHH24MISS')) str_time, 
 time(to_date(max(dtetim),'YYMMDDHH24MISS')) last_time,
 /* carry-over from yesterday? */
 case when substr(min(dtetim),7,3) = '000' then 'Y' else 'N' end yesterday,
 /* started more than X hours ago? */  
 case when min(dtetim) < to_char(current_timestamp - 2 hour, 'YYMMDDHH24MISS') then 'Y' else 'N' end been_around,
 /* last_activity of any sort at least XX minutes ago? */
 case when max(to_date(dtetim,'YYMMDDHH24MISS')) < current_timestamp - 60 minutes then 'Y' else 'N' end totally_idle,
 sum(JBTCPU) total_CPU,
 /* CPU use in ms in the last X hours */ 
 sum(case when dtetim >= to_char(current_timestamp - 2 hour, 'YYMMDDHH24MISS') then JBTCPU else 0 end) recent_cpu
FROM qtemp.qapmjobmi                                               
WHERE jbtype = 'I' 
GROUP BY JBNAME, JBUSER, JBNBR                                     
) 

select
  /* don't kill more then 1,000 jobs at a time */
  case when seq < 1000 then decimal(0,6,0) else 9999999 end end_sqlcode, 
  a.* from (
select
  current_timestamp retrieval_time,  
  job_number||'/'||trim(job_user)||'/'||trim(job_name) jobid,
  row_number() over (order by last_time) SEQ, 
    a.*  
from job_info a
where
  been_around = 'Y' and recent_cpu < 20 and sts in ('rollover', 'started')
  and totally_idle = 'Y'
order by SEQ
) a ;
