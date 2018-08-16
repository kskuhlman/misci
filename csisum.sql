call dbmon.set_csi_collection();

drop table csisum; 
create table csisum as (
select mi.intnum, mi.datetime, sys.smscpucpu CPU_pct, sys.smscpuqpu CPU_sql, mi.smmtcpup cpu_pct_inter, 
  mi2.smmtcpup cpu_pct_batch, decimal(round(SMOARSPT / 1000.0,2),5,2) IRT, 
  decimal(round(SMOMRSPT / 1000.0,2),7,2) max_irt, mi.smmnum job_count_inter, mi2.smmnum job_count_batch,  
  os.smoitrnr trans_rate_inter, os.smoblior logical_io_batch,  pol.smpmfltr fault_rate_machine_pool, 
  pol.smpuafltr fault_rate_user_pool, pol.smpumfltr fault_rate_max, pol.smpumfltp fault_rate_max_pool,
  sys.smsctstgu / 1024 temp_storage_gb, os.smosplfr spoolf_rate,
  mi.smmmcnbr ||'/'||trim(mi.smmmcuser)||'/'||mi.smmmcname max_CPU_inter, 
  mi2.smmmcnbr ||'/'||trim(mi2.smmmcuser)||'/'||mi2.smmmcname max_CPU_batch, 
  smomrnbr||'/'||trim(smomruser)||'/'||smomrname max_irt_job
from qtemp.QAPMSMJMI mi
join qtemp.QAPMSMJMI mi2
  on mi.intnum = mi2.intnum and mi.smmbdd = 1 
     and mi2.smmbdd = 2 /* breakdown dimension 1=inter, 2=batch */
join qtemp.QAPMSMJOS os
  on mi.intnum = os.intnum
join qtemp.QAPMSMPOL pol
  on mi.intnum = pol.intnum
join qtemp.QAPMSMSYS sys
  on mi.intnum = sys.intnum
order by mi.intnum asc
) with data
;

select * from csisum where hour(datetime) between 11 and 16 order by 1; 

select irt, count(*) intervals_with_irt, 
  hour(min(datetime))||':'||right(digits(minute(min(datetime))),2) first_occurance, 
  hour(max(datetime))||':'||right(digits(minute(max(datetime))),2) last_occurance,
  decimal(round(avg(cpu_pct),2),5,2) cpu_pct, decimal(round(avg(cpu_sql),2),5,2) cpu_sql, 
  decimal(round(avg(cpu_pct_inter),2),5,2) cpu_pct_inter, 
  decimal(round(avg(cpu_pct_batch),2),7,2) cpu_pct_batch, decimal(avg(max_irt),9,1) max_irt, 
  avg(job_count_inter) job_count_inter, avg(job_count_batch) job_count_batch,
  decimal(round(avg(trans_rate_inter),2),7,2) trans_rate_inter, 
  decimal(round(avg(logical_io_batch),0),9,0) logical_io_batch, 
  avg(fault_rate_machine_pool) fault_rate_machine_pool, 
  avg(fault_rate_user_pool) fault_rate_user_pool, 
  avg(fault_rate_max) fault_rate_max, avg(temp_storage_gb) temp_storage_gb, 
  decimal(round(avg(spoolf_rate),2),7,2) spoolf_rate
from csisum c
where hour(datetime) between 11 and 16
group by irt
order by 1;
