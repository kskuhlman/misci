-- Roll-our-own inactive timeout:  
--  Using CSI data, find jobs that started X hours ago, haven’t ended, 
--   and have used less than Y ms of CPU time in the last Z hours? 

call dbmon.set_csi_collection();

SELECT row_number() over (order by sum(jbtcpu)) SEQ,               
 JBNAME, JBUSER, JBNBR, sum(JBTCPU) CPU,                           
 case when max(JBSTSF) = 0 then 'collected'                        
      when max(JBSTSF) = 1 then 'started'                          
      when max(JBSTSF) = 2 then 'ended'                            
      when max(JBSTSF) = 3 then 'start & end' end sts,             
 min(dtetim) tim1, max(dtetim) tim2                                
FROM qtemp.qapmjobmi
WHERE jbtype = 'I'
GROUP BY JBNAME, JBUSER, JBNBR                                     
having max(JBSTSF) <= 1   /* has not ended */
  and sum(JBTCPU) < 100   /* has used less then 100 ms of CPU today */
  /* started more than an hour ago */  
  and min(dtetim) < to_char(current_timestamp - 1 hour, 'YYMMDDHH24MISS')   
order by cpu desc;
