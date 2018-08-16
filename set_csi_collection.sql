-- Create aliases in QTEMP for certain CSI collection members                    
--  active at an input time.  If no input, assume current.                       
                                                                                 
set current schema dbmon;                                                                                
set path=dbmon;                                                                                

CREATE OR REPLACE PROCEDURE set_csi_collection (start_time timestamp)            
    specific setcsicol                                                           
    MODIFIES SQL DATA                                                            
    SET OPTION COMMIT = *NONE, DBGVIEW = *SOURCE                                 
                                                                                 
P1: BEGIN ATOMIC                                                                 
                                                                                 
    -- Declare Variables                                                         
    Declare tbl Char(10);                                                        
    Declare member Char(10);                                                     
    Declare sqlString Varchar(5000);                                             
    Declare SQLCODE integer default 0;                                           
    Declare found char(1) default '0';                                           
                                                                        
  -- Determine day's collection services investigator (CSI) members     
declare COLLECTIONMEMBERS cursor for                                    
WITH ACTIVE_COLLECTIONS AS (SELECT * FROM                               
  (SELECT CCCNNAME COLLECTION_ID,                                       
   /* CONVERT UTC START & END TIMES TO LOCAL TIME */                    
   CCSTRDT + (CURRENT_TIMEZONE / 10000) HOURS START_TIME_LOCAL,         
   CASE WHEN CCENDDT = TIMESTAMP('0001-01-01')                          
     THEN TIMESTAMP('9999-12-31')                                       
     ELSE CCENDDT + (CURRENT_TIMEZONE / 10000) HOURS END END_TIME_LOCAL 
  FROM QUSRSYS.QAPMCCCNTB WHERE CCCLRID = '*CS') A                      
  WHERE START_TIME_LOCAL < START_TIME                                   
    AND (END_TIME_LOCAL >= START_TIME))                                 
SELECT SYSTEM_TABLE_NAME TBL, MAX(SYSTEM_TABLE_MEMBER) MBR              
FROM QSYS2.SYSPARTITIONSTAT A                                           
JOIN ACTIVE_COLLECTIONS B                                               
  ON A.SYSTEM_TABLE_MEMBER = B.COLLECTION_ID                            
WHERE SYSTEM_TABLE_SCHEMA = 'QMPGDATA'                                  
  AND SYSTEM_TABLE_NAME IN ('QAPMSMCMN', 'QAPMSMDSK', 'QAPMSMJMI',      
    'QAPMSMJOS', 'QAPMSMPOL', 'QAPMSMSYS','QAPMJOBOS','QAPMJOBMI')      
GROUP BY SYSTEM_TABLE_NAME;                                             
                                                                        
  open collectionMembers;                                               
  fetch collectionMembers into tbl, member;                             
  
                                                              
  -- loop through collection list                             
  while SQLCODE >= 0 and SQLCODE <> 100 do                    
    set sqlString = 'create or replace alias qtemp.'||tbl||   
      ' for QMPGDATA.'||tbl||'('||member||')';                
    execute Immediate sqlString;                              
                                                              
    fetch collectionMembers into tbl, member;                 
   end while;                                                 
   close collectionMembers;                                   
END P1                                                        
;                                                             
                                                              
CREATE OR REPLACE PROCEDURE set_csi_collection ()             
  specific setcsicol1                                         
  SET OPTION COMMIT = *NONE, DBGVIEW = *SOURCE                
                                                              
P1: BEGIN ATOMIC                                              
  call set_csi_collection(current_timestamp);                 
END P1                                                        
;                                                             
