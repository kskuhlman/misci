-- Summarize SQL0901 errors by application.
SELECT date(failtime) faildate, FAILRSN, DBGROUP, APPLIB, APPNAME, APPTYPE, count(*) cnt,
    min(jobname) first_job, max(JOBNAME) last_job, count(distinct jobname) nbr_jobs
FROM qrecovery.QSQ901S
WHERE failtime > current_timestamp - 1 months
GROUP BY date(failtime), FAILRSN, DBGROUP, APPLIB, APPNAME, APPTYPE 
ORDER BY 1 desc;

-- Summarize -901 errors by error message.
SELECT date(failtime) faildate, FAILRSN, DBGROUP, APPLIB, /* APPNAME, */ APPTYPE, count(*) cnt, min(jobname) first_job, max( JOBNAME) last_job, count(distinct jobname) nbr_jobs, left(msgs, 200) msgs
FROM qrecovery.QSQ901S
WHERE failtime > current_timestamp - 14 days
GROUP BY date(failtime), FAILRSN, DBGROUP, APPLIB, APPNAME, APPTYPE, left(msgs, 200)
ORDER BY 1 desc;
