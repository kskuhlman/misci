
-- Automated DBE tasks for Navigator - Documents the plan cache stored procedures.
-- https://www.ibm.com/developerworks/community/wikis/home?lang=en#!/wiki/IBM%20i%20Technology%20Updates/page/Automated%20DBE%20tasks%20for%20Navigator

set current schema dbmon;

-- A list of libraries that are constant between systems (and therefore don't need to be stripped of their schema references when qualified.
create table dbmon.staticlibs (library char(10) primary key); 

include 'plan_cache_analysis_data.sql';


-- Parse the entries in the plan cache dump to get the schema / table / column references.
drop table qtemp.parsed;
create table qtemp.parsed as (
WITH
plan_statements (naming_mode, dec_point, string_delim, statement_number, statement_text) AS (
  select '*SYS' naming_mode, '*PERIOD' dec_point, '*APOSTSQL' string_delim, statement_number, statement_text_long
  from dbmon.m170822_QQQ1000 s  -- m170718_QQQ1000
  ),
parsed_statements (seq, name_type, name, schema, column_name, name_start_position, statement_number, statement_text) AS (
  SELECT row_number() over(partition by statement_number order by name_start_position) seq, name_type, c.name, c.schema, c.column_name, c.name_start_position, statement_number,
     statement_text
  FROM plan_statements, TABLE(qsys2.parse_statement(statement_text, naming_mode, dec_point, string_delim)) c
  WHERE c.name_type = 'TABLE'
  )
select *  from parsed_statements
ORDER BY statement_number, seq
) with data;

create index qtemp.parsed01 on qtemp.parsed (statement_number, schema);
create index qtemp.parsed02 on qtemp.parsed (schema, statement_number);
create index qtemp.parsed03 on qtemp.parsed (statement_number, seq);

-- Remove schema references from statements.
/* only remove library references from statements when the library is qualified & not one that's constant, like QSYS or SCRUBLIB */
drop table qtemp.cleanplans;
create table qtemp.cleanplans as (
with 
parsed_statements as (
  select * from qtemp.parsed
  where statement_number in (
  select statement_number from qtemp.parsed 
  where schema is not null and schema not in (select library from dbmon.staticlibs) 
  group by statement_number)),  

stripped_statements (seq, name_type, name, schema, column_name, name_start_position, statement_number, statement_text, original_statement) as (
  /* This is a recursive statement, to strip off the first & then subsequent library names from a given statement */
  select seq, name_type, name, schema, column_name, name_start_position, statement_number,
    case when schema is null then a.statement_text else regexp_replace(a.statement_text, a.schema||' *[.|/] *','',1,1,'i') end statement_text, 
    a.statement_text original_statement
  from parsed_statements a where seq = 1
  union all
  select b.seq, b.name_type, b.name, b.schema, b.column_name, b.name_start_position, b.statement_number,
    regexp_replace(a.statement_text, b.schema||' *[.|/] *','',1,1,'i') statement_text,
    b.statement_text original_statement
  from stripped_statements a, parsed_statements b
  where b.schema is not null and a.statement_number = b.statement_number and b.seq = a.seq + 1 
  ),
final_statement (statement_number, statement_text_parsed, original_statement) as (
  select statement_number, statement_text, original_statement from stripped_statements a
  where (a.statement_number, a.seq) in (select statement_number, max(b.seq) from stripped_statements b group by statement_number)) 

select * from final_statement
) with data;

select count(*), count(distinct statement_number) from dbmon.m170822_QQQ1000;   -- 32745	32745
select count(*), count(distinct statement_number) from qtemp.cleanplans; -- 68538	32745


--------------------------------------
---- Audit ---------------------------
--------------------------------------
-- Check of parsed dump
-- Grab the statements that don't need their schemas stripped off.
select count(*), count(distinct statement_number) from qtemp.parsed where statement_number in (
  select statement_number from qtemp.parsed
  where schema is null or schema in (select library from dbmon.staticlibs)
  group by statement_number
);  -- 67852	32441

-- And now the ones that need stripping.
select count(*), count(distinct statement_number) from qtemp.parsed
where statement_number in (
  select statement_number from qtemp.parsed
  where schema is not null and schema not in (select library from dbmon.staticlibs)
  group by statement_number
); -- 788	328


-- Test cases for the regular expression expression we used to strip schemas off of statements.
--   Should just return one row: 'INSERT INTO BAR':
select upper(regexp_replace(a.statement_text, schema||' *[.|/] *','',1,1,'i')) from (select 'Insert into FOO/BAR' statement_text, 'FOO' schema from sysibm.sysdummy1) a union
select upper(regexp_replace(a.statement_text, schema||' *[.|/] *','',1,1,'i')) from (select 'Insert into FOO.BAR' statement_text, 'foo' schema from sysibm.sysdummy1) a union
select upper(regexp_replace(a.statement_text, schema||' *[.|/] *','',1,1,'i')) from (select 'INSERT INTO FOO / BAR' statement_text, 'FOO' schema from sysibm.sysdummy1) a union
select upper(regexp_replace(a.statement_text, schema||' *[.|/] *','',1,1,'i')) from (select 'INSERT INTO FOO . BAR' statement_text, 'foo' schema from sysibm.sysdummy1) a;

select count(distinct statement_text_parsed) distinct_statements, count(*) 
from qtemp.cleanplans;

create table qtemp.statement_xref as (
select b.statement_number, a.statement_number related_statement_number from qtemp.cleanplans a 
join
  (select statement_text_parsed, max(statement_number) statement_number
   from qtemp.cleanplans b 
   group by statement_text_parsed
  ) b
on a.statement_text_parsed = b.statement_text_parsed
) with data;

select count(*), count(distinct statement_number) from qtemp.statement_xref; 
