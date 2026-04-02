------------GP43/GP5
--DROP EXTERNAL TABLE check_process_ext;
--CREATE EXTERNAL WEB TABLE check_process_ext
--(
--  hostname text,
--  port text,
--  username text,
--  dbname text,
--  masterip text,
--  sessionid text,
--  segid text,
--  slicex text
--)
--EXECUTE E'export HOSTN=`hostname`;ps -ef |grep postgres |grep con |grep -v grep |grep -v primary|grep -v mirror|grep -v master|awk ''{print $9,$10,$11,$12,$13,$14,$16}'' |sed  ''1,\$ s/^/''\$\HOSTN'' /g''' ON HOST
--FORMAT 'TEXT' (DELIMITER ' ');
----------GP6
DROP EXTERNAL TABLE check_process_ext;
CREATE EXTERNAL WEB TABLE check_process_ext
(
  hostname text,
  port text,
  username text,
  dbname text,
  masterip text,
  sessionid text,
  segid text,
  slicex text
)
EXECUTE E'export HOSTN=`hostname`;ps -ef |grep postgres |grep con |grep -v grep |grep -v primary|grep -v mirror|awk ''{print $9,$10,$11,$12,$13,$14,$16}'' |sed  ''1,\$ s/^/''\$\HOSTN'' /g''' ON HOST
FORMAT 'TEXT' (DELIMITER ' ');




select segid,sessionid,count(*) from check_process_ext group by 1,2;

select segid,sessionid,sess_proc_count from 
(
  select segid,sessionid,sess_proc_count,
  ROW_NUMBER() OVER ( PARTITION BY sessionid ORDER BY sess_proc_count desc) rn1
  from (
    select segid,sessionid,count(*) sess_proc_count from check_process_ext group by 1,2
  ) t1
) t2 where rn1=1 --and sess_proc_count>150
order by sess_proc_count desc;





