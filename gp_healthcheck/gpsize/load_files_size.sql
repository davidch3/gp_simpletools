drop table if exists public.gp_seg_size_ora cascade;
create table public.gp_seg_size_ora
(
  acl text
 ,num int
 ,sysuser text
 ,sysgroup text
 ,size numeric
 ,modtime timestamp
 ,filename text
)distributed randomly;

drop table if exists public.gp_seg_table_size cascade;
create table public.gp_seg_table_size
(
  hostname text
 ,oid           oid
 ,relnamespace  oid
 ,relname       name
 ,reltablespace oid
 ,relfilenode   oid
 ,size          numeric
 ,relfilecount  int
 ,max_modtime timestamp
) distributed randomly;


CREATE LANGUAGE plpythonu;


DROP FUNCTION IF EXISTS public.hostname();
CREATE or replace FUNCTION public.hostname() RETURNS
text
as $$
import socket
return socket.gethostname()
$$ LANGUAGE plpythonu;



DROP FUNCTION IF EXISTS public.load_files_size();
CREATE or REPLACE FUNCTION public.load_files_size() RETURNS text
as $$
import subprocess

rows = plpy.execute("select current_database() dbname, current_setting('port') portno;")
(dbname, portno) = (rows[0]["dbname"], rows[0]["portno"])
rows = plpy.execute("select '/tmp/fs_'||current_setting('gp_dbid')||'.dat' as filename;")
filename = rows[0]["filename"]

def run_psql_utility(v_sql):
    psql_cmd = """PGOPTIONS='-c gp_session_role=utility' psql -v ON_ERROR_STOP=1 -d %s -p %s -A -X -t -c \"%s\" """ % (dbname, portno, v_sql)
    #plpy.info(psql_cmd)
    p = subprocess.Popen(psql_cmd,shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE)
    p_stdout = p.stdout.read()
    p_stderr = p.stderr.read()
    p.wait()
    if p.returncode != 0: 
        plpy.error(p_stderr)

rows = plpy.execute("select 'export LANG=en_US.utf8;ls -l --time-style=''+%Y-%m-%d_%H:%M:%S'' '||current_setting('data_directory')||'/base/'||c.oid||'> /tmp/fs_'||current_setting('gp_dbid')||'.dat ; ' as cmd1 from pg_database c where c.datname=current_database();")
cmd1 = rows[0]["cmd1"]
rows = plpy.execute("select string_agg('ls -l --time-style=''+%Y-%m-%d_%H:%M:%S'' ' ||case when current_setting('gp_dbid')::int=c.db_id_1 then trim(c.location_1) when current_setting('gp_dbid')::int=c.db_id_2 then trim(c.location_2) end ||'/*/'||(SELECT oid from pg_database where datname=current_database())||' >> /tmp/fs_'||current_setting('gp_dbid')||'.dat',' ; ') as cmd2 from gp_persistent_filespace_node c;")
cmd2 = rows[0]["cmd2"]
if cmd2 is None:
    ls_cmd = cmd1
else:
    ls_cmd = cmd1+cmd2
#plpy.info(ls_cmd)
p = subprocess.Popen(ls_cmd,shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE)
p_stdout = p.stdout.read()
p_stderr = p.stderr.read()
p.wait()
if p.returncode != 0: 
    plpy.notice(p_stderr)

sed_cmd = "sed -i 's/[ ]\{1,\}/|/g;/?/d;/^total/d;/^\//d;/^$/d' "+filename
#plpy.info(sed_cmd)
p = subprocess.Popen(sed_cmd,shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE)
p_stdout = p.stdout.read()
p_stderr = p.stderr.read()
p.wait()
if p.returncode != 0: 
    plpy.error(p_stderr)

copy_sql = "copy public.gp_seg_size_ora from '"+filename+"' delimiter '|';"
run_psql_utility(copy_sql)

insert_sql = "insert into public.gp_seg_table_size (select hostname(),a.oid,a.relnamespace,a.relname,a.reltablespace,a.relfilenode,b.size,b.relfilecount,b.max_modtime from pg_class a join (select split_part(filename,'.',1) as relfilenode,sum(size) size,count(*) relfilecount,max(modtime) max_modtime from gp_seg_size_ora group by 1) b on a.relfilenode::text=b.relfilenode);"
run_psql_utility(insert_sql)

return "OK"
$$ LANGUAGE plpythonu;


--truncate gp_seg_size_ora;
--truncate gp_seg_table_size;
--select gp_segment_id,public.load_files_size() from gp_dist_random('gp_id');


