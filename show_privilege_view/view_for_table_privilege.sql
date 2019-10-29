create or replace view view_for_table_privilege as
select schemaname,
tablename,
tabletype,
reltablespace,
case when show_acl_role(relaclstr)='' then rolname else show_acl_role(relaclstr) end as rolname,
case when show_acl_role(relaclstr)=rolname then 'TRUE' when show_acl_role(relaclstr)='' then 'TRUE' else 'FALSE' end as isowner,
case when show_acl_table(relaclstr)='' then 'Select;Insert;Update;Delete;Truncate;' else show_acl_table(relaclstr) end as privelege
from (
select nspname as schemaname
,relname as tablename
,rol.rolname
,coalesce(tbs.spcname,'pg_default') as reltablespace
,case when relkind='r' then 'Table' else 'View' end as tabletype
,null as relaclstr
from pg_class rel inner join pg_namespace nsp on nsp.oid=rel.relnamespace 
inner join pg_authid rol on rel.relowner=rol.oid 
left join pg_tablespace tbs on rel.reltablespace=tbs.oid
where rel.relkind in ('r','v','x') and rel.relname not like '%_1_prt_%' and rel.relacl is null
union all
select nspname as schemaname
,relname as tablename
,rol.rolname
,coalesce(tbs.spcname,'pg_default') as reltablespace
,case when relkind='r' then 'Table' else 'View' end as tabletype
,unnest(rel.relacl) as relaclstr
from pg_class rel inner join pg_namespace nsp on nsp.oid=rel.relnamespace 
inner join pg_authid rol on rel.relowner=rol.oid 
left join pg_tablespace tbs on rel.reltablespace=tbs.oid
where rel.relkind in ('r','v','x') and rel.relname not like '%_1_prt_%' and rel.relacl is not null
) as pri;

