CREATE OR REPLACE FUNCTION show_privelege_role(priflag text,nspacl aclitem[])
       RETURNS text AS
$$
DECLARE
  i int;
  aclstr text;
  pristr text;
  username text;
  resultstr text;
BEGIN
  if array_lower(nspacl,1) is null then
    return '';
  end if; 
  
  username := '';
  pristr := '='||priflag||E'\/';
  for i in array_lower(nspacl,1) .. array_upper(nspacl,1)
  loop
    aclstr := nspacl[i];
    if aclstr ~ pristr then 
      if substr(aclstr,1,1)='=' then 
        --public;
        username := username||'public;';
      else 
        --granted role;
        username := username||substr(aclstr,1,position('=' in aclstr)-1)||';';
      end if;
    end if;
  end loop;
  if trim(username)!='' then
    resultstr := substr(username,1,length(username)-1);
  end if;
  
  return resultstr;

END;
$$
LANGUAGE plpgsql volatile;




----------------------------------------------------------------

CREATE OR REPLACE FUNCTION show_acl_role(aclstr aclitem)
       RETURNS text AS
$$
DECLARE
  invar text;
  username text;
BEGIN
  if aclstr is null then
    return '';
  end if; 
  invar := aclstr;
  username := split_part(trim(invar),'=',1);
  if trim(username)='' then 
    username := 'public';
  end if;

  return username;
END;
$$
LANGUAGE plpgsql volatile;

CREATE OR REPLACE FUNCTION show_acl_schema(aclstr aclitem)
       RETURNS text AS
$$
DECLARE
  i int;
  invar text;
  pristr text;
  resultstr text;
BEGIN
  if aclstr is null then
    return '';
  end if; 
  invar := aclstr;
  pristr := split_part(split_part(trim(invar),'=',2),E'\/',1);
  --raise info '%',pristr;
  resultstr := '';
  if pristr ~ 'U' then
    resultstr := resultstr||'Usage;';
  end if;
  if pristr ~ 'C' then
    resultstr := resultstr||'Create;';
  end if;
  
  return resultstr;
END;
$$
LANGUAGE plpgsql volatile;

CREATE OR REPLACE FUNCTION show_acl_table(aclstr aclitem)
       RETURNS text AS
$$
DECLARE
  i int;
  invar text;
  pristr text;
  resultstr text;
BEGIN
  if aclstr is null then
    return '';
  end if; 
  invar := aclstr;
  pristr := split_part(split_part(trim(invar),'=',2),E'\/',1);
  resultstr := '';
  if pristr ~ 'r' then
    resultstr := resultstr||'Select;';
  end if;
  if pristr ~ 'a' then
    resultstr := resultstr||'Insert;';
  end if;
  if pristr ~ 'w' then
    resultstr := resultstr||'Update;';
  end if;
  if pristr ~ 'd' then
    resultstr := resultstr||'Delete;';
  end if;
  if pristr ~ 'D' then
    resultstr := resultstr||'Truncate;';
  end if;
  
  return resultstr;
END;
$$
LANGUAGE plpgsql volatile;


------------------------------------------------------------
--schema
select nspname schemaname
,case when show_acl_role(nspaclstr)='' then rolname else show_acl_role(nspaclstr) end as usename
,show_acl_schema(nspaclstr) schema_privillege 
from
(select nspname,rolname,null as nspaclstr
from pg_namespace a,pg_roles b where a.nspowner=b.oid and a.nspname not like 'pg%' and nspacl is null
union all
select nspname,rolname,unnest(nspacl) as nspaclstr
from pg_namespace a,pg_roles b where a.nspowner=b.oid and a.nspname not like 'pg%' and nspacl is not null
) foo;







