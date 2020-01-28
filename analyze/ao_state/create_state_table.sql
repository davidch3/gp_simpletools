create table check_ao_state (
 reloid bigint,
 schemaname text,
 tablename text,
 modcount bigint,
 last_checktime timestamp without time zone
) distributed by (reloid);

