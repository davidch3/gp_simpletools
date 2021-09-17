Preparation:
Check if plpythonu is created: select * from pg_language;
If not, please create language plpythonu: create language plpythonu;
 
Installation: 
For GP4.3 and GP5:
psql dbname -af ./gpsize/load_files_size.sql
For GP6:
psql dbname -af ./gpsize/load_files_size_v6.sql

Information:
This component is used to query all data files on all segment instances. Query result load into table public.gp_seg_size.
Base on public.gp_seg_size, we can calculate schema size, table size, tablespace size, tablespace file numbers.
But not recommend to calculate database size, because of public.gp_seg_size is not include data file size on master instance.

Usage:
1, Prepare gpsize data, insert into gp_seg_table_size
truncate gp_seg_size_ora;
truncate gp_seg_table_size;
select gp_segment_id,public.load_files_size() from gp_dist_random('gp_id');

2, Use SQL in gpsize.sql / gpsize_v6.sql, Calculate schema size, tablespace size, large table in DB ...





