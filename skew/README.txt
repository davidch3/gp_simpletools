Installation: 
For GP4.3 and GP5:
psql dbname -af ./skew/skewcheck_func.sql
For GP6:
psql dbname -af ./skew/skewcheck_func_gp6.sql
For GP7:
psql dbname -af ./skew/skewcheck_func_gp7.sql

Usage: Check table skew in each schema. example: select * from skewcheck_func('public');

Output column:
tablename: Skew table name.
sys_segcount: Total segment instances in GP cluster.
data_segcount: The number of segment instances have data in this table.
maxsize_segid: The max size segmentID in this table.
maxsize: Size of segmentID above.
skew: skew rate, (max - avg)/avg. 
dk: Distribution key of table, if null is randomly.

Even if skew=0, but data_segcount<sys_segcount, This table is skew.

