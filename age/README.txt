INFORMATION:
Find out age>300,000,000, use vacuum freeze.

vacuum_high_age_otherdb.sh:
For database: gpperfmon, postgres, template1, template0

vacuum_high_age.pl:
Use for GP4.3.x.x
Find out age>300,000,000 (Limit 8000), use vacuum freeze. Program have 3 parallel jobs.
Usage:
perl vacuum_high_age.pl dbname duration(hours) log_dir
Program will exit when run time larger than duration(hours)
Example:
perl vacuum_high_age.pl testdb 2 /home/gpadmin/gpAdminLogs

vacuum_high_age_5.pl:
Use for GP5.x.x
Find out age>300,000,000 (Limit 9000), use vacuum freeze. Program have 3 parallel jobs.
Usage:
perl vacuum_high_age_5.pl dbname duration(hours) log_dir
Program will exit when run time larger than duration(hours)
Example:
perl vacuum_high_age_5.pl testdb 2 /home/gpadmin/gpAdminLogs


