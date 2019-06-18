gp_kill_idle.sh:

Script use for kill idle session and idle transaction session. Using function pg_terminate_backend.
Sample: sh gp_kill_idle.sh.
Script can setup in crontab, run for each hour.
If you want to change the time last, you can change this two lines in script:
INT_IDLE_TRAN=1
INT_IDLE_CONN=24
