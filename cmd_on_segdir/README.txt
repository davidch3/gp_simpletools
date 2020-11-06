Usage: Run command and shell in all segment instance directories, include master and all segment instances

Example: 
perl run_on_all_segdir.pl "echo \"hello\""
perl run_on_all_segdir.pl "grep gpadmin pg_hba.conf |awk '{print \\\$4}'"

Information:
Because the script should connect to GP to load all the segment info, Script should run on GP starting or GP master starting status.

