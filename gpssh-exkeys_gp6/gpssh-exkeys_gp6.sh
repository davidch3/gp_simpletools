#!/bin/bash
set -o errexit

if [[ $# -lt 2  ]]; then
  echo "invalid parameters!"
  echo "usage:`basename $0` -f allhosts "
  exit 1; 
fi

while getopts f: opts
do
        case $opts in
        f) allhosts_FILENAME=$OPTARG 
           ;;
        \?)
           echo "invalid parameters!"  
           echo "usage:`basename $0` -f allhosts "
           exit 1;
                ;;
        esac 
done

if [ ! -f "$allhosts_FILENAME" ]; then
  echo "Hostfile \"$allhosts_FILENAME\" does not exists!!!"
  exit 1
fi

read -p "Please input user password of all hosts: " passwd

if [ -f "$HOME/.ssh/id_rsa" ]; then
  echo "$HOME/.ssh/id_rsa file exists ... key generation skipped"
else
  ssh-keygen -t rsa -N "" -f $HOME/.ssh/id_rsa -q
fi

HOSTLIST=`cat $allhosts_FILENAME |grep -v "^$" |grep -v "^#"`
for hostname in $HOSTLIST
do
  echo "[$hostname] Add known_host and ssh-copy-id ..."
  ssh-keygen -R $hostname 1>/dev/null 2>/dev/null
  ssh-keyscan -H $hostname 2>/dev/null >> $HOME/.ssh/known_hosts 
  sshpass -p $passwd ssh-copy-id $hostname 1>/dev/null
done

echo "Running gpssh-exkeys ..."
source /usr/local/greenplum-db/greenplum_path.sh
gpssh-exkeys -f $allhosts_FILENAME




