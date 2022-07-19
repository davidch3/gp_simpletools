#!/bin/bash
set +o errexit

if [ $# -ne 2 ] && [ $# -ne 4 ]; then
  echo "invalid parameters!"
  echo "
Usage:`basename $0` -f allhosts 
   or 
      `basename $0` -f allhosts -e allexpands"
  exit 1; 
fi

while getopts "f:e": opts
do
        case $opts in
        f) allhosts_FILENAME=$OPTARG 
           ;;
        e) allexpands_FILENAME=$OPTARG 
           ;;
        \?)
           echo "invalid parameters!"  
           echo "
Usage:`basename $0` -f allhosts 
   or 
      `basename $0` -f allhosts -e allexpands"
           exit 1;
                ;;
        esac 
done

DATETIME=`date +%Y%m%d%H%M%S`

if [ ! -f "$allhosts_FILENAME" ]; then
  echo "Hostfile \"$allhosts_FILENAME\" does not exists!!!"
  exit 1
fi

USERNAME=`whoami`

read -p "Please input <$USERNAME> password of all hosts: " passwd

if [ "$allexpands_FILENAME"x = ""x ]; then
  ssh-keygen -t rsa -N "" -f $HOME/.ssh/id_rsa -q
  if [ -f "$HOME/.ssh/known_hosts" ]; then
    echo "Clean up known_hosts"
    cp $HOME/.ssh/known_hosts $HOME/.ssh/known_hosts.bak$DATETIME
    > $HOME/.ssh/known_hosts
  fi
fi

if [ "$allexpands_FILENAME"x != ""x ]; then
  HOSTLIST=`cat $allexpands_FILENAME |grep -v "^$" |grep -v "^#"`
else
  HOSTLIST=`cat $allhosts_FILENAME |grep -v "^$" |grep -v "^#"`
fi
for hostname in $HOSTLIST
do
  echo "[$hostname] Add known_host and ssh-copy-id ..."
  ssh-keygen -R $hostname 
  ssh-keyscan -H $hostname 1>> $HOME/.ssh/known_hosts 
  sshpass -p $passwd ssh-copy-id -i $HOME/.ssh/id_rsa.pub $hostname
done

echo "Running gpssh-exkeys ..."
source /usr/local/greenplum-db/greenplum_path.sh
gpssh-exkeys -f $allhosts_FILENAME




