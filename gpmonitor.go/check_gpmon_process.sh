#/bin/bash

CONFIG_FILE="/home/gpadmin/gpmonitor.go/monitor.conf"
METRIC_NAME="gpdb_monitor_processes"
METRIC_FILE="$METRIC_NAME.txt"

get_conf_value() {
    local key="$1"
    grep -E "^[[:space:]]*${key}[[:space:]]*=" "$CONFIG_FILE" | tail -1 | sed 's/^[^=]*=//' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//'
}

if [ ! -f "$CONFIG_FILE" ]; then
    echo "Config file not found: $CONFIG_FILE"
    exit 1
fi

OUTPUT_DIR=$(get_conf_value "GPmon_Path")
if [ -z "$OUTPUT_DIR" ]; then
    echo "Output dir not found in config"
    exit 1
fi

echo $OUTPUT_DIR

PROCESS_CNT=$(pgrep -x gpmonitor|wc -l)

if [ $PROCESS_CNT -eq 0 ]; then
    MONITOR_RESULT="1"
    WARNING_MSG="监控程序未启动"
else
    MONITOR_RESULT="0"
    WARNING_MSG="监控进程正常"
fi

SQL="
insert into monitor.monitor_history 
(ctime,monitor_id,metric_name,monitor_result,warning_msg,textfile_name,status)
values
(now(),0,'${METRIC_NAME}','${MONITOR_RESULT}','${WARNING_MSG}','${METRIC_FILE}',2);
"
echo "${MONITOR_RESULT}" > ${OUTPUT_DIR}/${METRIC_FILE}
psql postgres -At -c "$SQL"

exit 0

