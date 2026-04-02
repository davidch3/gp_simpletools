package main

import (
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

const (
	StatusNotStart  = 0 // 未启动
	StatusRunning   = 1 // 处理中
	StatusCompleted = 2 // 处理完成
	StatusFailed    = 3 // 处理失败
)

type MonitorSetup struct {
	MonitorID     int
	Description   string
	MetricName    string
	CheckInterval int
	IfWarning     bool
	WarningValue  string
	Memo          string
	// 注意：这里刻意不放 status/时间/错误（你说“读出不需要管状态时间等”）
}

func LoadMonitorSetups() ([]MonitorSetup, error) {
	var gpconn *pgx.Conn
	var err error
	var rows pgx.Rows
	if gpconn, err = DBconnect(gpconnString, 30*time.Second); err != nil {
		return nil, fmt.Errorf("DBConnect error: %w", err)
	}
	defer DBclose(gpconn)

	rows, err = QueryRows(gpconn,
		`	select monitor_id, description, metric_name, 
		       check_interval, if_warning, warning_value, memo
		  from monitor.monitor_setup
		 order by monitor_id
	`)
	if err != nil {
		return nil, fmt.Errorf("load monitor_setup failed: %w", err)
	}
	defer rows.Close()

	var list []MonitorSetup
	for rows.Next() {
		var m MonitorSetup
		if err := rows.Scan(
			&m.MonitorID,
			&m.Description,
			&m.MetricName,
			&m.CheckInterval,
			&m.IfWarning,
			&m.WarningValue,
			&m.Memo,
		); err != nil {
			return nil, fmt.Errorf("scan monitor_setup failed: %w", err)
		}
		list = append(list, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate monitor_setup failed: %w", err)
	}

	return list, nil
}

func ResetMonitorStatus() error {
	var gpconn *pgx.Conn
	var err error
	var rowcnt int64

	if gpconn, err = DBconnect(gpconnString, 30*time.Second); err != nil {
		return fmt.Errorf("DBConnect error: %w", err)
	}
	defer DBclose(gpconn)

	rowcnt, err = ExecSQL(gpconn,
		`update monitor.monitor_setup set status=0,last_finishtime=null where 1=1`)
	if err != nil {
		return fmt.Errorf("Reset monitor_setup status failed: %w", err)
	}
	if rowcnt == 0 {
		return fmt.Errorf("Updated rows is 0!")
	}

	return nil
}

func NeedRun(it MonitorSetup, gpconn *pgx.Conn) (bool, error) {
	var err error
	var row pgx.Row
	var mon_status int
	var LastFinishTime pgtype.Timestamp
	var isTimeToRun bool
	row, err = QueryRow(gpconn,
		`select status,last_finishtime,
		case when last_finishtime is null then true
		else now() - last_finishtime > check_interval * interval '1 minute' end as isTimeToRun
		from monitor.monitor_setup where monitor_id=$1`, it.MonitorID)
	if err = row.Scan(&mon_status, &LastFinishTime, &isTimeToRun); err != nil {
		return false, fmt.Errorf("Query monitor item [%d] status error: %w", it.MonitorID, err)
	}

	switch mon_status {
	case StatusRunning:
		mylog.Log("[%d][%s] is still running!", it.MonitorID, it.MetricName)
		return false, nil
	case StatusNotStart:
		mylog.Log("[%d][%s] is not started!", it.MonitorID, it.MetricName)
		return true, nil
	case StatusCompleted, StatusFailed:
		mylog.Log("[%d][%s] LastFinish[%s] isTimeToRun[%v]",
			it.MonitorID, it.MetricName, LastFinishTime.Time.Format("2006-01-02 15:04:05"), isTimeToRun)
		return isTimeToRun, nil
	default:
		mylog.Error("[%d][%s] has invalid status [%d]!", it.MonitorID, it.MetricName, mon_status)
		return false, fmt.Errorf("[%d][%s] has invalid status [%d]!", it.MonitorID, it.MetricName, mon_status)
	}
}

func TryStartMonitor(it MonitorSetup, gpconn *pgx.Conn) error {
	var err error
	var rowcnt int64
	rowcnt, err = ExecSQL(gpconn,
		`update monitor.monitor_setup set status=$1,last_finishtime=now() where monitor_id=$2`, StatusRunning, it.MonitorID)
	if err != nil {
		mylog.Error("[%d][%s] MonitorSetup status update error: %v", it.MonitorID, it.MetricName, err)
		return fmt.Errorf("[%d][%s] MonitorSetup status update error: %w", it.MonitorID, it.MetricName, err)
	}
	if rowcnt == 0 {
		mylog.Error("[%d][%s] MonitorSetup status update rows is 0!", it.MonitorID, it.MetricName)
		return fmt.Errorf("[%d][%s] MonitorSetup status update rows is 0!", it.MonitorID, it.MetricName)
	}

	return nil
}

func InsertHistory_UpdateStatus(it MonitorSetup, MonitorResult string, WarningMsg string, Status int, errmsg string, gpconn *pgx.Conn) error {
	var err error

	_, err = ExecSQL(gpconn,
		`update monitor.monitor_setup set status=$1,last_finishtime=now(),last_error=$2
		where monitor_id=$3`, Status, errmsg, it.MonitorID)
	if err != nil {
		mylog.Error("Update monitor_setup status failed: %v", err)
		return fmt.Errorf("Update monitor_setup status failed: %w", err)
	}

	_, err = ExecSQL(gpconn,
		`insert into monitor.monitor_history
		(ctime, monitor_id, metric_name, monitor_result, warning_msg,
		 textfile_name, status, last_error)
		values
		(now(),$1,$2,$3,$4,$5,$6,$7)
	`, it.MonitorID, it.MetricName, MonitorResult, WarningMsg, it.MetricName+".txt", Status, errmsg)
	if err != nil {
		mylog.Error("Insert monitor_history failed: %v", err)
		return fmt.Errorf("Insert monitor_history failed: %w", err)
	}
	return nil
}

func Monitor_Maintenance(it MonitorSetup) error {
	var gpconn *pgx.Conn
	var err error
	if gpconn, err = DBconnect(gpconnString, 30*time.Second); err != nil {
		return fmt.Errorf("DBConnect error: %w", err)
	}
	defer DBclose(gpconn)

	var isRun bool
	isRun, err = NeedRun(it, gpconn)
	if err != nil {
		return err
	}
	if !isRun {
		return nil
	}

	err = TryStartMonitor(it, gpconn)
	if err != nil {
		return err
	}

	var sResult string
	var sWarningMsg string
	_, err = ExecSQL(gpconn,
		`delete from monitor.monitor_history where ctime < now() - ($1 * interval '1 day');`, Del_Hisdata_interv)
	if err != nil {
		mylog.Error("Delete monitor_history error: %v", err)
		Err_string := fmt.Sprintf("%v", err)
		InsertHistory_UpdateStatus(it, "", "", StatusFailed, Err_string, gpconn)
		return fmt.Errorf("Delete monitor_history error: %w", err)
	}

	_, err = ExecSQL(gpconn, `vacuum analyze monitor.monitor_history;`)
	if err != nil {
		mylog.Error("vacuum analyze monitor_history error: %v", err)
		Err_string := fmt.Sprintf("%v", err)
		InsertHistory_UpdateStatus(it, "", "", StatusFailed, Err_string, gpconn)
		return fmt.Errorf("vacuum analyze monitor_history error: %w", err)
	}

	_, err = ExecSQL(gpconn, `vacuum analyze monitor.monitor_setup;`)
	if err != nil {
		mylog.Error("vacuum analyze monitor_setup error: %v", err)
		Err_string := fmt.Sprintf("%v", err)
		InsertHistory_UpdateStatus(it, "", "", StatusFailed, Err_string, gpconn)
		return fmt.Errorf("vacuum analyze monitor_setup error: %w", err)
	}

	sResult = "0"
	sWarningMsg = "Monitor维护任务正常结束"
	mylog.Log(sWarningMsg)
	MonitorOutput(it, sResult)
	InsertHistory_UpdateStatus(it, sResult, sWarningMsg, StatusCompleted, "", gpconn)

	return nil
}

func UnloadHostfile() error {
	var gpconn *pgx.Conn
	var err error
	if gpconn, err = DBconnect(gpconnString, 30*time.Second); err != nil {
		return fmt.Errorf("DBConnect error: %w", err)
	}
	defer DBclose(gpconn)

	_, err = ExecSQL(gpconn,
		`copy (select distinct hostname from gp_segment_configuration where content>=0) to '/tmp/.allsegs.txt'`)
	if err != nil {
		return fmt.Errorf("Unload allsegs error: %w", err)
	}
	_, err = ExecSQL(gpconn,
		`copy (select distinct hostname from gp_segment_configuration) to '/tmp/.allhosts.txt'`)
	if err != nil {
		return fmt.Errorf("Unload allhosts error: %w", err)
	}

	return nil
}
