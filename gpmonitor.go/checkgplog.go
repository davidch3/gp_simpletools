package main

import (
	"bufio"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

func GPMasPanic(it MonitorSetup) error {
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

	var row pgx.Row
	var StartTime string
	var StartDate string
	CurrDate := time.Now().Format("2006-01-02")
	row, err = QueryRow(gpconn,
		`select coalesce(to_char(max(ctime),'YYYY-MM-DD HH24:MI:SS'),''),coalesce(to_char(max(ctime),'YYYY-MM-DD'),'') from monitor.monitor_history where monitor_id=$1`, it.MonitorID)
	if err = row.Scan(&StartTime, &StartDate); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			StartDate = CurrDate
			StartTime = time.Now().Format("2006-01-02") + " 00:00:00"
		} else {
			mylog.Error("Query last check panic time error: %v", err)
			Err_string := fmt.Sprintf("%v", err)
			InsertHistory_UpdateStatus(it, "", "", StatusFailed, Err_string, gpconn)
			return fmt.Errorf("Query last check panic time error: %w", err)
		}
	}
	mylog.Log("StartDate[%s],StartTime[%s]", StartDate, StartTime)

	var cmdres CmdResult
	var checkcmd string
	checkcmd = fmt.Sprintf(`gplogfilter -b "%s" -t -f "PANIC" $MASTER_DATA_DIRECTORY/pg_log/gpdb-%s_*.csv 2>/dev/null|grep "PANIC:" |wc -l`, StartTime, StartDate)
	mylog.Raw(checkcmd)
	cmdres = ExecBashTimeout(300*time.Second, checkcmd)
	if cmdres.Err != nil {
		mylog.Error("gplogfilter check master panic 1 error: %v", cmdres.Err)
		Err_string := fmt.Sprintf("%v", cmdres.Err)
		InsertHistory_UpdateStatus(it, "", "", StatusFailed, Err_string, gpconn)
		return fmt.Errorf("gplogfilter check master panic 1 error: %w", cmdres.Err)
	}
	check1, _ := strconv.Atoi(strings.TrimRight(cmdres.Stdout, "\r\n"))
	check2 := 0
	if CurrDate != StartDate {
		checkcmd = fmt.Sprintf(`gplogfilter -b "%s" -t -f "PANIC" $MASTER_DATA_DIRECTORY/pg_log/gpdb-%s_*.csv 2>/dev/null|grep "PANIC:" |wc -l`, StartTime, CurrDate)
		mylog.Raw(checkcmd)
		cmdres = ExecBashTimeout(300*time.Second, checkcmd)
		if cmdres.Err != nil {
			mylog.Error("gplogfilter check master panic 2 error: %v", cmdres.Err)
			Err_string := fmt.Sprintf("%v", cmdres.Err)
			InsertHistory_UpdateStatus(it, "", "", StatusFailed, Err_string, gpconn)
			return fmt.Errorf("gplogfilter check master panic 2 error: %w", cmdres.Err)
		}
		check2, _ = strconv.Atoi(strings.TrimRight(cmdres.Stdout, "\r\n"))
	}

	var sResult string
	var sWarningMsg string
	if check1+check2 > 0 {
		sResult = "1"
		sWarningMsg = fmt.Sprintf("Master上发现%d条PANIC错误", check1+check2)
	} else {
		sResult = "0"
		sWarningMsg = "Master上未发现PANIC错误"
	}
	mylog.Log(sWarningMsg)
	MonitorOutput(it, sResult)
	InsertHistory_UpdateStatus(it, sResult, sWarningMsg, StatusCompleted, "", gpconn)

	return nil
}

func GPSegsPanic(it MonitorSetup) error {
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

	var row pgx.Row
	var StartTime string
	var StartDate string
	CurrDate := time.Now().Format("2006-01-02")
	row, err = QueryRow(gpconn,
		`select coalesce(to_char(max(ctime),'YYYY-MM-DD HH24:MI:SS'),''),coalesce(to_char(max(ctime),'YYYY-MM-DD'),'') from monitor.monitor_history where monitor_id=$1`, it.MonitorID)
	if err = row.Scan(&StartTime, &StartDate); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			StartDate = CurrDate
			StartTime = time.Now().Format("2006-01-02") + " 00:00:00"
		} else {
			mylog.Error("Query last check panic time error: %v", err)
			Err_string := fmt.Sprintf("%v", err)
			InsertHistory_UpdateStatus(it, "", "", StatusFailed, Err_string, gpconn)
			return fmt.Errorf("Query last check panic time error: %w", err)
		}
	}
	mylog.Log("StartDate[%s],StartTime[%s]", StartDate, StartTime)

	var cmdres CmdResult
	var localcmd string
	var checkcmd string
	localcmd = fmt.Sprintf(`gplogfilter -b \"%s\" -t -f \"PANIC\" /data*/primary/gpseg*/pg_log/gpdb-%s_*.csv 2>/dev/null|grep \"PANIC:\" |wc -l`, StartTime, StartDate)
	checkcmd = fmt.Sprintf(`gpssh -f /tmp/.allsegs.txt "source /usr/local/greenplum-db/greenplum_path.sh; %s"`, localcmd)
	mylog.Raw(checkcmd)
	cmdres = ExecBashTimeout(300*time.Second, checkcmd)
	if cmdres.Err != nil {
		mylog.Error("gplogfilter check segment panic 1 error: %v", cmdres.Err)
		Err_string := fmt.Sprintf("%v", cmdres.Err)
		InsertHistory_UpdateStatus(it, "", "", StatusFailed, Err_string, gpconn)
		return fmt.Errorf("gplogfilter check master segment 1 error: %w", cmdres.Err)
	}
	check1 := strings.TrimRight(cmdres.Stdout, "\r\n")
	check2 := ""

	if CurrDate != StartDate {
		localcmd = fmt.Sprintf(`gplogfilter -b \"%s\" -t -f \"PANIC\" /data*/primary/gpseg*/pg_log/gpdb-%s_*.csv 2>/dev/null|grep \"PANIC:\" |wc -l`, StartTime, CurrDate)
		checkcmd = fmt.Sprintf(`gpssh -f /tmp/.allsegs.txt "source /usr/local/greenplum-db/greenplum_path.sh; %s"`, localcmd)
		mylog.Raw(checkcmd)
		cmdres = ExecBashTimeout(300*time.Second, checkcmd)
		if cmdres.Err != nil {
			mylog.Error("gplogfilter check segment panic 2 error: %v", cmdres.Err)
			Err_string := fmt.Sprintf("%v", cmdres.Err)
			InsertHistory_UpdateStatus(it, "", "", StatusFailed, Err_string, gpconn)
			return fmt.Errorf("gplogfilter check segment panic 2 error: %w", cmdres.Err)
		}
		check2 = strings.TrimRight(cmdres.Stdout, "\r\n")
	}

	OutString := check1 + "\n" + check2
	var sResult string
	var sWarningMsg string
	scanner := bufio.NewScanner(strings.NewReader(OutString))
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)
	cnt := 0
	sWarningMsg = ""
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, " ", 2)
		if ii, _ := strconv.Atoi(strings.TrimRight(parts[1], "\n")); ii > 0 {
			if sWarningMsg == "" {
				sWarningMsg = sWarningMsg + fmt.Sprintf("%s节点上发现PANIC报错", parts[0])
			} else {
				sWarningMsg = sWarningMsg + "," + fmt.Sprintf("%s节点上发现PANIC报错", parts[0])
			}
			cnt = cnt + ii
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("Scan gpssh output failed: %w", err)
	}
	sResult = fmt.Sprintf("%d", cnt)
	if sWarningMsg == "" {
		sWarningMsg = "Segment上未发现PANIC错误"
	}
	mylog.Log(sWarningMsg)
	MonitorOutput(it, sResult)
	InsertHistory_UpdateStatus(it, sResult, sWarningMsg, StatusCompleted, "", gpconn)

	return nil
}

func GPInternalError(it MonitorSetup) error {
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

	var row pgx.Row
	var StartTime string
	var StartDate string
	CurrDate := time.Now().Format("2006-01-02")
	row, err = QueryRow(gpconn,
		`select coalesce(to_char(max(ctime),'YYYY-MM-DD HH24:MI:SS'),''),coalesce(to_char(max(ctime),'YYYY-MM-DD'),'') from monitor.monitor_history where monitor_id=$1`, it.MonitorID)
	if err = row.Scan(&StartTime, &StartDate); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			StartDate = CurrDate
			StartTime = time.Now().Format("2006-01-02") + " 00:00:00"
		} else {
			mylog.Error("Query last check internal error time error: %v", err)
			Err_string := fmt.Sprintf("%v", err)
			InsertHistory_UpdateStatus(it, "", "", StatusFailed, Err_string, gpconn)
			return fmt.Errorf("Query last check internal error time error: %w", err)
		}
	}
	mylog.Log("StartDate[%s],StartTime[%s]", StartDate, StartTime)

	var cmdres CmdResult
	var checkcmd string
	checkcmd = fmt.Sprintf(`gplogfilter -b "%s" -t -f "internal error" $MASTER_DATA_DIRECTORY/pg_log/gpdb-%s_*.csv 2>/dev/null|grep "internal error" |wc -l`, StartTime, StartDate)
	mylog.Raw(checkcmd)
	cmdres = ExecBashTimeout(300*time.Second, checkcmd)
	if cmdres.Err != nil {
		mylog.Error("gplogfilter check internal error 1 error: %v", cmdres.Err)
		Err_string := fmt.Sprintf("%v", cmdres.Err)
		InsertHistory_UpdateStatus(it, "", "", StatusFailed, Err_string, gpconn)
		return fmt.Errorf("gplogfilter check internal error 1 error: %w", cmdres.Err)
	}
	check1, _ := strconv.Atoi(strings.TrimRight(cmdres.Stdout, "\r\n"))
	check2 := 0
	if CurrDate != StartDate {
		checkcmd = fmt.Sprintf(`gplogfilter -b "%s" -t -f "internal error" $MASTER_DATA_DIRECTORY/pg_log/gpdb-%s_*.csv 2>/dev/null|grep "internal error" |wc -l`, StartTime, CurrDate)
		mylog.Raw(checkcmd)
		cmdres = ExecBashTimeout(300*time.Second, checkcmd)
		if cmdres.Err != nil {
			mylog.Error("gplogfilter check internal error 2 error: %v", cmdres.Err)
			Err_string := fmt.Sprintf("%v", cmdres.Err)
			InsertHistory_UpdateStatus(it, "", "", StatusFailed, Err_string, gpconn)
			return fmt.Errorf("gplogfilter check internal error 2 error: %w", cmdres.Err)
		}
		check2, _ = strconv.Atoi(strings.TrimRight(cmdres.Stdout, "\r\n"))
	}

	var sResult string
	var sWarningMsg string
	if check1+check2 > 0 {
		sResult = "1"
		sWarningMsg = fmt.Sprintf("Master上发现%d条internal error", check1+check2)
	} else {
		sResult = "0"
		sWarningMsg = "Master上未发现internal error"
	}
	mylog.Log(sWarningMsg)
	MonitorOutput(it, sResult)
	InsertHistory_UpdateStatus(it, sResult, sWarningMsg, StatusCompleted, "", gpconn)

	return nil
}

func GPOutOfMemory(it MonitorSetup) error {
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

	var row pgx.Row
	var StartTime string
	var StartDate string
	CurrDate := time.Now().Format("2006-01-02")
	row, err = QueryRow(gpconn,
		`select coalesce(to_char(max(ctime),'YYYY-MM-DD HH24:MI:SS'),''),coalesce(to_char(max(ctime),'YYYY-MM-DD'),'') from monitor.monitor_history where monitor_id=$1`, it.MonitorID)
	if err = row.Scan(&StartTime, &StartDate); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			StartDate = CurrDate
			StartTime = time.Now().Format("2006-01-02") + " 00:00:00"
		} else {
			mylog.Error("Query last check oom time error: %v", err)
			Err_string := fmt.Sprintf("%v", err)
			InsertHistory_UpdateStatus(it, "", "", StatusFailed, Err_string, gpconn)
			return fmt.Errorf("Query last check oom time error: %w", err)
		}
	}
	mylog.Log("StartDate[%s],StartTime[%s]", StartDate, StartTime)

	var cmdres CmdResult
	var checkcmd string
	checkcmd = fmt.Sprintf(`gplogfilter -b "%s" -t -f "Out of memory" $MASTER_DATA_DIRECTORY/pg_log/gpdb-%s_*.csv 2>/dev/null|grep "Out of memory" |wc -l`, StartTime, StartDate)
	mylog.Raw(checkcmd)
	cmdres = ExecBashTimeout(300*time.Second, checkcmd)
	if cmdres.Err != nil {
		mylog.Error("gplogfilter check oom 1 error: %v", cmdres.Err)
		Err_string := fmt.Sprintf("%v", cmdres.Err)
		InsertHistory_UpdateStatus(it, "", "", StatusFailed, Err_string, gpconn)
		return fmt.Errorf("gplogfilter check oom 1 error: %w", cmdres.Err)
	}
	check1, _ := strconv.Atoi(strings.TrimRight(cmdres.Stdout, "\r\n"))
	check2 := 0
	if CurrDate != StartDate {
		checkcmd = fmt.Sprintf(`gplogfilter -b "%s" -t -f "Out of memory" $MASTER_DATA_DIRECTORY/pg_log/gpdb-%s_*.csv 2>/dev/null|grep "Out of memory" |wc -l`, StartTime, CurrDate)
		mylog.Raw(checkcmd)
		cmdres = ExecBashTimeout(300*time.Second, checkcmd)
		if cmdres.Err != nil {
			mylog.Error("gplogfilter check oom 2 error: %v", cmdres.Err)
			Err_string := fmt.Sprintf("%v", cmdres.Err)
			InsertHistory_UpdateStatus(it, "", "", StatusFailed, Err_string, gpconn)
			return fmt.Errorf("gplogfilter check oom 2 error: %w", cmdres.Err)
		}
		check2, _ = strconv.Atoi(strings.TrimRight(cmdres.Stdout, "\r\n"))
	}

	var sResult string
	var sWarningMsg string
	if check1+check2 > 0 {
		sResult = "1"
		sWarningMsg = fmt.Sprintf("Master上发现%d条Out of memory错误", check1+check2)
	} else {
		sResult = "0"
		sWarningMsg = "Master上未发现Out of memory错误"
	}
	mylog.Log(sWarningMsg)
	MonitorOutput(it, sResult)
	InsertHistory_UpdateStatus(it, sResult, sWarningMsg, StatusCompleted, "", gpconn)

	return nil
}
