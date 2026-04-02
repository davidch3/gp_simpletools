package main

import (
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

func GPTempSize(it MonitorSetup) error {
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
	var sResult string
	var sWarningMsg string
	var checksql string
	if gpver == "gp4" || gpver == "gp5" {
		checksql = `SELECT procpid,sess_id,usename,segid,round(size/1024/1024/1024,2) AS workfile_size
        FROM gp_toolkit.gp_workfile_usage_per_query order by size desc limit 1;`
	} else {
		checksql = `SELECT pid,sess_id,usename,segid,round(size/1024/1024/1024,2) AS workfile_size
        FROM gp_toolkit.gp_workfile_usage_per_query order by size desc limit 1;`
	}
	row, err = QueryRow(gpconn, checksql)
	var Pid int
	var Sessid int
	var Username string
	var SegID int
	var WorkfileSize float64
	if err = row.Scan(&Pid, &Sessid, &Username, &SegID, &WorkfileSize); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			WorkfileSize = 0.00
		} else {
			mylog.Error("Check Workfile size per query error: %v", err)
			Err_string := fmt.Sprintf("%v", err)
			InsertHistory_UpdateStatus(it, "", "", StatusFailed, Err_string, gpconn)
			return fmt.Errorf("Check Workfile size per query error: %w", err)
		}
	}
	if math.Abs(WorkfileSize) < 0.01 {
		sResult = "0.00"
		sWarningMsg = "数据库运行-当前没有SQL输出workfile"
	} else {
		sResult = fmt.Sprintf("%.2f", WorkfileSize)
		sWarningMsg = fmt.Sprintf("数据库运行-临时文件情况: 用户%s会话号con%d中的SQL，在实例gpseg%d上输出的workfile大小为%.2fGB", Username, Sessid, SegID, WorkfileSize)
	}
	mylog.Log(sWarningMsg)
	MonitorOutput(it, sResult)
	InsertHistory_UpdateStatus(it, sResult, sWarningMsg, StatusCompleted, "", gpconn)

	return nil
}

func GPCheckTableLock(it MonitorSetup) error {
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
	var sResult string
	var sWarningMsg string
	row, err = QueryRow(gpconn,
		`select lock_table,run_user,run_procpid,wait_user,wait_procpid,round(extract(EPOCH FROM wait_time)/60) as wait_time
        from v_check_lock_tables order by wait_time desc limit 1;`)
	var LockTable string
	var RunUser string
	var RunPid int
	var WaitUser string
	var WaitPid int
	var WaitTime int
	if err = row.Scan(&LockTable, &RunUser, &RunPid, &WaitUser, &WaitPid, &WaitTime); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			WaitTime = 0
		} else {
			mylog.Error("Check Table Lock error: %v", err)
			Err_string := fmt.Sprintf("%v", err)
			InsertHistory_UpdateStatus(it, "", "", StatusFailed, Err_string, gpconn)
			return fmt.Errorf("Check Table Lock error: %w", err)
		}
	}
	if WaitTime == 0 {
		sResult = "0"
		sWarningMsg = "数据库运行-当前没有等表锁的SQL"
	} else {
		sResult = fmt.Sprintf("%d", WaitTime)
		sWarningMsg = fmt.Sprintf("数据库运行-等锁情况: 用户%s提交的进程号%d的会话锁住表%s, 用户%s提交的SQL已经等待%d分钟", RunUser, RunPid, LockTable, WaitUser, WaitTime)
	}
	mylog.Log(sWarningMsg)
	MonitorOutput(it, sResult)
	InsertHistory_UpdateStatus(it, sResult, sWarningMsg, StatusCompleted, "", gpconn)

	return nil
}

func GPCheckNetworkFlooding(it MonitorSetup) error {
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
	_, err = ExecSQL_timeout(Check_TimeOut_Sec*time.Second,
		`with foo as (select  generate_series(1,1000) from gp_dist_random('gp_id')) 
		select count(*) from foo a ,foo b ;`)
	if err != nil {
		if IsTimeout(err) {
			sResult = "1"
			sWarningMsg = fmt.Sprintf("数据库运行-网络泛洪检查: SQL笛卡尔积执行超过%d秒, 可能存在网络泛洪", Check_TimeOut_Sec)
			mylog.Log(sWarningMsg)
			MonitorOutput(it, sResult)
			InsertHistory_UpdateStatus(it, sResult, sWarningMsg, StatusCompleted, "", gpconn)
		} else {
			mylog.Error("Check Network Flooding error: %v", err)
			Err_string := fmt.Sprintf("%v", err)
			InsertHistory_UpdateStatus(it, "", "", StatusFailed, Err_string, gpconn)
			return fmt.Errorf("Check Network Flooding error: %w", err)
		}
	}

	sResult = "0"
	sWarningMsg = "数据库运行-网络泛洪检查: SQL笛卡尔积执行未超时"
	mylog.Log(sWarningMsg)
	MonitorOutput(it, sResult)
	InsertHistory_UpdateStatus(it, sResult, sWarningMsg, StatusCompleted, "", gpconn)

	return nil
}

func GPLoadRunSql(it MonitorSetup) error {
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
	var sResult string
	var sWarningMsg string
	var checksql string
	if gpver == "gp4" || gpver == "gp5" {
		checksql = `select count(*) from pg_stat_activity where current_query not like '%IDLE%';`
	} else {
		checksql = `select count(*) from pg_stat_activity where state='active';`
	}
	row, err = QueryRow(gpconn, checksql)
	var cnt int
	if err = row.Scan(&cnt); err != nil {
		mylog.Error("Check Running SQL error: %v", err)
		Err_string := fmt.Sprintf("%v", err)
		InsertHistory_UpdateStatus(it, "", "", StatusFailed, Err_string, gpconn)
		return fmt.Errorf("Check Running SQL error: %w", err)
	}
	sResult = fmt.Sprintf("%d", cnt)
	sWarningMsg = fmt.Sprintf("数据库负载情况-数据库当前执行SQL数: %d", cnt)
	mylog.Log(sWarningMsg)
	MonitorOutput(it, sResult)
	InsertHistory_UpdateStatus(it, sResult, sWarningMsg, StatusCompleted, "", gpconn)

	return nil
}

func GPLoadWaitSql(it MonitorSetup) error {
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
	var sResult string
	var sWarningMsg string
	var checksql string
	if gpver == "gp6" {
		checksql = `select count(*) from pg_stat_activity where waiting=true and state='active';`
	} else if gpver == "gp7" || strings.Contains(gpver, "cbdb") {
		checksql = `select count(*) from pg_stat_activity where wait_event_type='Lock' and state='active';`
	} else {
		checksql = `select count(*) from pg_stat_activity where waiting=true and current_query not like '%IDLE%';`
	}
	row, err = QueryRow(gpconn, checksql)
	var cnt int
	if err = row.Scan(&cnt); err != nil {
		mylog.Error("Check Waiting SQL error: %v", err)
		Err_string := fmt.Sprintf("%v", err)
		InsertHistory_UpdateStatus(it, "", "", StatusFailed, Err_string, gpconn)
		return fmt.Errorf("Check Waiting SQL error: %w", err)
	}
	sResult = fmt.Sprintf("%d", cnt)
	sWarningMsg = fmt.Sprintf("数据库负载情况-数据库当前等待执行SQL数: %d", cnt)
	mylog.Log(sWarningMsg)
	MonitorOutput(it, sResult)
	InsertHistory_UpdateStatus(it, sResult, sWarningMsg, StatusCompleted, "", gpconn)

	return nil
}

func GPConnections(it MonitorSetup) error {
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
	var sResult string
	var sWarningMsg string
	row, err = QueryRow(gpconn, `select count(*) from pg_stat_activity;`)
	var cnt int
	if err = row.Scan(&cnt); err != nil {
		mylog.Error("Check Total connections error: %v", err)
		Err_string := fmt.Sprintf("%v", err)
		InsertHistory_UpdateStatus(it, "", "", StatusFailed, Err_string, gpconn)
		return fmt.Errorf("Check Total connections error: %w", err)
	}
	sResult = fmt.Sprintf("%d", cnt)
	sWarningMsg = fmt.Sprintf("数据库负载情况-数据库当前总连接数: %d", cnt)
	mylog.Log(sWarningMsg)
	MonitorOutput(it, sResult)
	InsertHistory_UpdateStatus(it, sResult, sWarningMsg, StatusCompleted, "", gpconn)

	return nil
}

func GPResourGroupRun(it MonitorSetup) error {
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
	var sResult string
	var sWarningMsg string
	row, err = QueryRow(gpconn, `select sum(num_running) from gp_toolkit.gp_resgroup_status;`)
	var cnt int
	if err = row.Scan(&cnt); err != nil {
		mylog.Error("Check Resource Group running SQLs error: %v", err)
		Err_string := fmt.Sprintf("%v", err)
		InsertHistory_UpdateStatus(it, "", "", StatusFailed, Err_string, gpconn)
		return fmt.Errorf("Check Resource Group running SQLs error: %w", err)
	}
	sResult = fmt.Sprintf("%d", cnt)
	sWarningMsg = fmt.Sprintf("数据库负载情况-资源组中总执行数量: %d", cnt)
	mylog.Log(sWarningMsg)
	MonitorOutput(it, sResult)
	InsertHistory_UpdateStatus(it, sResult, sWarningMsg, StatusCompleted, "", gpconn)

	return nil
}

func GPResourGroupWait(it MonitorSetup) error {
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
	var sResult string
	var sWarningMsg string
	row, err = QueryRow(gpconn, `select sum(num_queueing) from gp_toolkit.gp_resgroup_status;`)
	var cnt int
	if err = row.Scan(&cnt); err != nil {
		mylog.Error("Check Resource Group queueing SQLs error: %v", err)
		Err_string := fmt.Sprintf("%v", err)
		InsertHistory_UpdateStatus(it, "", "", StatusFailed, Err_string, gpconn)
		return fmt.Errorf("Check Resource Group queueing SQLs error: %w", err)
	}
	sResult = fmt.Sprintf("%d", cnt)
	sWarningMsg = fmt.Sprintf("数据库负载情况-资源组中总等待数量: %d", cnt)
	mylog.Log(sWarningMsg)
	MonitorOutput(it, sResult)
	InsertHistory_UpdateStatus(it, sResult, sWarningMsg, StatusCompleted, "", gpconn)

	return nil
}

func GPResourQueueRun(it MonitorSetup) error {
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
	var sResult string
	var sWarningMsg string
	row, err = QueryRow(gpconn, `select count(*) from gp_toolkit.gp_resq_activity where resqstatus = 'running';`)
	var cnt int
	if err = row.Scan(&cnt); err != nil {
		mylog.Error("Check Resource Queue running SQLs error: %v", err)
		Err_string := fmt.Sprintf("%v", err)
		InsertHistory_UpdateStatus(it, "", "", StatusFailed, Err_string, gpconn)
		return fmt.Errorf("Check Resource Queue running SQLs error: %w", err)
	}
	sResult = fmt.Sprintf("%d", cnt)
	sWarningMsg = fmt.Sprintf("数据库负载情况-资源队列中总执行数量: %d", cnt)
	mylog.Log(sWarningMsg)
	MonitorOutput(it, sResult)
	InsertHistory_UpdateStatus(it, sResult, sWarningMsg, StatusCompleted, "", gpconn)

	return nil
}

func GPResourQueueWait(it MonitorSetup) error {
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
	var sResult string
	var sWarningMsg string
	row, err = QueryRow(gpconn, `select count(*) from gp_toolkit.gp_resq_activity where resqstatus = 'waiting';`)
	var cnt int
	if err = row.Scan(&cnt); err != nil {
		mylog.Error("Check Resource Queue queueing SQLs error: %v", err)
		Err_string := fmt.Sprintf("%v", err)
		InsertHistory_UpdateStatus(it, "", "", StatusFailed, Err_string, gpconn)
		return fmt.Errorf("Check Resource Queue queueing SQLs error: %w", err)
	}
	sResult = fmt.Sprintf("%d", cnt)
	sWarningMsg = fmt.Sprintf("数据库负载情况-资源队列中总等待数量: %d", cnt)
	mylog.Log(sWarningMsg)
	MonitorOutput(it, sResult)
	InsertHistory_UpdateStatus(it, sResult, sWarningMsg, StatusCompleted, "", gpconn)

	return nil
}

func GPIDLEinTransaction(it MonitorSetup) error {
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
	var sResult string
	var sWarningMsg string
	var checksql string
	if gpver == "gp4" || gpver == "gp5" {
		checksql = `select count(*) from pg_stat_activity where current_query='<IDLE> in transaction';`
	} else {
		checksql = `select count(*) from pg_stat_activity where state='idle in transaction';`
	}
	row, err = QueryRow(gpconn, checksql)
	var cnt int
	if err = row.Scan(&cnt); err != nil {
		mylog.Error("Check IDLE in transactions error: %v", err)
		Err_string := fmt.Sprintf("%v", err)
		InsertHistory_UpdateStatus(it, "", "", StatusFailed, Err_string, gpconn)
		return fmt.Errorf("Check IDLE in transactions error: %w", err)
	}
	sResult = fmt.Sprintf("%d", cnt)
	sWarningMsg = fmt.Sprintf("数据库负载情况-数据库中空事务的总数: %d", cnt)
	mylog.Log(sWarningMsg)
	MonitorOutput(it, sResult)
	InsertHistory_UpdateStatus(it, sResult, sWarningMsg, StatusCompleted, "", gpconn)

	return nil
}

func GPSQLRuningTime(it MonitorSetup) error {
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
	var sResult string
	var sWarningMsg string
	var checksql string
	if gpver == "gp4" || gpver == "gp5" {
		checksql = `select round(extract(EPOCH FROM now()-query_start)/60) 
        from pg_stat_activity where current_query not like '%IDLE%' order by query_start limit 1;`
	} else {
		checksql = `select round(extract(EPOCH FROM now()-query_start)/60) 
        from pg_stat_activity where state='active' order by query_start limit 1;`
	}
	row, err = QueryRow(gpconn, checksql)
	var running_time int
	if err = row.Scan(&running_time); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			sResult = "0"
			sWarningMsg = "数据库运行-当前没有SQL在运行"
			mylog.Log(sWarningMsg)
			MonitorOutput(it, sResult)
			InsertHistory_UpdateStatus(it, sResult, sWarningMsg, StatusCompleted, "", gpconn)
		} else {
			mylog.Error("Check SQL Running time error: %v", err)
			Err_string := fmt.Sprintf("%v", err)
			InsertHistory_UpdateStatus(it, "", "", StatusFailed, Err_string, gpconn)
			return fmt.Errorf("Check SQL Running time error: %w", err)
		}
	}
	sResult = fmt.Sprintf("%d", running_time)
	sWarningMsg = fmt.Sprintf("数据库负载情况-运行时间最长的SQL运行时长%d分钟", running_time)
	mylog.Log(sWarningMsg)
	MonitorOutput(it, sResult)
	InsertHistory_UpdateStatus(it, sResult, sWarningMsg, StatusCompleted, "", gpconn)

	return nil
}

func Check_session_process(it MonitorSetup) error {
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
	var sResult string
	var sWarningMsg string
	row, err = QueryRow(gpconn,
		`select sessionid,sess_proc_count from (
        	select sessionid,sess_proc_count,
        	ROW_NUMBER() OVER ( PARTITION BY sessionid ORDER BY sess_proc_count DESC) rn1
        	from (
            	select segid,sessionid,count(*) sess_proc_count from check_process_ext group by 1,2
            ) t1
        ) t2 where rn1=1
        order by sess_proc_count desc limit 1;`)
	var Sessid string
	var ProcCount int
	if err = row.Scan(&Sessid, &ProcCount); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			sResult = "0"
			sWarningMsg = "数据库负载情况-当前没有SQL在运行"
			mylog.Log(sWarningMsg)
			MonitorOutput(it, sResult)
			InsertHistory_UpdateStatus(it, sResult, sWarningMsg, StatusCompleted, "", gpconn)
		} else {
			mylog.Error("Check SQL Running time error: %v", err)
			Err_string := fmt.Sprintf("%v", err)
			InsertHistory_UpdateStatus(it, "", "", StatusFailed, Err_string, gpconn)
			return fmt.Errorf("Check SQL Running time error: %w", err)
		}
	}
	sResult = fmt.Sprintf("%d", ProcCount)
	sWarningMsg = fmt.Sprintf("会话%s目前进程数最多, 在单个实例子进程数%d个", Sessid, ProcCount)
	mylog.Log(sWarningMsg)
	MonitorOutput(it, sResult)
	InsertHistory_UpdateStatus(it, sResult, sWarningMsg, StatusCompleted, "", gpconn)

	return nil
}

func GPPerformance(it MonitorSetup) error {
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
	_, err = ExecSQL_timeout(Check_TimeOut_Sec*time.Second,
		`drop table if exists check_gpdb_testdata;
        create table check_gpdb_testdata (col1 int) distributed randomly;
        insert into check_gpdb_testdata select generate_series(1,500000) as t;
        select count(*) from check_gpdb_testdata;
        truncate check_gpdb_testdata;
        drop table if exists check_gpdb_testdata;`)
	if err != nil {
		if IsTimeout(err) {
			sResult = "1"
			sWarningMsg = "数据库运行SQL效率检测超时"
			mylog.Log(sWarningMsg)
			MonitorOutput(it, sResult)
			InsertHistory_UpdateStatus(it, sResult, sWarningMsg, StatusCompleted, "", gpconn)
		} else {
			mylog.Error("Check GP performance error: %v", err)
			Err_string := fmt.Sprintf("%v", err)
			InsertHistory_UpdateStatus(it, "", "", StatusFailed, Err_string, gpconn)
			return fmt.Errorf("Check GP performance error: %w", err)
		}
	}

	sResult = "0"
	sWarningMsg = "数据库运行SQL效率正常"
	mylog.Log(sWarningMsg)
	MonitorOutput(it, sResult)
	InsertHistory_UpdateStatus(it, sResult, sWarningMsg, StatusCompleted, "", gpconn)

	return nil
}

func GPCheckpoint(it MonitorSetup) error {
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
	_, err = ExecSQL_timeout(Check_TimeOut_Sec*time.Second, `checkpoint;`)
	if err != nil {
		if IsTimeout(err) {
			sResult = "1"
			sWarningMsg = "checkpoint执行检测超时"
			mylog.Log(sWarningMsg)
			MonitorOutput(it, sResult)
			InsertHistory_UpdateStatus(it, sResult, sWarningMsg, StatusCompleted, "", gpconn)
			return fmt.Errorf("GP checkpoint timeout: %w", err)
		} else {
			mylog.Error("Check GP checkpoint error: %v", err)
			Err_string := fmt.Sprintf("%v", err)
			InsertHistory_UpdateStatus(it, "", "", StatusFailed, Err_string, gpconn)
			return fmt.Errorf("Check GP checkpoint error: %w", err)
		}
	}

	sResult = "0"
	sWarningMsg = "checkpoint执行效率正常"
	mylog.Log(sWarningMsg)
	MonitorOutput(it, sResult)
	InsertHistory_UpdateStatus(it, sResult, sWarningMsg, StatusCompleted, "", gpconn)

	return nil
}
