package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

func GPSegDown(it MonitorSetup) error {
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
	row, err = QueryRow(gpconn, `select count(*) from gp_segment_configuration where status = 'd'`)
	var cnt int
	if err = row.Scan(&cnt); err != nil {
		mylog.Error("Check GPSegment Down error: %v", err)
		Err_string := fmt.Sprintf("%v", err)
		InsertHistory_UpdateStatus(it, "", "", StatusFailed, Err_string, gpconn)
		return fmt.Errorf("Check GPSegment Down error: %w", err)
	}
	var sResult string
	var sWarningMsg string
	if cnt == 0 {
		sResult = "0"
		sWarningMsg = "数据库状态-没有实例处于Down状态"
	} else {
		sResult = "1"
		sWarningMsg = fmt.Sprintf("数据库状态-集群中有%d个实例处于Down状态", cnt)
	}
	mylog.Log(sWarningMsg)
	MonitorOutput(it, sResult)
	InsertHistory_UpdateStatus(it, sResult, sWarningMsg, StatusCompleted, "", gpconn)

	return nil
}

func GPSync(it MonitorSetup) error {
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
	row, err = QueryRow(gpconn, `select count(*) from gp_segment_configuration where mode <> 's' and content>=0;`)
	var cnt int
	if err = row.Scan(&cnt); err != nil {
		mylog.Error("Check GPSegment Sync error: %v", err)
		Err_string := fmt.Sprintf("%v", err)
		InsertHistory_UpdateStatus(it, "", "", StatusFailed, Err_string, gpconn)
		return fmt.Errorf("Check GPSegment Sync error: %w", err)
	}
	var sResult string
	var sWarningMsg string
	if cnt == 0 {
		sResult = "0"
		sWarningMsg = "数据库状态-数据库实例间同步状态正常"
	} else {
		sResult = "1"
		sWarningMsg = fmt.Sprintf("数据库状态-有%d个实例同步状态不正常", cnt)
	}
	mylog.Log(sWarningMsg)
	MonitorOutput(it, sResult)
	InsertHistory_UpdateStatus(it, sResult, sWarningMsg, StatusCompleted, "", gpconn)

	return nil
}

func GPRoleSwitch(it MonitorSetup) error {
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
	row, err = QueryRow(gpconn, `select count(*) from gp_segment_configuration where preferred_role<>role;`)
	var cnt int
	if err = row.Scan(&cnt); err != nil {
		mylog.Error("Check GPRole switch error: %v", err)
		Err_string := fmt.Sprintf("%v", err)
		InsertHistory_UpdateStatus(it, "", "", StatusFailed, Err_string, gpconn)
		return fmt.Errorf("Check GPRole switch error: %w", err)
	}
	var sResult string
	var sWarningMsg string
	if cnt == 0 {
		sResult = "0"
		sWarningMsg = "数据库状态-集群中没有实例角色发生切换"
	} else {
		sResult = "1"
		sWarningMsg = fmt.Sprintf("数据库状态-有%d个实例发生切换", cnt)
	}
	mylog.Log(sWarningMsg)
	MonitorOutput(it, sResult)
	InsertHistory_UpdateStatus(it, sResult, sWarningMsg, StatusCompleted, "", gpconn)

	return nil
}

func GPMasterAge(it MonitorSetup) error {
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
	row, err = QueryRow(gpconn, `select datname,age(datfrozenxid) age from pg_database order by 2 desc limit 1;`)
	var DatName string
	var Age int64
	if err = row.Scan(&DatName, &Age); err != nil {
		mylog.Error("Check GP Master age error: %v", err)
		Err_string := fmt.Sprintf("%v", err)
		InsertHistory_UpdateStatus(it, "", "", StatusFailed, Err_string, gpconn)
		return fmt.Errorf("Check GP Master age error: %w", err)
	}
	var sResult string
	var sWarningMsg string
	sResult = fmt.Sprintf("%d", Age)
	sWarningMsg = fmt.Sprintf("Master实例Age检查-Age最大的数据库是%s, Age为%d", DatName, Age)
	mylog.Log(sWarningMsg)
	MonitorOutput(it, sResult)
	InsertHistory_UpdateStatus(it, sResult, sWarningMsg, StatusCompleted, "", gpconn)

	return nil
}

func GPSegmentAge(it MonitorSetup) error {
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
	row, err = QueryRow(gpconn, `select datname,age(datfrozenxid) age from gp_dist_random('pg_database') order by 2 desc limit 1;`)
	var DatName string
	var Age int64
	if err = row.Scan(&DatName, &Age); err != nil {
		mylog.Error("Check GP Segment age error: %v", err)
		Err_string := fmt.Sprintf("%v", err)
		InsertHistory_UpdateStatus(it, "", "", StatusFailed, Err_string, gpconn)
		return fmt.Errorf("Check GP Segment age error: %w", err)
	}
	var sResult string
	var sWarningMsg string
	sResult = fmt.Sprintf("%d", Age)
	sWarningMsg = fmt.Sprintf("Segment实例Age检查-Age最大的数据库是%s, Age为%d", DatName, Age)
	mylog.Log(sWarningMsg)
	MonitorOutput(it, sResult)
	InsertHistory_UpdateStatus(it, sResult, sWarningMsg, StatusCompleted, "", gpconn)

	return nil
}

func GPStateE(it MonitorSetup) error {
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
	var cmdres CmdResult
	cmdres = ExecCmd("gpstate", "-e")
	if cmdres.Err != nil {
		mylog.Error("gpstate -e error: %v", cmdres.Err)
		mylog.Raw("%s", cmdres.Stdout)
		Err_string := fmt.Sprintf("gpstate -e error: %v", cmdres.Err)
		sResult = "1"
		sWarningMsg = "数据库状态-segment实例状态不正常"
		mylog.Log(sWarningMsg)
		MonitorOutput(it, sResult)
		InsertHistory_UpdateStatus(it, "", "", StatusFailed, Err_string, gpconn)
		return fmt.Errorf("gpstate -e error: %w", cmdres.Err)
	}

	if strings.Contains(cmdres.Stdout, "All segments are running normally") {
		sResult = "0"
		sWarningMsg = "数据库状态-segment实例状态正常"
	} else {
		sResult = "1"
		sWarningMsg = "数据库状态-segment实例状态不正常"
	}
	mylog.Log(sWarningMsg)
	MonitorOutput(it, sResult)
	InsertHistory_UpdateStatus(it, sResult, sWarningMsg, StatusCompleted, "", gpconn)

	return nil
}

func GPStateF(it MonitorSetup) error {
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
	var cmdres CmdResult
	cmdres = ExecCmd("gpstate", "-f")
	if cmdres.Err != nil {
		mylog.Error("gpstate -f error: %v", cmdres.Err)
		mylog.Raw("%s", cmdres.Stdout)
		Err_string := fmt.Sprintf("gpstate -f error: %v", cmdres.Err)
		sResult = "1"
		sWarningMsg = "数据库状态-主备master同步状态不正常"
		mylog.Log(sWarningMsg)
		MonitorOutput(it, sResult)
		InsertHistory_UpdateStatus(it, "", "", StatusFailed, Err_string, gpconn)
		return fmt.Errorf("gpstate -f error: %w", cmdres.Err)
	}

	if strings.Contains(cmdres.Stdout, "Sync state: sync") {
		sResult = "0"
		sWarningMsg = "数据库状态-主备master同步状态正常"
	} else {
		sResult = "1"
		sWarningMsg = "数据库状态-主备master同步状态不正常"
	}
	mylog.Log(sWarningMsg)
	MonitorOutput(it, sResult)
	InsertHistory_UpdateStatus(it, sResult, sWarningMsg, StatusCompleted, "", gpconn)

	return nil
}
