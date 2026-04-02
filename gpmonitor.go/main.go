package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

type DBConnInfo struct {
	hostname string
	portno   string
	dbname   string
	username string
	passwd   string
}

const Check_TimeOut_Sec = 300  //time.Second
const Del_Hisdata_interv = 180 //days

var gpconnString string
var Cfg *Config
var mylog *Logger
var gpver string

// 调用各个检查函数
func DispatchAndRun(it MonitorSetup) error {
	switch it.MetricName {
	case "gpdb_segment_state":
		return GPStateE(it)
	case "gpdb_standby_master_state":
		return GPStateF(it)
	case "gpdb_segment_down":
		return GPSegDown(it)
	case "gpdb_primary_mirror_sync_state":
		return GPSync(it)
	case "gpdb_segment_role_switch":
		return GPRoleSwitch(it)
	case "gpdb_workfile_usage_per_query":
		return GPTempSize(it)
	case "gpdb_check_lock_tables":
		return GPCheckTableLock(it)
	case "gpdb_check_network_flooding":
		return GPCheckNetworkFlooding(it)
	case "gpdb_total_sql":
		return GPLoadRunSql(it)
	case "gpdb_total_waiting_sql":
		return GPLoadWaitSql(it)
	case "gpdb_total_connections":
		return GPConnections(it)
	case "gpdb_resource_group_running_sql":
		return GPResourGroupRun(it)
	case "gpdb_resource_group_waiting_sql":
		return GPResourGroupWait(it)
	case "gpdb_total_idle_in_transaction":
		return GPIDLEinTransaction(it)
	case "gpdb_long_sql_running_time":
		return GPSQLRuningTime(it)
	case "gpdb_check_session_processes":
		return Check_session_process(it)
	case "gpdb_performance_test":
		return GPPerformance(it)
	case "gpdb_checkpoint_performance":
		return GPCheckpoint(it)
	case "gpdb_check_master_age":
		return GPMasterAge(it)
	case "gpdb_check_segment_age":
		return GPSegmentAge(it)
	case "gpdb_check_master_panic":
		return GPMasPanic(it)
	case "gpdb_check_segment_panic":
		return GPSegsPanic(it)
	case "gpdb_check_internal_error":
		return GPInternalError(it)
	case "gpdb_check_oom":
		return GPOutOfMemory(it)
	case "gpdb_resource_queue_running_sql":
		return GPResourQueueRun(it)
	case "gpdb_resource_queue_waiting_sql":
		return GPResourQueueWait(it)
	case "monitor_maintenance_task":
		return Monitor_Maintenance(it) //每天执行一次monitor表的维护
	default:
		// 未实现的 monitor_id：不算失败，记录一下即可
		mylog.Log("unknown MonitorID: %d MetricName: %s", it.MonitorID, it.MetricName)
		return nil
	}
}

func startWorkers(ctx context.Context, workerNum int, jobCh <-chan MonitorSetup) {
	for i := 0; i < workerNum; i++ {
		go func(workerID int) {
			for {
				select {
				case <-ctx.Done():
					return
				case it := <-jobCh:
					func(item MonitorSetup) {
						defer func() {
							if r := recover(); r != nil {
								fmt.Println("panic in worker:", workerID, r)
							}
						}()
						_ = DispatchAndRun(item) // 这里调用检查分派逻辑
					}(it)
				}
			}
		}(i)
	}
}

// 主循环：永远跑，直到 ctx 被取消（收到退出信号）
func mainLoop(ctx context.Context, itemlist []MonitorSetup, interval time.Duration, maxWorkers int) {
	jobCh := make(chan MonitorSetup, maxWorkers)
	startWorkers(ctx, maxWorkers, jobCh)

	for {
		// A) 一轮：依次处理每个监控项
		for _, it := range itemlist {
			// 如果被要求退出，就立刻结束
			select {
			case <-ctx.Done():
				return
			case jobCh <- it:
			}
		}

		// B) 一轮做完，等待interval后下一轮
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}

func main() {
	var err error
	// Load configuration
	Cfg, err = LoadConfig("monitor.conf")
	if err != nil {
		fmt.Println("[ERROR] Load config file failed: ", err)
		os.Exit(2)
	}
	fmt.Println("GPlogon_file: ", Cfg.GPlogon_file)
	fmt.Println("Cluster_Name: ", Cfg.Cluster_Name)
	fmt.Println("GPmon_Path: ", Cfg.GPmon_Path)
	fmt.Println("Log_Path: ", Cfg.Log_Path)
	fmt.Println("maxWorkers: ", Cfg.maxWorkers)
	_, err = os.Stat(Cfg.GPmon_Path)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println("目录不存在：", Cfg.GPmon_Path, err)
		} else if os.IsPermission(err) {
			fmt.Println("目录权限不足：", Cfg.GPmon_Path, err)
		} else {
			fmt.Println("其他错误:", err)
		}
		os.Exit(2)
	}
	_, err = os.Stat(Cfg.Log_Path)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println("目录不存在：", Cfg.Log_Path, err)
		} else if os.IsPermission(err) {
			fmt.Println("目录权限不足：", Cfg.Log_Path, err)
		} else {
			fmt.Println("其他错误:", err)
		}
		os.Exit(2)
	}
	mylog, err = InitLogger(Cfg.Log_Path)
	if err != nil {
		fmt.Println("Init logger failed:", err)
		os.Exit(2)
	}
	defer mylog.Close()

	mylog.Raw("=================================")
	mylog.Raw("GP Monitor Start")
	mylog.Raw("=================================")
	// Load GP connection info
	conn := &DBConnInfo{}
	connstr, err := DecryptWithGPG("gpdb_pwd_key", Cfg.GPlogon_file)
	if err != nil {
		mylog.Error("Decrypt error: %v", err)
		os.Exit(2)
	}
	//mylog.Log("Decrypt Info: %s", connstr)
	connpart := strings.Split(connstr, ":")
	if len(connpart) != 5 {
		mylog.Error("Invalid DB connection config, please check ./gplogon")
		os.Exit(2)
	}
	conn.hostname = connpart[0]
	conn.portno = connpart[1]
	conn.dbname = connpart[2]
	conn.username = connpart[3]
	conn.passwd = strings.Trim(connpart[4], "\r\n")
	gpconnString = fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s sslmode=disable application_name=gpmonitor",
		conn.hostname, conn.portno, conn.dbname, conn.username, conn.passwd)
	//mylog.Log("GP Connect String: %s", gpconnString)

	gpver, err = get_gpver()
	if err != nil {
		mylog.Error("%v", err)
		os.Exit(2)
	}
	mylog.Log("GP Version is: %s", gpver)

	var itemlist []MonitorSetup
	mylog.Log("Load monitor_setup...")
	itemlist, err = LoadMonitorSetups()
	if err != nil {
		mylog.Error("Load MonitorSetup: %v", err)
		os.Exit(2)
	}
	for _, it := range itemlist {
		mylog.Raw("%d,%s,%s,%d,%v,%s,%s",
			it.MonitorID, it.MetricName, it.Description, it.CheckInterval, it.IfWarning, it.WarningValue, it.Memo)
	}

	mylog.Log("Reset monitor_setup status...")
	err = ResetMonitorStatus()
	if err != nil {
		mylog.Error("Reset MonitorStatus: %v", err)
		os.Exit(2)
	}

	_ = UnloadHostfile()

	// 创建一个“可取消”的 ctx：当收到 Ctrl+C 或 kill(SIGTERM) 时会自动取消
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop() // 释放 signal 相关资源
	// 进入主循环：每轮执行所有监控项，轮与轮之间有时间间隔
	mainLoop(ctx, itemlist, 60*time.Second, Cfg.maxWorkers)

}
