package main

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func DBconnect(connString string, connectTimeout time.Duration) (*pgx.Conn, error) {
	if connectTimeout <= 0 {
		connectTimeout = 10 * time.Second
	}

	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()

	c, err := pgx.Connect(ctx, connString)
	if err != nil {
		mylog.Error("DB Connect error: %v", err)
		return nil, err
	}

	return c, nil
}

func DBclose(dbconn *pgx.Conn) error {
	if dbconn == nil {
		return nil
	}
	err := dbconn.Close(context.Background())
	dbconn = nil
	return err
}

func ExecSQL(dbconn *pgx.Conn, sql string, args ...any) (int64, error) {
	if dbconn == nil {
		return 0, fmt.Errorf("DB is not connected")
	}

	ct, err := dbconn.Exec(context.Background(), sql, args...)
	if err != nil {
		return 0, err
	}
	return ct.RowsAffected(), nil
}

func ExecSQL_timeout(timeout time.Duration, sql string, args ...any) (int64, error) {
	var newconn *pgx.Conn
	var err error
	//由于timeout，可能会导致connection被破坏，因此不能使用外部传入的conn指针
	//需要在函数内部自行连接数据库
	if newconn, err = DBconnect(gpconnString, 30*time.Second); err != nil {
		return 0, fmt.Errorf("DBConnect error: %w", err)
	}
	defer DBclose(newconn)

	if timeout <= 0 {
		return 0, fmt.Errorf("Invalid timeout")
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var ct pgconn.CommandTag
	ct, err = newconn.Exec(ctx, sql, args...)
	if err != nil {
		return 0, err
	}
	return ct.RowsAffected(), nil
}

func IsTimeout(err error) bool {
	return errors.Is(err, context.DeadlineExceeded)
}

func QueryRow(dbconn *pgx.Conn, sql string, args ...any) (pgx.Row, error) {
	if dbconn == nil {
		return nil, fmt.Errorf("DB is not connected")
	}
	return dbconn.QueryRow(context.Background(), sql, args...), nil
}

func QueryRows(dbconn *pgx.Conn, sql string, args ...any) (pgx.Rows, error) {
	if dbconn == nil {
		return nil, fmt.Errorf("DB is not connected")
	}
	return dbconn.Query(context.Background(), sql, args...)
}

func get_gpver() (string, error) {
	var gpconn *pgx.Conn
	var err error
	if gpconn, err = DBconnect(gpconnString, 30*time.Second); err != nil {
		return "", fmt.Errorf("DBConnect error: %w", err)
	}
	defer DBclose(gpconn)

	var ver_string string
	var row pgx.Row
	row, err = QueryRow(gpconn, `select version();`)
	if err = row.Scan(&ver_string); err != nil {
		mylog.Error("Check GP version error: %v", err)
		return "", fmt.Errorf("Check GP version error: %w", err)
	}
	if strings.Contains(ver_string, "Greenplum Database") {
		re := regexp.MustCompile(`Greenplum Database (\d+)\.`)
		m := re.FindStringSubmatch(ver_string)
		if len(m) < 2 {
			return "", fmt.Errorf("Cannot find GP version")
		}
		return "gp" + m[1], nil
	} else if strings.Contains(ver_string, "Apache Cloudberry") {
		re := regexp.MustCompile(`Apache Cloudberry (\d+)\.`)
		m := re.FindStringSubmatch(ver_string)
		if len(m) < 2 {
			return "", fmt.Errorf("Cannot find CBDB version")
		}
		return "cbdb" + m[1], nil
	} else {
		mylog.Error("Unknown GP version: %s", ver_string)
		return "", fmt.Errorf("Unknown GP version: %s", ver_string)
	}
}
