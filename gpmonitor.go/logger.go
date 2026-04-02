package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Logger struct {
	mu         sync.Mutex
	file       *os.File
	currentDay string
	logPath    string
}

// 初始化（只传目录）
func InitLogger(logPath string) (*Logger, error) {
	l := &Logger{
		logPath: logPath,
	}
	if err := l.rotateIfNeeded(); err != nil {
		return nil, err
	}
	return l, nil
}

// 核心：判断是否需要切换日志
func (l *Logger) rotateIfNeeded() error {
	today := time.Now().Format("20060102")

	// 没变就不用动
	if l.currentDay == today && l.file != nil {
		return nil
	}

	// 关闭旧文件
	if l.file != nil {
		_ = l.file.Close()
	}

	// 创建新文件
	filename := filepath.Join(l.logPath, "gpmonitor_"+today+".log")

	f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	l.file = f
	l.currentDay = today
	return nil
}

func (l *Logger) FormatLog(level string, format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// 每次写之前检查是否需要切换
	if err := l.rotateIfNeeded(); err != nil {
		fmt.Println("rotate log failed:", err)
		return
	}

	msg := fmt.Sprintf(format, args...)

	line := fmt.Sprintf("%s [%s] %s\n",
		time.Now().Format("2006-01-02 15:04:05.000000"),
		level,
		msg)

	// 输出到终端
	fmt.Print(line)

	// 写文件
	if l.file != nil {
		if _, err := l.file.WriteString(line); err != nil {
			fmt.Println("write log file failed:", err)
		}
	}
}

func (l *Logger) PlainLog(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// 每次写之前检查是否需要切换
	if err := l.rotateIfNeeded(); err != nil {
		fmt.Println("rotate log failed:", err)
		return
	}

	line := fmt.Sprintf(format, args...) + "\n"

	// 输出到终端
	fmt.Print(line)

	// 写文件
	if l.file != nil {
		if _, err := l.file.WriteString(line); err != nil {
			fmt.Println("write log file failed:", err)
		}
	}
}

func (l *Logger) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file != nil {
		_ = l.file.Close()
		l.file = nil
	}
}

func (l *Logger) Log(format string, args ...any) {
	l.FormatLog("LOG", format, args...)
}

func (l *Logger) Warn(format string, args ...any) {
	l.FormatLog("WARNING", format, args...)
}

func (l *Logger) Error(format string, args ...any) {
	l.FormatLog("ERROR", format, args...)
}

func (l *Logger) Raw(format string, args ...any) {
	l.PlainLog(format, args...)
}
