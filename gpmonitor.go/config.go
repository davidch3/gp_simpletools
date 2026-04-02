package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"

	//	"strconv"
	"strings"
)

type Config struct {
	GPlogon_file string
	Cluster_Name string
	GPmon_Path   string
	Log_Path     string
	maxWorkers   int
}

// LoadConfig 从配置文件读取配置
func LoadConfig(path string) (*Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	cfg := &Config{}

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// 跳过空行和注释
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// key = value
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid config line: %s", line)
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		switch key {
		case "GPlogon_file":
			cfg.GPlogon_file = value
		case "Cluster_Name":
			cfg.Cluster_Name = value
		case "GPmon_Path":
			cfg.GPmon_Path = value
		case "Log_Path":
			cfg.Log_Path = value
		case "maxWorkers":
			cfg.maxWorkers, _ = strconv.Atoi(value)
		default:
			// 未知配置项，通常忽略或报警
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return cfg, nil
}
