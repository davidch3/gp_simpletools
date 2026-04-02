package main

import (
	"fmt"
	"os"
	"path/filepath"
)

func MonitorOutput(it MonitorSetup, sResult string) error {
	var err error

	err = WriteMetricTextFile(Cfg.GPmon_Path, it.MetricName, sResult)
	if err != nil {
		return fmt.Errorf("Output failed: %w", err)
	}
	return nil

}

func WriteMetricTextFile(outputDir, metricName, content string) error {
	if outputDir == "" {
		return fmt.Errorf("outputDir is empty")
	}

	finalFile := filepath.Join(outputDir, metricName+".txt")
	tmpFile := filepath.Join(outputDir, metricName+".txt.tmp")

	// Write temp file
	if err := os.WriteFile(tmpFile, []byte(content), 0o644); err != nil {
		return fmt.Errorf("Write to temp file failed: %w", err)
	}

	// Rename to finalfile
	if err := os.Rename(tmpFile, finalFile); err != nil {
		_ = os.Remove(tmpFile)
		return fmt.Errorf("Rename temp to final failed: %w", err)
	}

	return nil
}
