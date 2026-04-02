package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"
	"time"
)

type CmdResult struct {
	Stdout string
	Stderr string
	Err    error
}

// DecryptWithGPG 解密文件，口令通过 stdin 传入，返回解密后的明文
func DecryptWithGPG(passphrase, encryptedFile string) (string, error) {
	cmd := exec.Command(
		"gpg",
		"--batch",
		"--yes",
		"--passphrase-fd", "0",
		"-a",
		"-o", "-", // 输出到 stdout
		encryptedFile, // 密文文件
	)

	// stdin = passphrase（等价于 echo passphrase | gpg ...）
	cmd.Stdin = bytes.NewBufferString(passphrase + "\n")

	// 丢弃 stderr（等价于 2>/dev/null）
	cmd.Stderr = io.Discard

	// 只读取 stdout
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return "", fmt.Errorf("stdout pipe failed: %v", err)
	}

	if err := cmd.Start(); err != nil {
		return "", fmt.Errorf("gpg start failed: %v", err)
	}

	outBytes, err := io.ReadAll(stdout)
	if err != nil {
		_ = cmd.Wait()
		return "", fmt.Errorf("read stdout failed: %v", err)
	}

	if err := cmd.Wait(); err != nil {
		return "", fmt.Errorf("gpg decrypt failed: %v", err)
	}

	return string(outBytes), nil
}

func ExecCmd(cmd string, args ...string) CmdResult {
	var stdout, stderr bytes.Buffer

	c := exec.Command(cmd, args...)
	c.Stdout = &stdout
	c.Stderr = &stderr

	err := c.Run()

	return CmdResult{
		Stdout: stdout.String(),
		Stderr: stderr.String(),
		Err:    err,
	}
}

func ExecCmdTimeout(timeout time.Duration, cmd string, args ...string) CmdResult {
	var stdout, stderr bytes.Buffer

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	c := exec.CommandContext(ctx, cmd, args...)
	c.Stdout = &stdout
	c.Stderr = &stderr

	err := c.Run()

	// 超时判断
	if ctx.Err() == context.DeadlineExceeded {
		return CmdResult{
			Stdout: stdout.String(),
			Stderr: stderr.String(),
			Err:    ctx.Err(),
		}
	}

	return CmdResult{
		Stdout: stdout.String(),
		Stderr: stderr.String(),
		Err:    err,
	}
}

func ExecBashTimeout(timeout time.Duration, script string) CmdResult {
	var stdout, stderr bytes.Buffer

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	c := exec.CommandContext(ctx, "/bin/bash", "-c", script)
	c.Stdout = &stdout
	c.Stderr = &stderr

	err := c.Run()

	if ctx.Err() == context.DeadlineExceeded {
		return CmdResult{
			Stdout: stdout.String(),
			Stderr: stderr.String(),
			Err:    ctx.Err(),
		}
	}

	return CmdResult{
		Stdout: stdout.String(),
		Stderr: stderr.String(),
		Err:    err,
	}
}
