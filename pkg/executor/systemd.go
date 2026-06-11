package executor

import (
	"fmt"
	"os"

	"sshot/pkg/types"
)

// executeSystemd 执行 systemd 服务管理
func (e *Executor) executeSystemd(systemdTask *types.SystemdTask) (string, error) {
	writer := e.OutputWriter
	if writer == nil {
		writer = os.Stdout
	}

	serviceName := e.SubstituteVars(systemdTask.Name)
	state := systemdTask.State
	enabled := systemdTask.Enabled
	daemon := systemdTask.Daemon

	if types.ExecOptions.Verbose {
		e.mu.Lock()
		fmt.Fprintf(writer, "    │ Systemd service: %s\n", serviceName)
		fmt.Fprintf(writer, "    │ Desired state: %s\n", state)
		if enabled {
			fmt.Fprintf(writer, "    │ Enable at boot: yes\n")
		}
		if daemon {
			fmt.Fprintf(writer, "    │ Daemon reload: yes\n")
		}
		e.mu.Unlock()
	}

	var output string
	var err error

	// 检查服务是否存在
	checkCmd := fmt.Sprintf("systemctl list-unit-files | grep -q '^%s.service'", serviceName)
	if _, err := e.executeCommand(checkCmd, true); err != nil {
		return "", fmt.Errorf("service %s does not exist", serviceName)
	}

	// 如果需要 daemon-reload
	if daemon {
		if types.ExecOptions.Verbose {
			e.mu.Lock()
			fmt.Fprintf(writer, "    │ Running systemctl daemon-reload\n")
			e.mu.Unlock()
		}
		if _, err := e.executeCommand("systemctl daemon-reload", true); err != nil {
			return "", fmt.Errorf("failed to reload systemd daemon: %w", err)
		}
	}

	// 根据状态执行操作
	switch state {
	case "started":
		output, err = e.systemdStart(serviceName)
	case "stopped":
		output, err = e.systemdStop(serviceName)
	case "restarted":
		output, err = e.systemdRestart(serviceName)
	case "reloaded":
		output, err = e.systemdReload(serviceName)
	default:
		return "", fmt.Errorf("unknown state: %s (supported: started, stopped, restarted, reloaded)", state)
	}

	if err != nil {
		return output, err
	}

	// 设置开机自启
	if enabled {
		if types.ExecOptions.Verbose {
			e.mu.Lock()
			fmt.Fprintf(writer, "    │ Enabling service at boot\n")
			e.mu.Unlock()
		}
		if _, err := e.executeCommand(fmt.Sprintf("systemctl enable %s", serviceName), true); err != nil {
			return output, fmt.Errorf("service %s %s but failed to enable: %w", serviceName, state, err)
		}
		output += fmt.Sprintf(" and enabled at boot")
	}

	return output, nil
}

// systemdStart 启动服务
func (e *Executor) systemdStart(serviceName string) (string, error) {
	// 检查服务是否已启动
	checkCmd := fmt.Sprintf("systemctl is-active --quiet %s", serviceName)
	if _, err := e.executeCommand(checkCmd, true); err == nil {
		return fmt.Sprintf("Service %s is already running", serviceName), nil
	}

	// 启动服务
	startCmd := fmt.Sprintf("systemctl start %s", serviceName)
	if _, err := e.executeCommand(startCmd, true); err != nil {
		return "", fmt.Errorf("failed to start service %s: %w", serviceName, err)
	}

	// 验证服务已启动
	if _, err := e.executeCommand(checkCmd, true); err != nil {
		return "", fmt.Errorf("service %s started but verification failed: %w", serviceName, err)
	}

	return fmt.Sprintf("Started service %s", serviceName), nil
}

// systemdStop 停止服务
func (e *Executor) systemdStop(serviceName string) (string, error) {
	// 检查服务是否已停止
	checkCmd := fmt.Sprintf("systemctl is-active --quiet %s", serviceName)
	if _, err := e.executeCommand(checkCmd, true); err == nil {
		// 服务正在运行，需要停止
		stopCmd := fmt.Sprintf("systemctl stop %s", serviceName)
		if _, err := e.executeCommand(stopCmd, true); err != nil {
			return "", fmt.Errorf("failed to stop service %s: %w", serviceName, err)
		}
		return fmt.Sprintf("Stopped service %s", serviceName), nil
	}

	return fmt.Sprintf("Service %s is already stopped", serviceName), nil
}

// systemdRestart 重启服务
func (e *Executor) systemdRestart(serviceName string) (string, error) {
	restartCmd := fmt.Sprintf("systemctl restart %s", serviceName)
	if _, err := e.executeCommand(restartCmd, true); err != nil {
		return "", fmt.Errorf("failed to restart service %s: %w", serviceName, err)
	}

	// 验证服务已启动
	checkCmd := fmt.Sprintf("systemctl is-active --quiet %s", serviceName)
	if _, err := e.executeCommand(checkCmd, true); err != nil {
		return "", fmt.Errorf("service %s restarted but verification failed: %w", serviceName, err)
	}

	return fmt.Sprintf("Restarted service %s", serviceName), nil
}

// systemdReload 重载服务配置
func (e *Executor) systemdReload(serviceName string) (string, error) {
	// 首先尝试 reload，如果失败则 restart
	reloadCmd := fmt.Sprintf("systemctl reload %s", serviceName)
	if _, err := e.executeCommand(reloadCmd, true); err == nil {
		return fmt.Sprintf("Reloaded service %s", serviceName), nil
	}

	// reload 失败，尝试 restart
	if types.ExecOptions.Verbose {
		e.mu.Lock()
		fmt.Printf("    │ Reload failed, attempting restart\n")
		e.mu.Unlock()
	}
	return e.systemdRestart(serviceName)
}
