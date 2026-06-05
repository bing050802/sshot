package executor

import (
	"bufio"
	"bytes"
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fgouteroux/sshot/pkg/types"
	"github.com/fgouteroux/sshot/pkg/utils"
	"golang.org/x/crypto/ssh/agent"
	"golang.org/x/crypto/ssh/knownhosts"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type Executor struct {
	Host           types.Host
	client         *ssh.Client
	Variables      map[string]interface{}
	Registers      map[string]string
	CompletedTasks map[string]bool
	GroupName      string
	mu             sync.Mutex
	OutputWriter   io.Writer
	StartTime      time.Time

	privilegedSess *ssh.Session
	privilegedIn   io.WriteCloser
	privilegedOut  io.Reader
	privilegedErr  io.Reader
	privMu         sync.Mutex // 保护特权 session 的互斥锁
}

// Singleton SSH agent client to avoid connection exhaustion
var (
	sshAgentOnce   sync.Once
	sshAgentClient agent.ExtendedAgent
)

// getPrivilegedSession 获取或创建特权 session（调用者必须持有 privMu 锁）
func (e *Executor) getPrivilegedSession() (*ssh.Session, io.WriteCloser, io.Reader, error) {
	// 注意：这个函数假设 privMu 已经被锁定
	// 如果已有 session 且仍然活跃，直接返回
	if e.privilegedSess != nil && e.privilegedIn != nil {
		// 检查 session 是否还活着
		if _, err := e.privilegedIn.Write([]byte("echo 'alive'\n")); err == nil {
			return e.privilegedSess, e.privilegedIn, e.privilegedOut, nil
		}
		// session 已死，关闭并重新创建
		e.privilegedSess.Close()
	}

	// 创建新的特权 session
	sess, err := e.client.NewSession()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create session: %w", err)
	}

	stdin, err := sess.StdinPipe()
	if err != nil {
		sess.Close()
		return nil, nil, nil, fmt.Errorf("failed to get stdin pipe: %w", err)
	}

	stdout, err := sess.StdoutPipe()
	if err != nil {
		sess.Close()
		return nil, nil, nil, fmt.Errorf("failed to get stdout pipe: %w", err)
	}

	stderr, err := sess.StderrPipe()
	if err != nil {
		sess.Close()
		return nil, nil, nil, fmt.Errorf("failed to get stderr pipe: %w", err)
	}
	_ = stderr // 暂不使用

	// 启动 bash
	if err := sess.Start("/bin/bash"); err != nil {
		sess.Close()
		return nil, nil, nil, fmt.Errorf("failed to start bash: %w", err)
	}

	// 如果需要切换用户（如 Ubuntu 20.04）
	if e.Host.BecomeUser != "" && e.Host.BecomePass != "" {
		switchCmd := fmt.Sprintf("sudo -S -u %s /bin/bash\n", e.Host.BecomeUser)
		if _, err := stdin.Write([]byte(switchCmd)); err != nil {
			sess.Close()
			return nil, nil, nil, fmt.Errorf("failed to switch user: %w", err)
		}
		if _, err := stdin.Write([]byte(e.Host.BecomePass + "\n")); err != nil {
			sess.Close()
			return nil, nil, nil, fmt.Errorf("failed to send sudo password: %w", err)
		}

		// 等待切换完成
		time.Sleep(500 * time.Millisecond)

		// 读取并丢弃欢迎信息
		buf := make([]byte, 4096)
		stdout.Read(buf)
	}

	// 缓存 session
	e.privilegedSess = sess
	e.privilegedIn = stdin
	e.privilegedOut = stdout

	return sess, stdin, stdout, nil
}

// executeCommandWithPrivilege 使用特权 session 执行命令（用于需要 sudo 的场景）
func (e *Executor) executeCommandWithPrivilege(cmd string) (string, error) {
	e.privMu.Lock()
	defer e.privMu.Unlock()

	_, stdin, _, err := e.getPrivilegedSession()
	if err != nil {
		return "", err
	}

	// 使用临时文件捕获输出
	tmpFile := fmt.Sprintf("/tmp/.priv_out_%d_%d", time.Now().UnixNano(), os.Getpid())
	wrappedCmd := fmt.Sprintf("%s > '%s' 2>&1; echo $? >> '%s'; cat '%s'; rm -f '%s'\n",
		cmd, tmpFile, tmpFile, tmpFile, tmpFile)

	if _, err := stdin.Write([]byte(wrappedCmd)); err != nil {
		return "", fmt.Errorf("failed to write command: %w", err)
	}

	// 等待命令执行
	time.Sleep(100 * time.Millisecond)

	// 读取结果
	result, err := e.executeCommandWithNewSession(fmt.Sprintf("cat '%s' 2>/dev/null", tmpFile))
	e.executeCommandWithNewSession(fmt.Sprintf("rm -f '%s'", tmpFile))

	if err != nil {
		return result, err
	}

	// 解析退出码
	result = strings.TrimSpace(result)
	lines := strings.Split(result, "\n")
	if len(lines) > 0 {
		lastLine := strings.TrimSpace(lines[len(lines)-1])
		if code, err := strconv.Atoi(lastLine); err == nil && code != 0 {
			output := strings.Join(lines[:len(lines)-1], "\n")
			return output, fmt.Errorf("exit status %d", code)
		}
		if len(lines) > 1 {
			result = strings.Join(lines[:len(lines)-1], "\n")
		} else {
			result = ""
		}
	}

	return result, nil
}

func (e *Executor) CollectFacts(factsConfig types.FactsConfig) error {
	writer := e.OutputWriter
	if writer == nil {
		writer = os.Stdout
	}

	if types.ExecOptions.Verbose {
		fmt.Fprintf(writer, "%s│%s Starting facts collection with %d collectors\n",
			utils.Color(utils.ColorCyan), utils.Color(utils.ColorReset), len(factsConfig.Collectors))
	}

	if e.Variables == nil {
		e.Variables = make(map[string]interface{})
	}

	for _, collector := range factsConfig.Collectors {
		fmt.Fprintf(writer, "  %s⚙%s Collecting facts: %s\n",
			utils.Color(utils.ColorCyan), utils.Color(utils.ColorReset), collector.Name)

		var output string
		var err error

		if types.ExecOptions.DryRun {
			output = `{"simulated": "data", "dry_run": true}`
		} else {
			output, err = e.executeCommand(collector.Command, collector.Sudo)
			if err != nil {
				return fmt.Errorf("failed to collect facts with %s: %w", collector.Name, err)
			}
		}

		// 尝试解析为 JSON
		var factData map[string]interface{}
		if err := json.Unmarshal([]byte(output), &factData); err != nil {
			// 如果不是 JSON，作为字符串存储
			e.Variables[collector.Name] = output
			if types.ExecOptions.Verbose {
				fmt.Fprintf(writer, "    %s→%s Stored as string (not JSON)\n",
					utils.Color(utils.ColorGray), utils.Color(utils.ColorReset))
			}
		} else {
			// 存储 JSON 结构
			e.Variables[collector.Name] = factData

			// 扁平化存储便于直接访问
			flattened := FlattenMap(factData, collector.Name+".")
			for k, v := range flattened {
				e.Variables[k] = v
			}

			if types.ExecOptions.Verbose {
				fmt.Fprintf(writer, "    %s→%s Collected %d fact entries\n",
					utils.Color(utils.ColorGray), utils.Color(utils.ColorReset), len(flattened))
			}
		}
	}

	return nil
}

// Add the missing flattenMap function
func FlattenMap(data map[string]interface{}, prefix string) map[string]string {
	items := make(map[string]string)

	for k, v := range data {
		key := prefix + k

		switch typedVal := v.(type) {
		case map[string]interface{}:
			// Recursively flatten nested maps
			subItems := FlattenMap(typedVal, key+".")
			for sk, sv := range subItems {
				items[sk] = sv
			}

		case []interface{}:
			// Convert arrays to JSON strings
			if jsonBytes, err := json.Marshal(typedVal); err == nil {
				items[key] = string(jsonBytes)
			}

		case string, float64, bool, int:
			// Convert basic types to strings
			items[key] = fmt.Sprintf("%v", typedVal)
		}
	}

	return items
}

func (e *Executor) ExecuteTask(task types.Task) error {

	if e.Registers == nil {
		e.Registers = make(map[string]string)
	}
	if e.Variables == nil {
		e.Variables = make(map[string]interface{})
	}
	if e.CompletedTasks == nil {
		e.CompletedTasks = make(map[string]bool)
	}

	writer := e.OutputWriter
	if writer == nil {
		writer = os.Stdout
	}

	if types.ExecOptions.Verbose {
		e.mu.Lock()
		log.SetOutput(writer)
		log.Printf("[VERBOSE] [%s] Executing task: %s", e.Host.Name, task.Name)
		log.SetOutput(os.Stderr)
		e.mu.Unlock()
	}

	// Check if task is restricted to specific groups
	if len(task.OnlyGroups) > 0 {
		groupAllowed := false
		for _, group := range task.OnlyGroups {
			if group == e.GroupName {
				groupAllowed = true
				break
			}
		}
		if !groupAllowed {
			// Skip task, not in allowed groups
			fmt.Fprintf(writer, "  ⊘ Skipped (not in allowed groups: %v)\n", task.OnlyGroups)
			return nil
		}
	}

	// Check if task should skip specific groups
	if len(task.SkipGroups) > 0 {
		for _, group := range task.SkipGroups {
			if group == e.GroupName {
				// Skip task, in excluded group
				fmt.Fprintf(writer, "  ⊘ Skipped (in excluded group: %s)\n", group)
				return nil
			}
		}
	}

	// Check for delegation - if this task is delegated to a different host,
	// skip it unless we're the delegated host
	if task.DelegateTo != "" && task.DelegateTo != e.Host.Name && task.DelegateTo != "localhost" {
		e.mu.Lock()
		fmt.Fprintf(writer, "  ↷ Skipped (delegated to: %s)\n", task.DelegateTo)
		e.CompletedTasks[task.Name] = true
		e.mu.Unlock()
		return nil
	}

	// If task is delegated to localhost, treat it as a local_action
	if task.DelegateTo == "localhost" && task.Command != "" {
		// Convert to local_action if delegated to localhost
		task.LocalAction = task.Command
		task.Command = ""
	}

	// Check if this is a run_once task that's already been executed
	if task.RunOnce {
		taskKey := task.Name
		types.RunOnceTasks.RLock()
		executed := types.RunOnceTasks.Executed[taskKey]
		types.RunOnceTasks.RUnlock()

		if executed {
			e.mu.Lock()
			fmt.Fprintf(writer, "  ↷ Skipped (run_once already executed)\n")
			e.CompletedTasks[task.Name] = true
			e.mu.Unlock()
			return nil
		}

		// Mark as executed after checking but before actually running
		// to prevent race conditions in parallel execution
		types.RunOnceTasks.Lock()
		types.RunOnceTasks.Executed[taskKey] = true
		types.RunOnceTasks.Unlock()
	}

	if len(task.DependsOn) > 0 {
		for _, dep := range task.DependsOn {
			if !e.CompletedTasks[dep] {
				return fmt.Errorf("dependency not met: task '%s' depends on '%s' which has not completed", task.Name, dep)
			}
		}
	}

	if task.Vars != nil {
		for k, v := range task.Vars {
			e.Variables[k] = v
		}
	}

	if task.When != "" {
		if types.ExecOptions.Verbose {
			e.mu.Lock()
			log.SetOutput(writer)
			log.Printf("[VERBOSE] [%s] Evaluating condition: %s", e.Host.Name, task.When)
			log.SetOutput(os.Stderr)
			e.mu.Unlock()
		}
		if !e.evaluateCondition(task.When) {
			e.mu.Lock()
			fmt.Fprintf(writer, "  ⊘ Skipped (when: %s)\n", task.When)
			e.CompletedTasks[task.Name] = true
			e.mu.Unlock()
			return nil
		}
	}

	var output string
	var err error

	if types.ExecOptions.DryRun {
		e.mu.Lock()
		fmt.Fprintf(writer, "  🔍 DRY-RUN: Would execute\n")

		// Special handling for delegation
		if task.DelegateTo != "" && task.DelegateTo != e.Host.Name && task.DelegateTo != "localhost" {
			fmt.Fprintf(writer, "      Command: %s\n", e.SubstituteVars(task.Command))
			fmt.Fprintf(writer, "      (would be skipped, delegated to: %s)\n", task.DelegateTo)
			e.CompletedTasks[task.Name] = true
			e.mu.Unlock()
			return nil
		}

		switch {
		case task.Command != "" && task.DelegateTo != "":
			fmt.Fprintf(writer, "      Command: %s (delegated to: %s)\n",
				e.SubstituteVars(task.Command), task.DelegateTo)
			if task.RunOnce {
				fmt.Fprintf(writer, "      (run once)\n")
			}
		case task.Command != "":
			fmt.Fprintf(writer, "      Command: %s\n", e.SubstituteVars(task.Command))
		case task.Shell != "":
			fmt.Fprintf(writer, "      Shell: %s\n", e.SubstituteVars(task.Shell))
		case task.Script != "":
			fmt.Fprintf(writer, "      Script: %s\n", task.Script)
		case task.LocalAction != "":
			fmt.Fprintf(writer, "      Local Action: %s\n", e.SubstituteVars(task.LocalAction))
			if task.RunOnce {
				fmt.Fprintf(writer, "      (run once)\n")
			}
		case task.Copy != nil:
			fmt.Fprintf(writer, "      Copy: %s → %s\n", task.Copy.Src, e.SubstituteVars(task.Copy.Dest))
		}
		if task.Sudo {
			fmt.Fprintf(writer, "      (with sudo)\n")
		}

		if len(task.AllowedExitCodes) > 0 {
			fmt.Fprintf(writer, "      Allowed exit codes: %v\n", task.AllowedExitCodes)
		}

		if len(task.DependsOn) > 0 {
			fmt.Fprintf(writer, "      Dependencies: %v\n", task.DependsOn)
		}
		if task.Retries > 0 {
			fmt.Fprintf(writer, "      Retries: %d (delay: %ds)\n", task.Retries, task.RetryDelay)
		}
		if task.Timeout > 0 {
			fmt.Fprintf(writer, "      Timeout: %ds\n", task.Timeout)
		}
		e.CompletedTasks[task.Name] = true
		e.mu.Unlock()
		return nil
	}

	// Execute with retry logic
	retries := task.Retries
	if retries == 0 && task.UntilSuccess {
		retries = 60 // Default max retries for until_success
	}
	retryDelay := time.Duration(task.RetryDelay) * time.Second
	if retryDelay == 0 && retries > 0 {
		retryDelay = 5 * time.Second // Default retry delay
	}

	timeout := time.Duration(task.Timeout) * time.Second
	var timeoutTimer *time.Timer
	var timeoutChan <-chan time.Time
	if timeout > 0 {
		timeoutTimer = time.NewTimer(timeout)
		timeoutChan = timeoutTimer.C
		defer timeoutTimer.Stop()
	}

	attempt := 0
	maxAttempts := retries + 1

	for {
		attempt++

		// Execute the task
		switch {
		case task.Command != "":
			output, err = e.executeCommand(task.Command, task.Sudo)
			// Check if the exit code is allowed
			if err != nil && len(task.AllowedExitCodes) > 0 {
				if types.ExecOptions.Verbose {
					e.mu.Lock()
					log.SetOutput(writer)
					log.Printf("[VERBOSE] [%s] Command failed with error: %v", e.Host.Name, err)
					log.Printf("[VERBOSE] [%s] Checking against allowed exit codes: %v", e.Host.Name, task.AllowedExitCodes)
					log.SetOutput(os.Stderr)
					e.mu.Unlock()
				}

				if e.isAllowedExitCode(err, task.AllowedExitCodes) {
					if types.ExecOptions.Verbose {
						e.mu.Lock()
						log.SetOutput(writer)
						log.Printf("[VERBOSE] [%s] Exit code is in allowed list, treating as success", e.Host.Name)
						log.SetOutput(os.Stderr)
						e.mu.Unlock()
					}
					err = nil
				}
			}
		case task.Shell != "":
			output, err = e.executeCommand(task.Shell, task.Sudo)
			// Check if the exit code is allowed
			if err != nil && len(task.AllowedExitCodes) > 0 {
				if types.ExecOptions.Verbose {
					e.mu.Lock()
					log.SetOutput(writer)
					log.Printf("[VERBOSE] [%s] Command failed with error: %v", e.Host.Name, err)
					log.Printf("[VERBOSE] [%s] Checking against allowed exit codes: %v", e.Host.Name, task.AllowedExitCodes)
					log.SetOutput(os.Stderr)
					e.mu.Unlock()
				}

				if e.isAllowedExitCode(err, task.AllowedExitCodes) {
					if types.ExecOptions.Verbose {
						e.mu.Lock()
						log.SetOutput(writer)
						log.Printf("[VERBOSE] [%s] Exit code is in allowed list, treating as success", e.Host.Name)
						log.SetOutput(os.Stderr)
						e.mu.Unlock()
					}
					err = nil
				}
			}
		case task.Script != "":
			output, err = e.executeScript(task.Script, task.Sudo)
			// Check if the exit code is allowed
			if err != nil && len(task.AllowedExitCodes) > 0 {
				if types.ExecOptions.Verbose {
					e.mu.Lock()
					log.SetOutput(writer)
					log.Printf("[VERBOSE] [%s] Command failed with error: %v", e.Host.Name, err)
					log.Printf("[VERBOSE] [%s] Checking against allowed exit codes: %v", e.Host.Name, task.AllowedExitCodes)
					log.SetOutput(os.Stderr)
					e.mu.Unlock()
				}

				if e.isAllowedExitCode(err, task.AllowedExitCodes) {
					if types.ExecOptions.Verbose {
						e.mu.Lock()
						log.SetOutput(writer)
						log.Printf("[VERBOSE] [%s] Exit code is in allowed list, treating as success", e.Host.Name)
						log.SetOutput(os.Stderr)
						e.mu.Unlock()
					}
					err = nil
				}
			}
		case task.LocalAction != "":
			output, err = e.executeLocalAction(task.LocalAction)
			// Check if the exit code is allowed
			if err != nil && len(task.AllowedExitCodes) > 0 {
				if types.ExecOptions.Verbose {
					e.mu.Lock()
					log.SetOutput(writer)
					log.Printf("[VERBOSE] [%s] Command failed with error: %v", e.Host.Name, err)
					log.Printf("[VERBOSE] [%s] Checking against allowed exit codes: %v", e.Host.Name, task.AllowedExitCodes)
					log.SetOutput(os.Stderr)
					e.mu.Unlock()
				}

				if e.isAllowedExitCode(err, task.AllowedExitCodes) {
					if types.ExecOptions.Verbose {
						e.mu.Lock()
						log.SetOutput(writer)
						log.Printf("[VERBOSE] [%s] Exit code is in allowed list, treating as success", e.Host.Name)
						log.SetOutput(os.Stderr)
						e.mu.Unlock()
					}
					err = nil
				}
			}
		case task.Command != "" && task.DelegateTo != "":
			output, err = e.executeDelegated(task.Command, task.DelegateTo)
			// Check if the exit code is allowed
			if err != nil && len(task.AllowedExitCodes) > 0 {
				if types.ExecOptions.Verbose {
					e.mu.Lock()
					log.SetOutput(writer)
					log.Printf("[VERBOSE] [%s] Command failed with error: %v", e.Host.Name, err)
					log.Printf("[VERBOSE] [%s] Checking against allowed exit codes: %v", e.Host.Name, task.AllowedExitCodes)
					log.SetOutput(os.Stderr)
					e.mu.Unlock()
				}

				if e.isAllowedExitCode(err, task.AllowedExitCodes) {
					if types.ExecOptions.Verbose {
						e.mu.Lock()
						log.SetOutput(writer)
						log.Printf("[VERBOSE] [%s] Exit code is in allowed list, treating as success", e.Host.Name)
						log.SetOutput(os.Stderr)
						e.mu.Unlock()
					}
					err = nil
				}
			}
		case task.Copy != nil:
			output, err = e.executeCopy(task.Copy)
		case task.Fetch != nil:
			output, err = e.executeFetch(task.Fetch) // 新增
		case task.WaitFor != "":
			output, err = e.executeWaitFor(task.WaitFor)
		default:
			return fmt.Errorf("no executable task type defined")
		}

		// Success!
		if err == nil {
			if attempt > 1 {
				e.mu.Lock()
				fmt.Fprintf(writer, "  ✓ Success (after %d attempts)\n", attempt)
				e.mu.Unlock()
			}
			break
		}

		// Check timeout
		if timeoutChan != nil {
			select {
			case <-timeoutChan:
				e.mu.Lock()
				fmt.Fprintf(writer, "  ✗ Timeout after %d seconds (attempted %d times)\n", task.Timeout, attempt)
				e.mu.Unlock()
				return fmt.Errorf("timeout after %d seconds: %w", task.Timeout, err)
			default:
			}
		}

		// Check if we should retry
		if attempt >= maxAttempts {
			break
		}

		// Log retry attempt
		if types.ExecOptions.Verbose {
			e.mu.Lock()
			log.SetOutput(writer)
			log.Printf("[VERBOSE] [%s] Attempt %d/%d failed, retrying in %v: %v",
				e.Host.Name, attempt, maxAttempts, retryDelay, err)
			log.SetOutput(os.Stderr)
			e.mu.Unlock()
		} else {
			e.mu.Lock()
			fmt.Fprintf(writer, "  ⟳ Attempt %d/%d failed, retrying in %v...\n", attempt, maxAttempts, retryDelay)
			e.mu.Unlock()
		}

		// Wait before retry (but check for timeout)
		if timeoutChan != nil {
			select {
			case <-timeoutChan:
				e.mu.Lock()
				fmt.Fprintf(writer, "  ✗ Timeout after %d seconds\n", task.Timeout)
				e.mu.Unlock()
				return fmt.Errorf("timeout after %d seconds: %w", task.Timeout, err)
			case <-time.After(retryDelay):
			}
		} else {
			time.Sleep(retryDelay)
		}
	}

	if task.Register != "" {
		// 确保 maps 已初始化
		if e.Registers == nil {
			e.Registers = make(map[string]string)
		}
		if e.Variables == nil {
			e.Variables = make(map[string]interface{})
		}

		// 注册输出
		e.Registers[task.Register] = output
		e.Variables[task.Register] = output

		if types.ExecOptions.Verbose {
			e.mu.Lock()
			log.SetOutput(writer)
			log.Printf("[VERBOSE] [%s] Registered output to: %s (length: %d bytes)",
				e.Host.Name, task.Register, len(output))
			log.SetOutput(os.Stderr)
			e.mu.Unlock()
		}
	}

	if err != nil {
		if task.IgnoreError {
			e.mu.Lock()
			fmt.Fprintf(writer, "  ⚠ Failed (ignored): %v\n", err)
			if output != "" {
				e.printOutput(writer, output)
			}
			e.CompletedTasks[task.Name] = true
			e.mu.Unlock()
			return nil
		}
		// Show output on error before returning
		if output != "" {
			e.mu.Lock()
			e.printOutput(writer, output)
			e.mu.Unlock()
		}
		return err
	}

	e.mu.Lock()
	if attempt == 1 {
		fmt.Fprintf(writer, "  %s✓ Success%s\n", utils.Color(utils.ColorGreen), utils.Color(utils.ColorReset))
	}
	if output != "" {
		e.printOutput(writer, output)
	}
	e.CompletedTasks[task.Name] = true
	e.mu.Unlock()

	return nil
}

func (e *Executor) executeCommand(cmd string, sudo bool) (string, error) {
	writer := e.OutputWriter
	if writer == nil {
		writer = os.Stdout
	}

	cmd = e.SubstituteVars(cmd)

	if sudo {
		cmd = "sudo -S " + cmd
	}

	if types.ExecOptions.Verbose {
		e.mu.Lock()
		log.SetOutput(writer)
		log.Printf("[VERBOSE] [%s] Executing: %s", e.Host.Name, cmd)
		log.SetOutput(os.Stderr)
		e.mu.Unlock()
	}

	// Handle dry-run mode
	if types.ExecOptions.DryRun {
		e.mu.Lock()
		fmt.Fprintf(writer, "  🔍 DRY-RUN: Would execute: %s\n", cmd)
		e.mu.Unlock()

		// For testing purposes in dry-run mode, we can simulate output
		// Check if this is an echo command and extract content
		if strings.HasPrefix(cmd, "echo ") {
			content := cmd[5:] // Skip "echo "
			// Handle various quoting styles
			if (strings.HasPrefix(content, "'") && strings.HasSuffix(content, "'")) ||
				(strings.HasPrefix(content, "\"") && strings.HasSuffix(content, "\"")) {
				return content[1 : len(content)-1], nil
			}
			return content, nil
		}

		return "DRY-RUN: Command would execute", nil
	}

	// 如果需要 sudo 并且配置了 become，使用特权 session
	//if sudo && (e.Host.BecomeUser != "" || e.Host.BecomePass != "") {
	//	return e.executeCommandWithPrivilege(cmd)
	//}

	// 分支2：降级原有单次session逻辑（无shell场景兼容）

	session, err := e.client.NewSession()
	if err != nil {
		return "", fmt.Errorf("failed to create session: %w", err)
	}
	defer session.Close()

	// Check if we should stream output in real-time
	if types.ExecOptions.Progress {
		return e.executeCommandStreaming(session, cmd, writer)
	}

	var stdout, stderr bytes.Buffer
	session.Stdout = &stdout
	session.Stderr = &stderr

	err = session.Run(cmd)
	output := stdout.String()
	if stderr.Len() > 0 {
		output += "\nSTDERR: " + stderr.String()
	}

	if types.ExecOptions.Verbose {
		e.mu.Lock()
		log.SetOutput(writer)
		log.Printf("[VERBOSE] [%s] Command output length: %d bytes", e.Host.Name, len(output))
		log.SetOutput(os.Stderr)
		e.mu.Unlock()
	}

	if err != nil {
		return output, fmt.Errorf("command failed: %w", err)
	}

	return output, nil
}

// min 辅助函数
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (e *Executor) executeCommandStreaming(session *ssh.Session, cmd string, writer io.Writer) (string, error) {
	var outputBuf bytes.Buffer

	// Create pipes for stdout and stderr
	stdout, err := session.StdoutPipe()
	if err != nil {
		return "", fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	stderr, err := session.StderrPipe()
	if err != nil {
		return "", fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	// Start the command
	if err := session.Start(cmd); err != nil {
		return "", fmt.Errorf("failed to start command: %w", err)
	}

	// Stream output in real-time
	var wg sync.WaitGroup
	wg.Add(2)

	// Stream stdout line by line
	go func() {
		defer wg.Done()
		scanner := bytes.NewReader(nil)
		buf := make([]byte, 4096)

		for {
			n, err := stdout.Read(buf)
			if n > 0 {
				data := buf[:n]
				outputBuf.Write(data)

				// Write immediately to output
				e.mu.Lock()
				_, _ = writer.Write([]byte("    │ "))
				_, _ = writer.Write(data)
				if data[n-1] != '\n' {
					_, _ = writer.Write([]byte("\n"))
				}
				e.mu.Unlock()
			}
			if err != nil {
				break
			}
		}
		_ = scanner
	}()

	// Stream stderr line by line
	go func() {
		defer wg.Done()
		buf := make([]byte, 4096)

		for {
			n, err := stderr.Read(buf)
			if n > 0 {
				data := buf[:n]
				outputBuf.Write(data)

				// Write immediately to output
				e.mu.Lock()
				_, _ = writer.Write([]byte("    │ [stderr] "))
				_, _ = writer.Write(data)
				if data[n-1] != '\n' {
					_, _ = writer.Write([]byte("\n"))
				}
				e.mu.Unlock()
			}
			if err != nil {
				break
			}
		}
	}()

	// Wait for command to complete
	cmdErr := session.Wait()

	// Wait for all output to be read
	wg.Wait()

	output := outputBuf.String()

	if cmdErr != nil {
		return output, fmt.Errorf("command failed: %w", cmdErr)
	}

	return output, nil
}

func (e *Executor) executeScript(scriptPath string, sudo bool) (string, error) {
	script, err := os.ReadFile(filepath.Clean(scriptPath))
	if err != nil {
		return "", fmt.Errorf("failed to read script: %w", err)
	}

	scriptContent := e.SubstituteVars(string(script))

	session, err := e.client.NewSession()
	if err != nil {
		return "", fmt.Errorf("failed to create session: %w", err)
	}
	defer session.Close()

	tmpFile := fmt.Sprintf("/tmp/script_%d.sh", time.Now().Unix())
	cmd := fmt.Sprintf("cat > %s && chmod +x %s", tmpFile, tmpFile)

	stdin, err := session.StdinPipe()
	if err != nil {
		return "", err
	}

	if err := session.Start(cmd); err != nil {
		return "", err
	}

	_, err = io.WriteString(stdin, scriptContent)
	if err != nil {
		return "", fmt.Errorf("failed to write script content into stdin: %w", err)
	}

	// Close stdin to signal EOF
	if err := stdin.Close(); err != nil {
		return "", fmt.Errorf("failed to close stdin: %w", err)
	}

	if err := session.Wait(); err != nil {
		return "", fmt.Errorf("failed to upload script: %w", err)
	}

	execCmd := tmpFile
	if sudo {
		execCmd = "sudo " + tmpFile
	}

	output, err := e.executeCommand(execCmd, false)
	if err != nil {
		return "", fmt.Errorf("failed to execute script: %w", err)
	}

	_, err = e.executeCommand(fmt.Sprintf("rm -f %s", tmpFile), false)
	if err != nil {
		return "", fmt.Errorf("failed to cleanup script: %w", err)
	}

	return output, err
}

func (e *Executor) executeCopy(copyTask *types.CopyTask) (string, error) {
	content, err := os.ReadFile(copyTask.Src)
	if err != nil {
		return "", fmt.Errorf("failed to read source file: %w", err)
	}

	contentStr := e.SubstituteVars(string(content))

	session, err := e.client.NewSession()
	if err != nil {
		return "", fmt.Errorf("failed to create session: %w", err)
	}
	defer session.Close()

	dest := e.SubstituteVars(copyTask.Dest)
	cmd := fmt.Sprintf("cat > %s", dest)

	stdin, err := session.StdinPipe()
	if err != nil {
		return "", err
	}

	if err := session.Start(cmd); err != nil {
		return "", err
	}

	_, err = io.WriteString(stdin, contentStr)
	if err != nil {
		return "", fmt.Errorf("failed to write file content into stdin: %w", err)
	}

	// Close stdin to signal EOF
	if err := stdin.Close(); err != nil {
		return "", fmt.Errorf("failed to close stdin: %w", err)
	}

	if err := session.Wait(); err != nil {
		return "", fmt.Errorf("failed to copy file: %w", err)
	}

	if copyTask.Mode != "" {
		_, err = e.executeCommand(fmt.Sprintf("chmod %s %s", copyTask.Mode, dest), false)
		if err != nil {
			return "", err
		}
	}

	return fmt.Sprintf("Copied %s to %s", copyTask.Src, dest), nil
}

func (e *Executor) executeWaitFor(condition string) (string, error) {
	parts := strings.SplitN(condition, ":", 2)
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid wait_for format: %s (expected type:value)", condition)
	}

	waitType := parts[0]
	waitValue := parts[1]

	maxRetries := 30
	retryDelay := 2 * time.Second

	for i := 0; i < maxRetries; i++ {
		var checkCmd string
		switch waitType {
		case "port":
			checkCmd = fmt.Sprintf("nc -z localhost %s", waitValue)
		case "service":
			checkCmd = fmt.Sprintf("systemctl is-active %s", waitValue)
		case "file":
			checkCmd = fmt.Sprintf("test -f %s", waitValue)
		case "http":
			checkCmd = fmt.Sprintf("curl -sf %s", waitValue)
		default:
			return "", fmt.Errorf("unknown wait_for type: %s", waitType)
		}

		_, err := e.executeCommand(checkCmd, false)
		if err == nil {
			return fmt.Sprintf("Condition met: %s", condition), nil
		}

		if i < maxRetries-1 {
			time.Sleep(retryDelay)
		}
	}

	return "", fmt.Errorf("timeout waiting for: %s", condition)
}

// SubstituteVars 替换字符串中的变量
// SubstituteVars 替换字符串中的变量，支持 JSON 路径访问
// 支持格式：
//
//	{{ variable }}           - 直接变量
//	{{ variable.key }}       - 嵌套 map 访问
//	{{ variable[0] }}        - 数组索引访问
//	{{ variable.key[0].sub }} - 混合访问
func (e *Executor) SubstituteVars(text string) string {
	if text == "" {
		return text
	}

	// 合并所有变量源
	allVars := e.getAllVariables()

	// 正则表达式匹配 {{ path }}
	re := regexp.MustCompile(`{{\s*([^{}]+?)\s*}}`)
	result := re.ReplaceAllStringFunc(text, func(match string) string {
		path := strings.TrimSpace(match[2 : len(match)-2])
		value := e.extractValueByPath(allVars, path)
		return fmt.Sprintf("%v", value)
	})

	return result
}

// getAllVariables 合并所有变量源
func (e *Executor) getAllVariables() map[string]interface{} {
	allVars := make(map[string]interface{})

	// 1. Host.Vars
	if e.Host.Vars != nil {
		for k, v := range e.Host.Vars {
			allVars[k] = v
		}
	}

	// 2. Variables (facts 等)
	if e.Variables != nil {
		for k, v := range e.Variables {
			allVars[k] = v
		}
	}

	// 3. Registers (任务输出)
	if e.Registers != nil {
		for k, v := range e.Registers {
			allVars[k] = v
		}
	}

	return allVars
}

// extractValueByPath 根据路径提取值
// 支持格式：
//
//	user.name
//	users[0].name
//	data.results[2].value
func (e *Executor) extractValueByPath(data map[string]interface{}, path string) interface{} {
	if path == "" {
		return ""
	}

	// 分割路径
	parts := e.parsePath(path)
	var current interface{} = data

	for _, part := range parts {
		if current == nil {
			return ""
		}

		// 处理数组索引: [0] 或 [index]
		if strings.HasPrefix(part, "[") && strings.HasSuffix(part, "]") {
			indexStr := part[1 : len(part)-1]
			index, err := strconv.Atoi(indexStr)
			if err != nil {
				return ""
			}

			// 尝试转换为数组
			switch v := current.(type) {
			case []interface{}:
				if index >= 0 && index < len(v) {
					current = v[index]
				} else {
					return ""
				}
			case []string:
				if index >= 0 && index < len(v) {
					current = v[index]
				} else {
					return ""
				}
			default:
				return ""
			}
		} else {
			// 处理 map 访问
			switch v := current.(type) {
			case map[string]interface{}:
				if val, exists := v[part]; exists {
					current = val
				} else {
					return ""
				}
			case map[string]string:
				if val, exists := v[part]; exists {
					current = val
				} else {
					return ""
				}
			default:
				return ""
			}
		}
	}

	// 转换为字符串
	return e.valueToString(current)
}

// parsePath 解析路径字符串为部分
// 例如: "users[0].name" -> ["users", "[0]", "name"]
func (e *Executor) parsePath(path string) []string {
	var parts []string
	var current strings.Builder
	inBracket := false

	for i := 0; i < len(path); i++ {
		ch := path[i]

		if ch == '[' {
			if current.Len() > 0 {
				parts = append(parts, current.String())
				current.Reset()
			}
			inBracket = true
			current.WriteByte(ch)
		} else if ch == ']' {
			current.WriteByte(ch)
			parts = append(parts, current.String())
			current.Reset()
			inBracket = false
		} else if ch == '.' && !inBracket {
			if current.Len() > 0 {
				parts = append(parts, current.String())
				current.Reset()
			}
		} else {
			current.WriteByte(ch)
		}
	}

	if current.Len() > 0 {
		parts = append(parts, current.String())
	}

	return parts
}

// valueToString 将任意值转换为字符串
func (e *Executor) valueToString(value interface{}) string {
	if value == nil {
		return ""
	}

	switch v := value.(type) {
	case string:
		return v
	case int, int32, int64, float32, float64, bool:
		return fmt.Sprintf("%v", v)
	case []interface{}:
		// 如果是数组，返回 JSON 格式
		jsonBytes, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprintf("%v", v)
		}
		return string(jsonBytes)
	case map[string]interface{}:
		// 如果是对象，返回 JSON 格式
		jsonBytes, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprintf("%v", v)
		}
		return string(jsonBytes)
	default:
		return fmt.Sprintf("%v", v)
	}
}
func (e *Executor) evaluateCondition(condition string) bool {
	condition = strings.TrimSpace(condition)

	if strings.HasSuffix(condition, "is defined") {
		varName := strings.TrimSuffix(condition, "is defined")
		varName = strings.TrimSpace(varName)
		_, exists := e.Variables[varName]
		return exists
	}

	if strings.Contains(condition, "==") {
		parts := strings.Split(condition, "==")
		if len(parts) == 2 {
			left := strings.TrimSpace(e.SubstituteVars(parts[0]))
			right := strings.TrimSpace(strings.Trim(parts[1], "'\""))
			return left == right
		}
	}

	return true
}

// In executor.go - Update isAllowedExitCode function to focus on command exit codes
func (e *Executor) isAllowedExitCode(err error, allowedCodes []int) bool {
	if err == nil {
		return true
	}

	// If no specific exit codes are allowed, only 0 is acceptable
	if len(allowedCodes) == 0 {
		return false
	}

	// Extract exit code from any error type
	exitCode := extractExitCode(err)
	if exitCode < 0 {
		// No valid exit code found
		return false
	}

	// Check if the exit code is in the allowed list
	for _, allowed := range allowedCodes {
		if exitCode == allowed {
			return true
		}
	}

	return false
}

// Helper function to extract exit code from various error types
func extractExitCode(err error) int {
	// Try SSH ExitError type first
	if exitErr, ok := err.(*ssh.ExitError); ok {
		return exitErr.ExitStatus()
	}

	// Try to parse from error message
	errStr := err.Error()

	// Common patterns for exit codes in error messages
	patterns := []string{
		"Process exited with status ",
		"exit status ",
		"exited with code ",
	}

	for _, pattern := range patterns {
		if idx := strings.Index(errStr, pattern); idx >= 0 {
			codeStr := strings.TrimSpace(errStr[idx+len(pattern):])
			// If there's more text after the number, trim it
			if spaceIdx := strings.Index(codeStr, " "); spaceIdx > 0 {
				codeStr = codeStr[:spaceIdx]
			}

			// Try to parse the exit code
			code, err := strconv.Atoi(codeStr)
			if err == nil {
				return code
			}
		}
	}

	// No valid exit code found
	return -1
}

// printOutput handles output formatting with optional truncation
func (e *Executor) printOutput(writer io.Writer, output string) {
	// If full output is enabled, show everything
	if types.ExecOptions.FullOutput {
		lines := strings.Split(strings.TrimSpace(output), "\n")
		if len(lines) == 1 {
			fmt.Fprintf(writer, "    %sOutput:%s %s\n", utils.Color(utils.ColorGray), utils.Color(utils.ColorReset), strings.TrimSpace(output))
		} else {
			fmt.Fprintf(writer, "    %sOutput:%s (%d lines)\n", utils.Color(utils.ColorGray), utils.Color(utils.ColorReset), len(lines))
			for _, line := range lines {
				fmt.Fprintf(writer, "      %s\n", line)
			}
		}
	} else {
		// Original truncation logic
		if len(output) < 500 {
			fmt.Fprintf(writer, "    %sOutput:%s %s\n", utils.Color(utils.ColorGray), utils.Color(utils.ColorReset), strings.TrimSpace(output))
		} else {
			lines := strings.Split(strings.TrimSpace(output), "\n")
			if len(lines) <= 10 {
				fmt.Fprintf(writer, "    %sOutput:%s\n", utils.Color(utils.ColorGray), utils.Color(utils.ColorReset))
				for _, line := range lines {
					fmt.Fprintf(writer, "      %s\n", line)
				}
			} else {
				fmt.Fprintf(writer, "    %sOutput%s (showing first 5 and last 5 lines of %d total):\n", utils.Color(utils.ColorGray), utils.Color(utils.ColorReset), len(lines))
				for i := 0; i < 5; i++ {
					fmt.Fprintf(writer, "      %s\n", lines[i])
				}
				fmt.Fprintf(writer, "      %s... (%d lines omitted) ...%s\n", utils.Color(utils.ColorGray), len(lines)-10, utils.Color(utils.ColorReset))
				for i := len(lines) - 5; i < len(lines); i++ {
					fmt.Fprintf(writer, "      %s\n", lines[i])
				}
			}
		}
	}
}

func (e *Executor) executeLocalAction(cmd string) (string, error) {
	cmd = e.SubstituteVars(cmd)

	if types.ExecOptions.Verbose {
		e.mu.Lock()
		log.SetOutput(e.OutputWriter)
		log.Printf("[VERBOSE] [%s] Executing locally: %s", e.Host.Name, cmd)
		log.SetOutput(os.Stderr)
		e.mu.Unlock()
	}

	// Create command
	parts := strings.Fields(cmd)
	if len(parts) == 0 {
		return "", fmt.Errorf("empty command")
	}

	command := exec.Command("/bin/sh", "-c", cmd)

	// Capture output
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr

	err := command.Run()
	output := stdout.String()
	if stderr.Len() > 0 {
		output += "\nSTDERR: " + stderr.String()
	}

	if err != nil {
		return output, fmt.Errorf("local command failed: %w", err)
	}

	return output, nil
}

func (e *Executor) executeDelegated(cmd string, delegateHost string) (string, error) {
	// If delegated to localhost, just run it locally
	if delegateHost == "localhost" || delegateHost == "127.0.0.1" {
		return e.executeLocalAction(cmd)
	}

	// Otherwise, for delegation to happen correctly, the task has to be
	// executed by the proper host's executor. This function should never
	// actually be called since we filter at a higher level.
	return "", fmt.Errorf("delegation to %s should be handled by skipping execution on non-delegate hosts", delegateHost)
}

func ResetRunOnceTracking() {
	types.RunOnceTasks.Lock()
	types.RunOnceTasks.Executed = make(map[string]bool)
	types.RunOnceTasks.Unlock()
}

func NewExecutor(host types.Host, groupName string) (*Executor, error) {
	if types.ExecOptions.Verbose {
		log.Printf("[VERBOSE] Connecting to Host: %s", host.Name)
	}

	// Get host key callback for verification
	hostKeyCallback, err := getHostKeyCallback(host.StrictHostKeyCheck)
	if err != nil {
		return nil, fmt.Errorf("failed to load host keys: %w", err)
	}

	config := &ssh.ClientConfig{
		User:            host.User,
		HostKeyCallback: hostKeyCallback,
		Timeout:         10 * time.Second,
	}

	var authMethods []ssh.AuthMethod

	// Determine which UseAgent to use (host-specific or default)
	useAgent := host.UseAgent

	if useAgent || (host.KeyFile == "" && host.Password == "" && os.Getenv("SSH_AUTH_SOCK") != "") {
		if agentAuth := getSSHAgent(); agentAuth != nil {
			authMethods = append(authMethods, agentAuth)
			if types.ExecOptions.Verbose {
				log.Printf("[VERBOSE] [%s] Using ssh-agent for authentication", host.Name)
			}
		} else if useAgent {
			return nil, fmt.Errorf("use_agent is true but ssh-agent is not available")
		}
	}

	if host.KeyFile != "" {
		keyPath := host.KeyFile
		if strings.HasPrefix(keyPath, "~/") {
			homeDir, err := os.UserHomeDir()
			if err == nil {
				keyPath = strings.Replace(keyPath, "~", homeDir, 1)
			}
		}

		if types.ExecOptions.Verbose {
			log.Printf("[VERBOSE] [%s] Reading key file: %s", host.Name, keyPath)
		}

		key, err := os.ReadFile(filepath.Clean(keyPath))
		if err != nil {
			return nil, fmt.Errorf("unable to read private key: %w", err)
		}

		var signer ssh.Signer
		if host.KeyPassword != "" {
			signer, err = ssh.ParsePrivateKeyWithPassphrase(key, []byte(host.KeyPassword))
			if err != nil {
				return nil, fmt.Errorf("unable to parse private key with passphrase: %w", err)
			}
		} else {
			signer, err = ssh.ParsePrivateKey(key)
			if err != nil {
				fmt.Printf("Private key for %s appears to be passphrase protected.\n", host.Name)
				fmt.Printf("Enter passphrase for %s: ", host.KeyFile)
				var passphrase string
				_, err = fmt.Scanln(&passphrase)
				if err != nil {
					return nil, fmt.Errorf("unable to read stdin for private key passphrase: %w", err)
				}
				signer, err = ssh.ParsePrivateKeyWithPassphrase(key, []byte(passphrase))
				if err != nil {
					return nil, fmt.Errorf("unable to parse private key with passphrase: %w", err)
				}
			}
		}

		authMethods = append(authMethods, ssh.PublicKeys(signer))
		if types.ExecOptions.Verbose {
			log.Printf("[VERBOSE] [%s] Using key file: %s", host.Name, keyPath)
		}
	}

	if host.Password != "" {
		authMethods = append(authMethods, ssh.Password(host.Password))
		if types.ExecOptions.Verbose {
			log.Printf("[VERBOSE] [%s] Using password authentication", host.Name)
		}
	}

	if len(authMethods) == 0 {
		return nil, fmt.Errorf("no authentication method provided (try: use_agent: true, key_file, or password)")
	}

	config.Auth = authMethods

	port := host.Port
	if port == 0 {
		port = 22
	}

	target := host.Address
	if target == "" {
		target = host.Hostname
	}
	if target == "" {
		return nil, fmt.Errorf("no address or hostname provided")
	}

	if types.ExecOptions.Verbose {
		log.Printf("[VERBOSE] [%s] Dialing %s:%d", host.Name, target, port)
	}

	if types.ExecOptions.DryRun {
		if types.ExecOptions.Verbose {
			log.Printf("[VERBOSE] [%s] DRY-RUN: Skipping actual SSH connection", host.Name)
		}
		vars := make(map[string]interface{})
		if host.Vars != nil {
			for k, v := range host.Vars {
				vars[k] = v
			}
		}

		return &Executor{
			Host:           host,
			client:         nil,
			Variables:      vars,
			Registers:      make(map[string]string),
			CompletedTasks: make(map[string]bool),
			GroupName:      groupName,
			OutputWriter:   os.Stdout,
			StartTime:      time.Now(),
		}, nil
	}

	client, err := ssh.Dial("tcp", fmt.Sprintf("%s:%d", target, port), config)
	if err != nil {
		return nil, fmt.Errorf("failed to dial: %w", err)
	}

	if types.ExecOptions.Verbose {
		log.Printf("[VERBOSE] [%s] Successfully connected", host.Name)
	}

	if client != nil {
		// 每 30 秒发送一个 keepalive
		go func() {
			ticker := time.NewTicker(30 * time.Second)
			defer ticker.Stop()
			for range ticker.C {
				_, _, err := client.SendRequest("keepalive@sshot", true, nil)
				if err != nil {
					return
				}
			}
		}()
	}

	return &Executor{
		Host:           host,
		client:         client,
		Variables:      host.Vars,
		Registers:      make(map[string]string),
		CompletedTasks: make(map[string]bool),
		GroupName:      groupName,
		OutputWriter:   os.Stdout,
		StartTime:      time.Now(),
	}, nil
}

func getSSHAgent() ssh.AuthMethod {
	sshAuthSock := os.Getenv("SSH_AUTH_SOCK")
	if sshAuthSock == "" {
		return nil
	}

	// Use sync.Once to create a single SSH agent connection
	sshAgentOnce.Do(func() {
		conn, err := net.Dial("unix", sshAuthSock)
		if err != nil {
			return
		}
		sshAgentClient = agent.NewClient(conn)
	})

	if sshAgentClient == nil {
		return nil
	}

	return ssh.PublicKeysCallback(sshAgentClient.Signers)
}

func (e *Executor) Close() error {
	if e.client != nil {
		return e.client.Close()
	}
	return nil
}

func getHostKeyCallback(strictHostKeyCheck *bool) (ssh.HostKeyCallback, error) {
	// Determine the actual value to use (default to true if nil)
	strict := true
	if strictHostKeyCheck != nil {
		strict = *strictHostKeyCheck
	}

	// If strict host key checking is disabled, use insecure callback
	// This is useful for testing environments but should be avoided in production
	if !strict {
		if types.ExecOptions.Verbose {
			log.Printf("[VERBOSE] WARNING: types.Host key verification is disabled (strict_host_key_check: false)")
		}
		return ssh.InsecureIgnoreHostKey(), nil //gosec:disable G106
	}

	// Try to load known_hosts file
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("unable to get home directory: %w", err)
	}

	knownHostsPath := filepath.Join(homeDir, ".ssh", "known_hosts")

	// Check if known_hosts exists
	if _, err := os.Stat(knownHostsPath); os.IsNotExist(err) {
		// Create .ssh directory if it doesn't exist
		sshDir := filepath.Join(homeDir, ".ssh")
		if err := os.MkdirAll(sshDir, 0700); err != nil {
			return nil, fmt.Errorf("unable to create .ssh directory: %w", err)
		}

		// Create empty known_hosts file
		if _, err := os.Create(filepath.Clean(knownHostsPath)); err != nil {
			return nil, fmt.Errorf("unable to create known_hosts file: %w", err)
		}

		if types.ExecOptions.Verbose {
			log.Printf("[VERBOSE] Created new known_hosts file at: %s", knownHostsPath)
		}
	}

	// Load known_hosts
	hostKeyCallback, err := knownhosts.New(knownHostsPath)
	if err != nil {
		return nil, fmt.Errorf("unable to load known_hosts: %w", err)
	}

	// Wrap the callback to provide better error messages
	return ssh.HostKeyCallback(func(hostname string, remote net.Addr, key ssh.PublicKey) error {
		err := hostKeyCallback(hostname, remote, key)
		if err != nil {
			// Extract hostname without port for ssh-keyscan command
			host, _, splitErr := net.SplitHostPort(hostname)
			if splitErr != nil {
				// If splitting fails, use the original hostname
				host = hostname
			}

			// Check if this is a host key mismatch or unknown host
			if keyErr, ok := err.(*knownhosts.KeyError); ok && len(keyErr.Want) > 0 {
				return fmt.Errorf("host key verification failed for %s: %w\nThe host key has changed. This could indicate a security breach.\nIf you trust this host, remove the old key from %s", hostname, err, knownHostsPath)
			}
			return fmt.Errorf("host key verification failed for %s: %w\nTo add this host, run: ssh-keyscan -H %s >> %s", hostname, err, host, knownHostsPath)
		}
		return nil
	}), nil
}

// executeFetch 从远程服务器拉取文件或目录到本地
func (e *Executor) executeFetch(fetchTask *types.FetchTask) (string, error) {
	writer := e.OutputWriter
	if writer == nil {
		writer = os.Stdout
	}

	src := e.SubstituteVars(fetchTask.Src)
	dest := e.SubstituteVars(fetchTask.Dest)

	if types.ExecOptions.Verbose {
		e.mu.Lock()
		fmt.Fprintf(writer, "    │ Fetch src: %s\n", src)
		fmt.Fprintf(writer, "    │ Fetch dest: %s\n", dest)
		fmt.Fprintf(writer, "    │ Flat mode: %v\n", fetchTask.Flat)
		e.mu.Unlock()
	}

	// 确保本地目标目录存在
	if err := os.MkdirAll(dest, 0755); err != nil {
		return "", fmt.Errorf("failed to create local directory %s: %w", dest, err)
	}

	// 对于简单的文件检查，不使用常驻 shell，而是创建新 session
	// 这样可以避免常驻 shell 的输出读取问题
	checkCmd := fmt.Sprintf("if [ -d '%s' ]; then echo 'directory'; elif [ -f '%s' ]; then echo 'file'; else echo 'notfound'; fi", src, src)

	if types.ExecOptions.Verbose {
		e.mu.Lock()
		fmt.Fprintf(writer, "    │ Check command: %s\n", checkCmd)
		e.mu.Unlock()
	}

	// 使用新的 session 执行检查命令
	fileType, err := e.executeCommandWithNewSession(checkCmd)
	if err != nil {
		return "", fmt.Errorf("failed to check remote source: %w", err)
	}

	fileType = strings.TrimSpace(fileType)

	if types.ExecOptions.Verbose {
		e.mu.Lock()
		fmt.Fprintf(writer, "    │ File type: '%s'\n", fileType)
		e.mu.Unlock()
	}

	switch fileType {
	case "notfound":
		return "", fmt.Errorf("remote source not found: %s", src)
	case "directory":
		return e.fetchDirectory(src, dest, fetchTask.Flat)
	case "file":
		return e.fetchFile(src, dest, fetchTask.Flat)
	default:
		return "", fmt.Errorf("unknown remote source type: '%s' for path: %s", fileType, src)
	}
}

// executeCommandWithNewSession 使用新的 SSH session 执行命令（不使用常驻 shell）
func (e *Executor) executeCommandWithNewSession(cmd string) (string, error) {
	session, err := e.client.NewSession()
	if err != nil {
		return "", fmt.Errorf("failed to create session: %w", err)
	}
	defer session.Close()

	var stdout bytes.Buffer
	session.Stdout = &stdout

	err = session.Run(cmd)
	output := stdout.String()

	if err != nil {
		return output, fmt.Errorf("command failed: %w", err)
	}

	return strings.TrimSpace(output), nil
}

// fetchFile 从远程拉取单个文件
func (e *Executor) fetchFile(remotePath, localPath string, flat bool) (string, error) {
	// 确定本地目标路径
	var targetPath string

	if flat {
		// 扁平化模式：只取文件名
		fileName := filepath.Base(remotePath)
		targetPath = filepath.Join(localPath, fileName)
	} else {
		// 保留目录结构：使用完整路径
		targetPath = filepath.Join(localPath, remotePath)
	}

	// 确保目标目录存在
	targetDir := filepath.Dir(targetPath)
	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create local directory %s: %w", targetDir, err)
	}

	// 创建本地文件
	localFile, err := os.Create(targetPath)
	if err != nil {
		return "", fmt.Errorf("failed to create local file %s: %w", targetPath, err)
	}
	defer localFile.Close()

	// 通过 SSH session 读取远程文件
	session, err := e.client.NewSession()
	if err != nil {
		return "", fmt.Errorf("failed to create session: %w", err)
	}
	defer session.Close()

	// 使用 cat 命令读取远程文件
	cmd := fmt.Sprintf("cat '%s'", remotePath)
	stdout, err := session.StdoutPipe()
	if err != nil {
		return "", fmt.Errorf("failed to get stdout pipe: %w", err)
	}

	stderr, err := session.StderrPipe()
	if err != nil {
		return "", fmt.Errorf("failed to get stderr pipe: %w", err)
	}

	if err := session.Start(cmd); err != nil {
		return "", fmt.Errorf("failed to start command: %w", err)
	}

	// 复制内容到本地文件
	_, err = io.Copy(localFile, stdout)
	if err != nil {
		return "", fmt.Errorf("failed to copy remote file content: %w", err)
	}

	// 检查 stderr
	var stderrBuf bytes.Buffer
	_, _ = io.Copy(&stderrBuf, stderr)
	if stderrBuf.Len() > 0 {
		return "", fmt.Errorf("remote command stderr: %s", stderrBuf.String())
	}

	if err := session.Wait(); err != nil {
		return "", fmt.Errorf("failed to fetch file: %w", err)
	}

	// 获取文件信息用于日志
	fileInfo, err := os.Stat(targetPath)
	if err != nil {
		return fmt.Sprintf("Fetched %s to %s", remotePath, targetPath), nil
	}

	return fmt.Sprintf("Fetched %s (%.2f KB) to %s", remotePath, float64(fileInfo.Size())/1024, targetPath), nil
}

// fetchDirectory 从远程拉取整个目录
// fetchDirectory 从远程拉取整个目录
func (e *Executor) fetchDirectory(remotePath, localPath string, flat bool) (string, error) {
	writer := e.OutputWriter
	if writer == nil {
		writer = os.Stdout
	}

	if flat {
		return "", fmt.Errorf("cannot fetch directory with flat=true, use flat=false for directories")
	}

	if types.ExecOptions.Verbose {
		e.mu.Lock()
		fmt.Fprintf(writer, "    │ Listing remote directory: %s\n", remotePath)
		e.mu.Unlock()
	}

	// 生成远程文件列表
	listCmd := fmt.Sprintf("find '%s' -type f 2>/dev/null | sort", remotePath)

	// 使用新 session 获取文件列表
	fileListOutput, err := e.executeCommandWithNewSession(listCmd)
	if err != nil {
		return "", fmt.Errorf("failed to list remote directory: %w", err)
	}

	// 解析文件列表
	files := strings.Split(strings.TrimSpace(fileListOutput), "\n")
	if len(files) == 0 || (len(files) == 1 && files[0] == "") {
		return "", fmt.Errorf("no files found in remote directory: %s", remotePath)
	}

	totalFiles := len(files)
	if types.ExecOptions.Verbose {
		e.mu.Lock()
		fmt.Fprintf(writer, "    │ Found %d files to fetch\n", totalFiles)
		e.mu.Unlock()
	}

	successCount := 0
	var errors []string

	// 创建进度通道
	//progress := make(chan struct {
	//	file string
	//	err  error
	//}, 1)

	// 使用带超时的 context
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	for i, remoteFile := range files {
		remoteFile = strings.TrimSpace(remoteFile)
		if remoteFile == "" {
			continue
		}

		select {
		case <-ctx.Done():
			// 超时处理
			e.mu.Lock()
			fmt.Fprintf(writer, "    │ ⏰ Timeout reached, fetched %d/%d files\n", successCount, totalFiles)
			e.mu.Unlock()
			return fmt.Sprintf("Fetched %d/%d files (timeout)", successCount, totalFiles),
				fmt.Errorf("timeout after fetching %d files", successCount)
		default:
			// 继续下载
		}

		// 计算相对路径
		relPath, err := filepath.Rel(remotePath, remoteFile)
		if err != nil {
			errors = append(errors, fmt.Sprintf("failed to get relative path for %s: %v", remoteFile, err))
			continue
		}

		// 构建本地目标路径
		localFilePath := filepath.Join(localPath, relPath)

		// 确保目标目录存在
		if err := os.MkdirAll(filepath.Dir(localFilePath), 0755); err != nil {
			errors = append(errors, fmt.Sprintf("failed to create directory for %s: %v", localFilePath, err))
			continue
		}

		// 显示进度
		if types.ExecOptions.Verbose {
			e.mu.Lock()
			fmt.Fprintf(writer, "    │ [%d/%d] Downloading: %s\n", i+1, totalFiles, filepath.Base(remoteFile))
			e.mu.Unlock()
		}

		// 拉取文件（带重试）
		if err := e.downloadFileWithRetry(remoteFile, localFilePath, 3); err != nil {
			errors = append(errors, fmt.Sprintf("failed to fetch %s: %v", remoteFile, err))
			e.mu.Lock()
			fmt.Fprintf(writer, "    │ ✗ Failed: %s - %v\n", filepath.Base(remoteFile), err)
			e.mu.Unlock()
			continue
		}

		successCount++

		if types.ExecOptions.Verbose {
			e.mu.Lock()
			fmt.Fprintf(writer, "    │ ✓ Fetched: %s\n", filepath.Base(remoteFile))
			e.mu.Unlock()
		}
	}

	// 最终报告
	e.mu.Lock()
	fmt.Fprintf(writer, "\n    │ %sFetch Summary:%s Fetched %d/%d files\n",
		utils.Color(utils.ColorGreen), utils.Color(utils.ColorReset), successCount, totalFiles)
	if len(errors) > 0 {
		fmt.Fprintf(writer, "    │ %sErrors:%s %d failures\n",
			utils.Color(utils.ColorRed), utils.Color(utils.ColorReset), len(errors))
		for _, errMsg := range errors {
			fmt.Fprintf(writer, "    │   - %s\n", errMsg)
		}
	}
	e.mu.Unlock()

	if len(errors) > 0 {
		return fmt.Sprintf("Fetched %d/%d files from %s to %s, errors: %s",
				successCount, totalFiles, remotePath, localPath, strings.Join(errors, "; ")),
			fmt.Errorf("partial failure: %d errors", len(errors))
	}

	return fmt.Sprintf("Fetched %d files from %s to %s", successCount, remotePath, localPath), nil
}

// downloadFileWithRetry 带重试的文件下载
func (e *Executor) downloadFileWithRetry(remotePath, localPath string, maxRetries int) error {
	var lastErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		if attempt > 1 {
			if types.ExecOptions.Verbose {
				e.mu.Lock()
				fmt.Printf("    │ Retry %d/%d for %s\n", attempt, maxRetries, filepath.Base(remotePath))
				e.mu.Unlock()
			}
			time.Sleep(2 * time.Second) // 重试前等待
		}

		err := e.downloadFileViaSFTP(remotePath, localPath)
		if err == nil {
			return nil
		}
		lastErr = err

		// 检查是否是连接错误，如果是，重建 SFTP 客户端
		if strings.Contains(err.Error(), "EOF") ||
			strings.Contains(err.Error(), "connection lost") ||
			strings.Contains(err.Error(), "timeout") {
			if types.ExecOptions.Verbose {
				e.mu.Lock()
				fmt.Printf("    │ Connection lost, will retry\n")
				e.mu.Unlock()
			}
			// 让下一次重试重新建立连接
			continue
		}
	}

	return fmt.Errorf("failed after %d attempts: %w", maxRetries, lastErr)
}

// downloadFile 下载单个文件（使用 SCP 协议）
func (e *Executor) downloadFile(remotePath, localPath string) error {
	// 使用 SSH session 和 SCP 协议
	session, err := e.client.NewSession()
	if err != nil {
		return fmt.Errorf("failed to create session: %w", err)
	}
	defer session.Close()

	// 使用 scp 协议读取远程文件
	cmd := fmt.Sprintf("scp -f '%s'", remotePath)

	stdin, err := session.StdinPipe()
	if err != nil {
		return fmt.Errorf("failed to get stdin pipe: %w", err)
	}

	stdout, err := session.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to get stdout pipe: %w", err)
	}

	stderr, err := session.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to get stderr pipe: %w", err)
	}

	if err := session.Start(cmd); err != nil {
		return fmt.Errorf("failed to start scp: %w", err)
	}

	// 读取 stderr 并记录
	go func() {
		var stderrBuf bytes.Buffer
		_, _ = io.Copy(&stderrBuf, stderr)
		if stderrBuf.Len() > 0 && types.ExecOptions.Verbose {
			e.mu.Lock()
			fmt.Fprintf(os.Stderr, "SCP stderr: %s\n", stderrBuf.String())
			e.mu.Unlock()
		}
	}()

	// 读取 SCP 协议的第一个响应字节
	// SCP 协议：C<模式> <大小> <文件名>\n
	response := make([]byte, 1)
	if _, err := stdout.Read(response); err != nil {
		return fmt.Errorf("failed to read scp response: %w", err)
	}

	if response[0] != 'C' {
		// 可能是错误响应
		errorBuf := make([]byte, 256)
		n, _ := stdout.Read(errorBuf)
		return fmt.Errorf("unexpected scp response: %c, message: %s", response[0], string(errorBuf[:n]))
	}

	// 读取文件信息行（直到 \n）
	reader := bufio.NewReader(stdout)
	infoLine, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read file info: %w", err)
	}
	infoLine = strings.TrimSuffix(infoLine, "\n")

	// 解析文件信息：格式 "0644 12345 filename"
	parts := strings.Fields(infoLine)
	if len(parts) < 3 {
		return fmt.Errorf("invalid scp file info format: %s", infoLine)
	}

	// 解析文件大小
	fileSize, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return fmt.Errorf("failed to parse file size: %w", err)
	}

	fileName := parts[2]
	_ = fileName // 可用于验证

	// 发送确认 (0x00)
	if _, err := stdin.Write([]byte{0}); err != nil {
		return fmt.Errorf("failed to send ack: %w", err)
	}

	// 创建本地文件
	localFile, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("failed to create local file: %w", err)
	}
	defer localFile.Close()

	// 使用 LimitedReader 确保只读取文件大小字节
	limitedReader := io.LimitReader(reader, fileSize)
	written, err := io.Copy(localFile, limitedReader)
	if err != nil {
		return fmt.Errorf("failed to copy file content: %w", err)
	}

	if written != fileSize {
		return fmt.Errorf("file size mismatch: expected %d bytes, got %d bytes", fileSize, written)
	}

	// 读取结束标记 (0x00)
	endMarker := make([]byte, 1)
	if _, err := reader.Read(endMarker); err != nil {
		return fmt.Errorf("failed to read end marker: %w", err)
	}

	if endMarker[0] != 0 {
		return fmt.Errorf("unexpected end marker: %x", endMarker[0])
	}

	// 发送最终确认
	if _, err := stdin.Write([]byte{0}); err != nil {
		return fmt.Errorf("failed to send final ack: %w", err)
	}

	if err := session.Wait(); err != nil {
		return fmt.Errorf("scp session failed: %w", err)
	}

	// 可选：保留远程文件的权限
	// 可以通过 stat 命令获取权限并应用到本地文件
	if types.ExecOptions.Verbose {
		e.mu.Lock()
		fmt.Printf("Downloaded %s (%d bytes) to %s\n", remotePath, fileSize, localPath)
		e.mu.Unlock()
	}

	return nil
}

// downloadFileViaSFTP 使用 SFTP 下载文件（增强版）
// downloadFileViaSFTP 支持断点续传的下载
func (e *Executor) downloadFileViaSFTP(remotePath, localPath string) error {
	// 创建 SFTP 客户端
	sftpClient, err := sftp.NewClient(e.client)
	if err != nil {
		return fmt.Errorf("failed to create sftp client: %w", err)
	}
	defer sftpClient.Close()

	// 打开远程文件
	remoteFile, err := sftpClient.Open(remotePath)
	if err != nil {
		return fmt.Errorf("failed to open remote file %s: %w", remotePath, err)
	}
	defer remoteFile.Close()

	// 获取远程文件信息
	remoteInfo, err := remoteFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat remote file: %w", err)
	}

	// 检查本地文件
	var localSize int64 = 0
	var localFile *os.File

	if localInfo, err := os.Stat(localPath); err == nil {
		localSize = localInfo.Size()

		// 如果本地文件大小等于远程文件，验证完整性
		if localSize == remoteInfo.Size() {
			if valid, err := e.verifyFileIntegrity(remotePath, localPath); err == nil && valid {
				if types.ExecOptions.Verbose {
					e.mu.Lock()
					fmt.Printf("    │ File already complete, skipping: %s\n", filepath.Base(remotePath))
					e.mu.Unlock()
				}
				return nil
			}
		}

		// 如果本地文件小于远程文件，断点续传
		if localSize > 0 && localSize < remoteInfo.Size() {
			if types.ExecOptions.Verbose {
				e.mu.Lock()
				fmt.Printf("    │ Resuming download from %d/%d bytes (%.1f%%)\n",
					localSize, remoteInfo.Size(), float64(localSize)/float64(remoteInfo.Size())*100)
				e.mu.Unlock()
			}

			// 打开本地文件用于追加
			localFile, err = os.OpenFile(localPath, os.O_APPEND|os.O_WRONLY, 0644)
			if err != nil {
				return fmt.Errorf("failed to open local file for append: %w", err)
			}
			defer localFile.Close()
		} else if localSize > remoteInfo.Size() {
			// 本地文件比远程大，说明可能有问题，重新下载
			if types.ExecOptions.Verbose {
				e.mu.Lock()
				fmt.Printf("    │ Local file larger than remote, re-downloading\n")
				e.mu.Unlock()
			}
			os.Remove(localPath)
			localSize = 0
		}
	}

	// 如果 localFile 为 nil，创建新文件
	if localFile == nil {
		localFile, err = os.Create(localPath)
		if err != nil {
			return fmt.Errorf("failed to create local file: %w", err)
		}
		defer localFile.Close()
	}

	// 从断点处开始读取
	if localSize > 0 {
		// 设置远程文件读取位置
		_, err = remoteFile.Seek(localSize, io.SeekStart)
		if err != nil {
			return fmt.Errorf("failed to seek remote file: %w", err)
		}
	}

	// 使用缓冲区下载
	buffer := make([]byte, 32*1024) // 32KB buffer
	totalWritten := localSize
	lastReport := time.Now()

	for {
		// 设置读取超时

		n, err := remoteFile.Read(buffer)
		if n > 0 {
			nw, err := localFile.Write(buffer[:n])
			if err != nil {
				return fmt.Errorf("failed to write to local file: %w", err)
			}
			totalWritten += int64(nw)

			// 每5秒报告一次进度
			if time.Since(lastReport) > 5*time.Second {
				progress := float64(totalWritten) / float64(remoteInfo.Size()) * 100
				if types.ExecOptions.Verbose {
					e.mu.Lock()
					fmt.Printf("    │ Progress: %.1f%% (%d/%d bytes)\n",
						progress, totalWritten, remoteInfo.Size())
					e.mu.Unlock()
				}
				lastReport = time.Now()
			}
		}

		if err != nil {
			if err == io.EOF {
				break // 下载完成
			}
			// 超时或连接错误，可以重试
			if strings.Contains(err.Error(), "timeout") || strings.Contains(err.Error(), "deadline") {
				if types.ExecOptions.Verbose {
					e.mu.Lock()
					fmt.Printf("    │ Timeout, will retry... (saved %d/%d bytes)\n",
						totalWritten, remoteInfo.Size())
					e.mu.Unlock()
				}
				// 返回部分下载的错误，让上层重试
				return fmt.Errorf("partial download: saved %d/%d bytes", totalWritten, remoteInfo.Size())
			}
			return fmt.Errorf("failed to read remote file: %w", err)
		}
	}

	// 确保文件已写入磁盘
	if err := localFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	// 验证下载完整性
	if totalWritten != remoteInfo.Size() {
		return fmt.Errorf("incomplete download: expected %d bytes, got %d bytes",
			remoteInfo.Size(), totalWritten)
	}

	// 最终验证
	if valid, err := e.verifyFileIntegrity(remotePath, localPath); err != nil || !valid {
		if err != nil {
			return fmt.Errorf("integrity check failed: %w", err)
		}
		return fmt.Errorf("integrity check failed: file may be corrupted")
	}

	// 设置文件权限和修改时间
	if err := os.Chmod(localPath, remoteInfo.Mode()); err != nil {
		if types.ExecOptions.Verbose {
			e.mu.Lock()
			fmt.Printf("    │ Warning: failed to set permissions: %v\n", err)
			e.mu.Unlock()
		}
	}

	if err := os.Chtimes(localPath, remoteInfo.ModTime(), remoteInfo.ModTime()); err != nil {
		if types.ExecOptions.Verbose {
			e.mu.Lock()
			fmt.Printf("    │ Warning: failed to set modification time: %v\n", err)
			e.mu.Unlock()
		}
	}

	if types.ExecOptions.Verbose {
		e.mu.Lock()
		fmt.Printf("    │ ✓ Download complete: %.2f MB\n", float64(totalWritten)/(1024*1024))
		e.mu.Unlock()
	}

	return nil
}

// verifyFileIntegrity 验证文件完整性（通过比较文件大小和可选的内容校验）
func (e *Executor) verifyFileIntegrity(remotePath, localPath string) (bool, error) {
	// 获取远程文件信息
	remoteCmd := fmt.Sprintf("stat -c '%%s' '%s'", remotePath)
	remoteSizeStr, err := e.executeCommandWithNewSession(remoteCmd)
	if err != nil {
		return false, err
	}
	remoteSize, _ := strconv.ParseInt(strings.TrimSpace(remoteSizeStr), 10, 64)

	// 获取本地文件信息
	localInfo, err := os.Stat(localPath)
	if err != nil {
		return false, err
	}

	if localInfo.Size() != remoteSize {
		return false, nil
	}

	// 对于大文件，只检查文件头尾的校验和（可选）
	// 对于小文件，可以计算完整 MD5
	if remoteSize < 100*1024*1024 { // 小于 100MB 的文件
		remoteMD5, err := e.getRemoteMD5(remotePath)
		if err == nil {
			localMD5, err := getLocalMD5(localPath)
			if err == nil && remoteMD5 != localMD5 {
				return false, nil
			}
		}
	}

	return true, nil
}

// compareFileChecksum 比较远程和本地文件的 MD5 校验和
func (e *Executor) compareFileChecksum(remotePath, localPath string) (bool, error) {
	// 获取远程文件的 MD5
	remoteMD5, err := e.getRemoteMD5(remotePath)
	if err != nil {
		return true, err
	}

	// 获取本地文件的 MD5
	localMD5, err := getLocalMD5(localPath)
	if err != nil {
		return true, err
	}

	// 如果 MD5 相同，文件内容一致
	if remoteMD5 == localMD5 {
		return false, nil
	}

	return true, nil
}

// getRemoteMD5 获取远程文件的 MD5 值
func (e *Executor) getRemoteMD5(remotePath string) (string, error) {
	// 尝试使用 md5sum
	md5Cmd := fmt.Sprintf("md5sum '%s' 2>/dev/null | cut -d' ' -f1", remotePath)
	output, err := e.executeCommandWithNewSession(md5Cmd)
	if err == nil && strings.TrimSpace(output) != "" {
		return strings.TrimSpace(output), nil
	}

	// 尝试使用 md5（BSD 系统）
	md5Cmd = fmt.Sprintf("md5 '%s' 2>/dev/null | awk '{print $NF}'", remotePath)
	output, err = e.executeCommandWithNewSession(md5Cmd)
	if err == nil && strings.TrimSpace(output) != "" {
		return strings.TrimSpace(output), nil
	}

	// 如果都失败，返回错误
	return "", fmt.Errorf("failed to get remote MD5 for %s", remotePath)
}

// getLocalMD5 获取本地文件的 MD5 值
func getLocalMD5(localPath string) (string, error) {
	file, err := os.Open(localPath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}
