package executor

import (
	"bytes"
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
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fgouteroux/sshot/pkg/types"
	"github.com/fgouteroux/sshot/pkg/utils"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
	"golang.org/x/crypto/ssh/knownhosts"
)

// 本地操作系统类型
var localOS = runtime.GOOS

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

	NotifyQueue  []string        // 新增：待执行的 handler 队列
	HandlersDone map[string]bool // 新增：记录已执行的 handlers
}

// Singleton SSH agent client
var (
	sshAgentOnce   sync.Once
	sshAgentClient agent.ExtendedAgent
)

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

		var factData map[string]interface{}
		if err := json.Unmarshal([]byte(output), &factData); err != nil {
			e.Variables[collector.Name] = output
			if types.ExecOptions.Verbose {
				fmt.Fprintf(writer, "    %s→%s Stored as string (not JSON)\n",
					utils.Color(utils.ColorGray), utils.Color(utils.ColorReset))
			}
		} else {
			e.Variables[collector.Name] = factData
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

func FlattenMap(data map[string]interface{}, prefix string) map[string]string {
	items := make(map[string]string)
	for k, v := range data {
		key := prefix + k
		switch typedVal := v.(type) {
		case map[string]interface{}:
			subItems := FlattenMap(typedVal, key+".")
			for sk, sv := range subItems {
				items[sk] = sv
			}
		case []interface{}:
			if jsonBytes, err := json.Marshal(typedVal); err == nil {
				items[key] = string(jsonBytes)
			}
		case string, float64, bool, int:
			items[key] = fmt.Sprintf("%v", typedVal)
		}
	}
	return items
}

func (e *Executor) ExecuteTask(task types.Task, handlers []types.Task) error {
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

	// Group restrictions
	if len(task.OnlyGroups) > 0 {
		groupAllowed := false
		for _, group := range task.OnlyGroups {
			if group == e.GroupName {
				groupAllowed = true
				break
			}
		}
		if !groupAllowed {
			fmt.Fprintf(writer, "  ⊘ Skipped (not in allowed groups: %v)\n", task.OnlyGroups)
			return nil
		}
	}

	if len(task.SkipGroups) > 0 {
		for _, group := range task.SkipGroups {
			if group == e.GroupName {
				fmt.Fprintf(writer, "  ⊘ Skipped (in excluded group: %s)\n", group)
				return nil
			}
		}
	}

	if task.DelegateTo != "" && task.DelegateTo != e.Host.Name && task.DelegateTo != "localhost" {
		e.mu.Lock()
		fmt.Fprintf(writer, "  ↷ Skipped (delegated to: %s)\n", task.DelegateTo)
		e.CompletedTasks[task.Name] = true
		e.mu.Unlock()
		return nil
	}

	if task.DelegateTo == "localhost" && task.Command != "" {
		task.LocalAction = task.Command
		task.Command = ""
	}

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
		types.RunOnceTasks.Lock()
		types.RunOnceTasks.Executed[taskKey] = true
		types.RunOnceTasks.Unlock()
	}

	if len(task.DependsOn) > 0 {
		for _, dep := range task.DependsOn {
			if !e.CompletedTasks[dep] {
				return fmt.Errorf("dependency not met: task '%s' depends on '%s'", task.Name, dep)
			}
		}
	}

	if task.Vars != nil {
		for k, v := range task.Vars {
			e.Variables[k] = v
		}
	}

	if task.When != "" {
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
		if task.Copy != nil {
			fmt.Fprintf(writer, "      Copy: %s → %s\n", task.Copy.Src, e.SubstituteVars(task.Copy.Dest))
		}
		if task.Fetch != nil {
			fmt.Fprintf(writer, "      Fetch: %s → %s\n", task.Fetch.Src, e.SubstituteVars(task.Fetch.Dest))
		}
		e.CompletedTasks[task.Name] = true
		e.mu.Unlock()
		return nil
	}

	// Execute task
	switch {
	case task.Command != "":
		output, err = e.executeCommand(task.Command, task.Sudo)
		if err != nil && len(task.AllowedExitCodes) > 0 && e.isAllowedExitCode(err, task.AllowedExitCodes) {
			err = nil
		}
	case task.Shell != "":
		output, err = e.executeCommand(task.Shell, task.Sudo)
		if err != nil && len(task.AllowedExitCodes) > 0 && e.isAllowedExitCode(err, task.AllowedExitCodes) {
			err = nil
		}
	case task.Script != "":
		output, err = e.executeScript(task.Script, task.Sudo)
	case task.LocalAction != "":
		output, err = e.executeLocalAction(task.LocalAction)
	case task.Copy != nil:
		output, err = e.executeCopy(task.Copy)
	case task.Fetch != nil:
		output, err = e.executeFetch(task.Fetch)
	case task.File != nil:
		output, err = e.executeFile(task.File)
	case task.Systemd != nil:
		output, err = e.executeSystemd(task.Systemd)
	case task.Archive != nil:
		output, err = e.executeArchive(task.Archive)
	case task.WaitFor != "":
		output, err = e.executeWaitFor(task.WaitFor)
	default:
		return fmt.Errorf("no executable task type defined")
	}

	if task.Register != "" && output != "" {
		e.Registers[task.Register] = output
		e.Variables[task.Register] = output
		if types.ExecOptions.Verbose {
			e.mu.Lock()
			log.Printf("[VERBOSE] [%s] Registered output to: %s", e.Host.Name, task.Register)
			e.mu.Unlock()
		}
	}

	// 任务执行成功后，处理 notify
	if err == nil && len(task.Notify) > 0 {
		e.mu.Lock()
		for _, handlerName := range task.Notify {
			// 检查 handler 是否存在
			handlerExists := false
			for _, h := range handlers {
				if h.Name == handlerName {
					handlerExists = true
					break
				}
			}
			if !handlerExists {
				e.mu.Unlock()
				return fmt.Errorf("handler '%s' not found", handlerName)
			}

			// 添加到通知队列（去重）
			alreadyQueued := false
			for _, queued := range e.NotifyQueue {
				if queued == handlerName {
					alreadyQueued = true
					break
				}
			}
			if !alreadyQueued && !e.HandlersDone[handlerName] {
				e.NotifyQueue = append(e.NotifyQueue, handlerName)
				if types.ExecOptions.Verbose {
					fmt.Fprintf(writer, "    │ Notified handler: %s\n", handlerName)
				}
			}
		}
		e.mu.Unlock()
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
		if output != "" {
			e.mu.Lock()
			e.printOutput(writer, output)
			e.mu.Unlock()
		}
		return err
	}

	e.mu.Lock()
	fmt.Fprintf(writer, "  %s✓ Success%s\n", utils.Color(utils.ColorGreen), utils.Color(utils.ColorReset))
	if output != "" {
		e.printOutput(writer, output)
	}
	e.CompletedTasks[task.Name] = true
	e.mu.Unlock()

	return nil
}

func (e *Executor) executeCommand(cmd string, sudo bool) (string, error) {
	cmd = e.SubstituteVars(cmd)
	if sudo {
		cmd = "sudo -S " + cmd
	}

	if types.ExecOptions.Verbose {
		e.mu.Lock()
		log.Printf("[VERBOSE] [%s] Executing: %s", e.Host.Name, cmd)
		e.mu.Unlock()
	}

	if types.ExecOptions.DryRun {
		if strings.HasPrefix(cmd, "echo ") {
			content := cmd[5:]
			if (strings.HasPrefix(content, "'") && strings.HasSuffix(content, "'")) ||
				(strings.HasPrefix(content, "\"") && strings.HasSuffix(content, "\"")) {
				return content[1 : len(content)-1], nil
			}
			return content, nil
		}
		return "DRY-RUN: Command would execute", nil
	}

	session, err := e.client.NewSession()
	if err != nil {
		return "", fmt.Errorf("failed to create session: %w", err)
	}
	defer session.Close()

	var stdout, stderr bytes.Buffer
	session.Stdout = &stdout
	session.Stderr = &stderr

	err = session.Run(cmd)
	output := stdout.String()
	if stderr.Len() > 0 {
		output += "\nSTDERR: " + stderr.String()
	}

	if err != nil {
		return output, fmt.Errorf("command failed: %w", err)
	}
	return strings.TrimSpace(output), nil
}

func (e *Executor) executeCommandWithNewSession(cmd string) (string, error) {
	session, err := e.client.NewSession()
	if err != nil {
		return "", fmt.Errorf("failed to create session: %w", err)
	}
	defer session.Close()

	var stdout bytes.Buffer
	session.Stdout = &stdout
	err = session.Run(cmd)
	return strings.TrimSpace(stdout.String()), err
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
		return "", err
	}
	stdin.Close()
	if err := session.Wait(); err != nil {
		return "", fmt.Errorf("failed to upload script: %w", err)
	}

	execCmd := tmpFile
	if sudo {
		execCmd = "sudo " + tmpFile
	}
	output, err := e.executeCommand(execCmd, false)
	e.executeCommand(fmt.Sprintf("rm -f %s", tmpFile), false)
	return output, err
}

func (e *Executor) executeCopy(copyTask *types.CopyTask) (string, error) {
	// 本地源路径转换
	src := e.SubstituteVars(copyTask.Src)
	src = toLocalPath(src)

	// 远程目标路径转换
	dest := e.SubstituteVars(copyTask.Dest)
	dest = toRemotePath(dest)

	if types.ExecOptions.Verbose {
		e.mu.Lock()
		fmt.Printf("    │ Copy src (local): %s\n", src)
		fmt.Printf("    │ Copy dest (remote): %s\n", dest)
		e.mu.Unlock()
	}

	content, err := os.ReadFile(src)
	if err != nil {
		return "", fmt.Errorf("failed to read source file: %w", err)
	}
	contentStr := e.SubstituteVars(string(content))

	// 创建远程目录
	destDir := filepath.Dir(dest)
	mkdirCmd := fmt.Sprintf("mkdir -p '%s'", destDir)
	if _, err := e.executeCommand(mkdirCmd, false); err != nil {
		return "", fmt.Errorf("failed to create remote directory: %w", err)
	}

	session, err := e.client.NewSession()
	if err != nil {
		return "", fmt.Errorf("failed to create session: %w", err)
	}
	defer session.Close()

	cmd := fmt.Sprintf("cat > '%s'", dest)
	stdin, err := session.StdinPipe()
	if err != nil {
		return "", err
	}
	if err := session.Start(cmd); err != nil {
		return "", err
	}
	_, err = io.WriteString(stdin, contentStr)
	if err != nil {
		return "", err
	}
	stdin.Close()
	if err := session.Wait(); err != nil {
		return "", fmt.Errorf("failed to copy file: %w", err)
	}

	if copyTask.Mode != "" {
		_, err = e.executeCommand(fmt.Sprintf("chmod %s '%s'", copyTask.Mode, dest), false)
		if err != nil {
			return "", err
		}
	}
	return fmt.Sprintf("Copied %s to %s", src, dest), nil
}

func (e *Executor) executeWaitFor(condition string) (string, error) {
	parts := strings.SplitN(condition, ":", 2)
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid wait_for format: %s", condition)
	}
	waitType, waitValue := parts[0], parts[1]

	for i := 0; i < 30; i++ {
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
		time.Sleep(2 * time.Second)
	}
	return "", fmt.Errorf("timeout waiting for: %s", condition)
}

func (e *Executor) executeLocalAction(cmd string) (string, error) {
	cmd = e.SubstituteVars(cmd)
	command := exec.Command("/bin/sh", "-c", cmd)
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	err := command.Run()
	output := stdout.String()
	if stderr.Len() > 0 {
		output += "\nSTDERR: " + stderr.String()
	}
	return output, err
}

func (e *Executor) executeDelegated(cmd string, delegateHost string) (string, error) {
	if delegateHost == "localhost" || delegateHost == "127.0.0.1" {
		return e.executeLocalAction(cmd)
	}
	return "", fmt.Errorf("delegation to %s not supported", delegateHost)
}

func (e *Executor) executeCommandStreaming(session *ssh.Session, cmd string, writer io.Writer) (string, error) {
	var outputBuf bytes.Buffer
	stdout, _ := session.StdoutPipe()
	stderr, _ := session.StderrPipe()
	session.Start(cmd)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		buf := make([]byte, 4096)
		for {
			n, err := stdout.Read(buf)
			if n > 0 {
				outputBuf.Write(buf[:n])
				e.mu.Lock()
				writer.Write([]byte("    │ "))
				writer.Write(buf[:n])
				e.mu.Unlock()
			}
			if err != nil {
				break
			}
		}
	}()
	go func() {
		defer wg.Done()
		buf := make([]byte, 4096)
		for {
			n, err := stderr.Read(buf)
			if n > 0 {
				outputBuf.Write(buf[:n])
				e.mu.Lock()
				writer.Write([]byte("    │ [stderr] "))
				writer.Write(buf[:n])
				e.mu.Unlock()
			}
			if err != nil {
				break
			}
		}
	}()
	err := session.Wait()
	wg.Wait()
	return outputBuf.String(), err
}

func (e *Executor) executeFetch(fetchTask *types.FetchTask) (string, error) {
	writer := e.OutputWriter
	if writer == nil {
		writer = os.Stdout
	}

	src := e.SubstituteVars(fetchTask.Src)
	src = toRemotePath(src)

	dest := e.SubstituteVars(fetchTask.Dest)
	dest = toLocalPath(dest)

	safeHostname := sanitizePathComponent(e.Host.Name)
	fullDest := filepath.Join(dest, safeHostname)

	if err := os.MkdirAll(fullDest, 0755); err != nil {
		return "", fmt.Errorf("failed to create directory: %w", err)
	}

	if types.ExecOptions.Verbose {
		e.mu.Lock()
		fmt.Fprintf(writer, "    │ Fetch src: %s\n", src)
		fmt.Fprintf(writer, "    │ Fetch dest: %s\n", fullDest)
		e.mu.Unlock()
	}

	checkCmd := fmt.Sprintf("if [ -d '%s' ]; then echo 'directory'; elif [ -f '%s' ]; then echo 'file'; else echo 'notfound'; fi", src, src)
	fileType, err := e.executeCommandWithNewSession(checkCmd)
	if err != nil {
		return "", err
	}
	fileType = strings.TrimSpace(fileType)

	switch fileType {
	case "notfound":
		return "", fmt.Errorf("remote source not found: %s", src)
	case "directory":
		return e.fetchDirectory(src, fullDest, fetchTask.Flat)
	case "file":
		return e.fetchFile(src, fullDest, fetchTask.Flat)
	}
	return "", fmt.Errorf("unknown type: %s", fileType)
}

func (e *Executor) fetchFile(remotePath, localPath string, flat bool) (string, error) {
	var targetPath string
	if flat {
		targetPath = filepath.Join(localPath, filepath.Base(remotePath))
	} else {
		cleanPath := strings.TrimPrefix(remotePath, "/")
		targetPath = filepath.Join(localPath, cleanPath)
	}

	if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
		return "", err
	}

	if err := e.downloadFileViaSFTP(remotePath, targetPath); err != nil {
		return "", err
	}

	info, _ := os.Stat(targetPath)
	return fmt.Sprintf("Fetched %s (%.2f KB)", remotePath, float64(info.Size())/1024), nil
}

func (e *Executor) fetchDirectory(remotePath, localPath string, flat bool) (string, error) {
	if flat {
		return "", fmt.Errorf("cannot fetch directory with flat=true")
	}

	listCmd := fmt.Sprintf("find '%s' -type f 2>/dev/null | sort", remotePath)
	filesStr, err := e.executeCommandWithNewSession(listCmd)
	if err != nil {
		return "", err
	}

	files := strings.Split(strings.TrimSpace(filesStr), "\n")
	if len(files) == 0 || files[0] == "" {
		return "", fmt.Errorf("no files found")
	}

	success := 0
	for i, f := range files {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		relPath, _ := filepath.Rel(remotePath, f)
		localFile := filepath.Join(localPath, relPath)
		os.MkdirAll(filepath.Dir(localFile), 0755)

		if types.ExecOptions.Verbose {
			e.mu.Lock()
			fmt.Printf("    │ [%d/%d] Downloading: %s\n", i+1, len(files), filepath.Base(f))
			e.mu.Unlock()
		}

		if err := e.downloadFileViaSFTP(f, localFile); err != nil {
			e.mu.Lock()
			fmt.Printf("    │ ✗ Failed: %s\n", filepath.Base(f))
			e.mu.Unlock()
			continue
		}
		success++
	}

	return fmt.Sprintf("Fetched %d/%d files", success, len(files)), nil
}

func (e *Executor) downloadFileViaSFTP(remotePath, localPath string) error {
	sftpClient, err := sftp.NewClient(e.client)
	if err != nil {
		return err
	}
	defer sftpClient.Close()

	remoteFile, err := sftpClient.Open(remotePath)
	if err != nil {
		return err
	}
	defer remoteFile.Close()

	remoteInfo, err := remoteFile.Stat()
	if err != nil {
		return err
	}

	// 检查本地文件
	if localInfo, err := os.Stat(localPath); err == nil {
		if localInfo.Size() == remoteInfo.Size() {
			if valid, _ := e.verifyFileIntegrity(remotePath, localPath); valid {
				if types.ExecOptions.Verbose {
					e.mu.Lock()
					fmt.Printf("    │ Skipping (already exists): %s\n", filepath.Base(remotePath))
					e.mu.Unlock()
				}
				return nil
			}
		}
	}

	localFile, err := os.Create(localPath)
	if err != nil {
		return err
	}
	defer localFile.Close()

	_, err = io.Copy(localFile, remoteFile)
	if err != nil {
		return err
	}

	os.Chmod(localPath, remoteInfo.Mode())
	os.Chtimes(localPath, remoteInfo.ModTime(), remoteInfo.ModTime())
	return nil
}

func (e *Executor) verifyFileIntegrity(remotePath, localPath string) (bool, error) {
	remoteSizeStr, _ := e.executeCommandWithNewSession(fmt.Sprintf("stat -c '%%s' '%s'", remotePath))
	remoteSize, _ := strconv.ParseInt(strings.TrimSpace(remoteSizeStr), 10, 64)

	localInfo, err := os.Stat(localPath)
	if err != nil || localInfo.Size() != remoteSize {
		return false, nil
	}

	if remoteSize < 100*1024*1024 {
		remoteMD5, _ := e.getRemoteMD5(remotePath)
		localMD5, _ := getLocalMD5(localPath)
		return remoteMD5 == localMD5, nil
	}
	return true, nil
}

func (e *Executor) getRemoteMD5(remotePath string) (string, error) {
	cmd := fmt.Sprintf("md5sum '%s' 2>/dev/null | cut -d' ' -f1", remotePath)
	out, err := e.executeCommandWithNewSession(cmd)
	if err == nil && out != "" {
		return strings.TrimSpace(out), nil
	}
	cmd = fmt.Sprintf("md5 '%s' 2>/dev/null | awk '{print $NF}'", remotePath)
	out, err = e.executeCommandWithNewSession(cmd)
	return strings.TrimSpace(out), err
}

func getLocalMD5(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := md5.New()
	io.Copy(h, f)
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func (e *Executor) SubstituteVars(text string) string {
	if text == "" {
		return text
	}
	allVars := e.getAllVariables()
	re := regexp.MustCompile(`{{\s*([^{}]+?)\s*}}`)
	return re.ReplaceAllStringFunc(text, func(match string) string {
		path := strings.TrimSpace(match[2 : len(match)-2])
		return fmt.Sprintf("%v", e.extractValueByPath(allVars, path))
	})
}

func (e *Executor) getAllVariables() map[string]interface{} {
	all := make(map[string]interface{})
	if e.Host.Vars != nil {
		for k, v := range e.Host.Vars {
			all[k] = v
		}
	}
	if e.Variables != nil {
		for k, v := range e.Variables {
			all[k] = v
		}
	}
	if e.Registers != nil {
		for k, v := range e.Registers {
			all[k] = v
		}
	}
	return all
}

func (e *Executor) extractValueByPath(data map[string]interface{}, path string) interface{} {
	if path == "" {
		return ""
	}
	parts := e.parsePath(path)
	var current interface{} = data
	for _, part := range parts {
		if current == nil {
			return ""
		}
		if strings.HasPrefix(part, "[") && strings.HasSuffix(part, "]") {
			idx, _ := strconv.Atoi(part[1 : len(part)-1])
			switch v := current.(type) {
			case []interface{}:
				if idx >= 0 && idx < len(v) {
					current = v[idx]
				} else {
					return ""
				}
			default:
				return ""
			}
		} else {
			switch v := current.(type) {
			case map[string]interface{}:
				if val, ok := v[part]; ok {
					current = val
				} else {
					return ""
				}
			default:
				return ""
			}
		}
	}
	return valueToString(current)
}

func (e *Executor) parsePath(path string) []string {
	var parts []string
	var cur strings.Builder
	inBracket := false
	for i := 0; i < len(path); i++ {
		ch := path[i]
		if ch == '[' {
			if cur.Len() > 0 {
				parts = append(parts, cur.String())
				cur.Reset()
			}
			inBracket = true
			cur.WriteByte(ch)
		} else if ch == ']' {
			cur.WriteByte(ch)
			parts = append(parts, cur.String())
			cur.Reset()
			inBracket = false
		} else if ch == '.' && !inBracket {
			if cur.Len() > 0 {
				parts = append(parts, cur.String())
				cur.Reset()
			}
		} else {
			cur.WriteByte(ch)
		}
	}
	if cur.Len() > 0 {
		parts = append(parts, cur.String())
	}
	return parts
}

func valueToString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case []interface{}, map[string]interface{}:
		b, _ := json.Marshal(v)
		return string(b)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func (e *Executor) evaluateCondition(condition string) bool {
	condition = strings.TrimSpace(condition)
	if strings.HasSuffix(condition, "is defined") {
		varName := strings.TrimSuffix(condition, "is defined")
		_, exists := e.Variables[strings.TrimSpace(varName)]
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

func (e *Executor) isAllowedExitCode(err error, allowedCodes []int) bool {
	if err == nil {
		return true
	}
	if exitErr, ok := err.(*ssh.ExitError); ok {
		code := exitErr.ExitStatus()
		for _, allowed := range allowedCodes {
			if code == allowed {
				return true
			}
		}
	}
	return false
}

func (e *Executor) printOutput(writer io.Writer, output string) {
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) == 1 {
		fmt.Fprintf(writer, "    Output: %s\n", lines[0])
	} else {
		fmt.Fprintf(writer, "    Output (%d lines):\n", len(lines))
		for _, line := range lines {
			fmt.Fprintf(writer, "      %s\n", line)
		}
	}
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

	hostKeyCallback, err := getHostKeyCallback(host.StrictHostKeyCheck)
	if err != nil {
		return nil, err
	}

	config := &ssh.ClientConfig{
		User:            host.User,
		HostKeyCallback: hostKeyCallback,
		Timeout:         10 * time.Second,
	}

	var authMethods []ssh.AuthMethod
	useAgent := host.UseAgent

	if useAgent || (host.KeyFile == "" && host.Password == "" && os.Getenv("SSH_AUTH_SOCK") != "") {
		if agentAuth := getSSHAgent(); agentAuth != nil {
			authMethods = append(authMethods, agentAuth)
		} else if useAgent {
			return nil, fmt.Errorf("ssh-agent not available")
		}
	}

	if host.KeyFile != "" {
		keyPath := host.KeyFile
		if strings.HasPrefix(keyPath, "~/") {
			home, _ := os.UserHomeDir()
			keyPath = strings.Replace(keyPath, "~", home, 1)
		}
		key, err := os.ReadFile(filepath.Clean(keyPath))
		if err != nil {
			return nil, err
		}
		var signer ssh.Signer
		if host.KeyPassword != "" {
			signer, err = ssh.ParsePrivateKeyWithPassphrase(key, []byte(host.KeyPassword))
		} else {
			signer, err = ssh.ParsePrivateKey(key)
		}
		if err != nil {
			return nil, err
		}
		authMethods = append(authMethods, ssh.PublicKeys(signer))
	}

	if host.Password != "" {
		authMethods = append(authMethods, ssh.Password(host.Password))
	}

	if len(authMethods) == 0 {
		return nil, fmt.Errorf("no authentication method")
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

	if types.ExecOptions.DryRun {
		return &Executor{
			Host:           host,
			Variables:      host.Vars,
			Registers:      make(map[string]string),
			CompletedTasks: make(map[string]bool),
			GroupName:      groupName,
			OutputWriter:   os.Stdout,
			StartTime:      time.Now(),
		}, nil
	}

	client, err := ssh.Dial("tcp", fmt.Sprintf("%s:%d", target, port), config)
	if err != nil {
		return nil, err
	}

	return &Executor{
		Host:           host,
		client:         client,
		Variables:      host.Vars,
		Registers:      make(map[string]string),
		CompletedTasks: make(map[string]bool),
		NotifyQueue:    make([]string, 0),     // 新增
		HandlersDone:   make(map[string]bool), // 新增
		GroupName:      groupName,
		OutputWriter:   os.Stdout,
		StartTime:      time.Now(),
	}, nil
}

func getSSHAgent() ssh.AuthMethod {
	sock := os.Getenv("SSH_AUTH_SOCK")
	if sock == "" {
		return nil
	}
	sshAgentOnce.Do(func() {
		conn, _ := net.Dial("unix", sock)
		if conn != nil {
			sshAgentClient = agent.NewClient(conn)
		}
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

func getHostKeyCallback(strict *bool) (ssh.HostKeyCallback, error) {
	strictMode := true
	if strict != nil {
		strictMode = *strict
	}
	if !strictMode {
		return ssh.InsecureIgnoreHostKey(), nil
	}
	home, _ := os.UserHomeDir()
	knownHostsPath := filepath.Join(home, ".ssh", "known_hosts")
	return knownhosts.New(knownHostsPath)
}

// 路径转换函数
func toLocalPath(path string) string {
	if localOS == "windows" {
		path = strings.ReplaceAll(path, "/", "\\")
		path = strings.TrimPrefix(path, "\\")
		if !strings.Contains(path, ":") && !filepath.IsAbs(path) {
			cwd, _ := os.Getwd()
			path = filepath.Join(cwd, path)
		}
	}
	return filepath.Clean(path)
}

func toRemotePath(path string) string {
	return strings.ReplaceAll(path, "\\", "/")
}

func sanitizePathComponent(name string) string {
	illegal := []string{"<", ">", ":", "\"", "/", "\\", "|", "?", "*"}
	result := name
	for _, ch := range illegal {
		result = strings.ReplaceAll(result, ch, "_")
	}
	result = strings.TrimSpace(result)
	result = strings.Trim(result, ".")
	if len(result) > 255 {
		result = result[:255]
	}
	return result
}
