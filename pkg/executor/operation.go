package executor

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/fgouteroux/sshot/pkg/types"
)

// executeFile 执行文件操作
func (e *Executor) executeFile(fileTask *types.FileTask) (string, error) {
	writer := e.OutputWriter
	if writer == nil {
		writer = os.Stdout
	}

	path := e.SubstituteVars(fileTask.Path)
	state := fileTask.State
	mode := fileTask.Mode

	// 转换远程路径格式
	path = toRemotePath(path)

	if types.ExecOptions.Verbose {
		e.mu.Lock()
		fmt.Fprintf(writer, "    │ File operation: state=%s, path=%s\n", state, path)
		if mode != "" {
			fmt.Fprintf(writer, "    │ Mode: %s\n", mode)
		}
		e.mu.Unlock()
	}

	switch state {
	case "touch":
		return e.fileTouch(path, mode)
	case "absent":
		return e.fileAbsent(path)
	case "directory":
		return e.fileDirectory(path, mode)
	default:
		return "", fmt.Errorf("unknown file state: %s (supported: touch, absent, directory)", state)
	}
}

// fileTouch 创建空文件，如果文件已存在则更新修改时间
func (e *Executor) fileTouch(path string, mode string) (string, error) {
	// 检查文件是否存在
	checkCmd := fmt.Sprintf("if [ -f '%s' ]; then echo 'exists'; else echo 'notfound'; fi", path)
	exists, err := e.executeCommandWithNewSession(checkCmd)
	if err != nil {
		return "", fmt.Errorf("failed to check file existence: %w", err)
	}
	exists = strings.TrimSpace(exists)

	var output string
	if exists == "exists" {
		// 文件存在，更新修改时间
		touchCmd := fmt.Sprintf("touch '%s'", path)
		if _, err := e.executeCommand(touchCmd, false); err != nil {
			return "", fmt.Errorf("failed to touch file: %w", err)
		}
		output = fmt.Sprintf("Updated timestamp of %s", path)
		if types.ExecOptions.Verbose {
			e.mu.Lock()
			fmt.Printf("    │ Updated timestamp: %s\n", path)
			e.mu.Unlock()
		}
	} else {
		// 文件不存在，创建空文件
		// 先确保目录存在
		dirCmd := fmt.Sprintf("mkdir -p '%s'", filepath.Dir(path))
		if _, err := e.executeCommand(dirCmd, false); err != nil {
			return "", fmt.Errorf("failed to create directory: %w", err)
		}

		// 创建空文件
		createCmd := fmt.Sprintf("touch '%s'", path)
		if _, err := e.executeCommand(createCmd, false); err != nil {
			return "", fmt.Errorf("failed to create file: %w", err)
		}
		output = fmt.Sprintf("Created empty file %s", path)
		if types.ExecOptions.Verbose {
			e.mu.Lock()
			fmt.Printf("    │ Created empty file: %s\n", path)
			e.mu.Unlock()
		}
	}

	// 设置文件权限
	if mode != "" {
		chmodCmd := fmt.Sprintf("chmod %s '%s'", mode, path)
		if _, err := e.executeCommand(chmodCmd, false); err != nil {
			return output, fmt.Errorf("file created but failed to set mode: %w", err)
		}
		output += fmt.Sprintf(" with mode %s", mode)
		if types.ExecOptions.Verbose {
			e.mu.Lock()
			fmt.Printf("    │ Set mode: %s\n", mode)
			e.mu.Unlock()
		}
	}

	return output, nil
}

// fileAbsent 删除文件或目录
func (e *Executor) fileAbsent(path string) (string, error) {
	// 检查路径是否存在
	checkCmd := fmt.Sprintf("if [ -e '%s' ]; then echo 'exists'; else echo 'notfound'; fi", path)
	exists, err := e.executeCommandWithNewSession(checkCmd)
	if err != nil {
		return "", fmt.Errorf("failed to check path existence: %w", err)
	}
	exists = strings.TrimSpace(exists)

	if exists == "notfound" {
		return fmt.Sprintf("File %s does not exist, nothing to delete", path), nil
	}

	// 判断是文件还是目录
	typeCmd := fmt.Sprintf("if [ -f '%s' ]; then echo 'file'; elif [ -d '%s' ]; then echo 'directory'; else echo 'other'; fi", path, path)
	fileType, err := e.executeCommandWithNewSession(typeCmd)
	if err != nil {
		return "", fmt.Errorf("failed to determine path type: %w", err)
	}
	fileType = strings.TrimSpace(fileType)

	var rmCmd string
	switch fileType {
	case "file":
		rmCmd = fmt.Sprintf("rm -f '%s'", path)
	case "directory":
		rmCmd = fmt.Sprintf("rm -rf '%s'", path)
	default:
		return "", fmt.Errorf("cannot delete: %s is not a regular file or directory", path)
	}

	if _, err := e.executeCommand(rmCmd, false); err != nil {
		return "", fmt.Errorf("failed to delete %s: %w", path, err)
	}

	if types.ExecOptions.Verbose {
		e.mu.Lock()
		fmt.Printf("    │ Deleted: %s (%s)\n", path, fileType)
		e.mu.Unlock()
	}

	return fmt.Sprintf("Deleted %s", path), nil
}

// fileDirectory 创建目录
func (e *Executor) fileDirectory(path string, mode string) (string, error) {
	// 检查目录是否已存在
	checkCmd := fmt.Sprintf("if [ -d '%s' ]; then echo 'exists'; else echo 'notfound'; fi", path)
	exists, err := e.executeCommandWithNewSession(checkCmd)
	if err != nil {
		return "", fmt.Errorf("failed to check directory existence: %w", err)
	}
	exists = strings.TrimSpace(exists)

	var output string
	if exists == "exists" {
		output = fmt.Sprintf("Directory %s already exists", path)
		if types.ExecOptions.Verbose {
			e.mu.Lock()
			fmt.Printf("    │ Directory exists: %s\n", path)
			e.mu.Unlock()
		}
	} else {
		// 创建目录（包括父目录）
		mkdirCmd := fmt.Sprintf("mkdir -p '%s'", path)
		if _, err := e.executeCommand(mkdirCmd, false); err != nil {
			return "", fmt.Errorf("failed to create directory: %w", err)
		}
		output = fmt.Sprintf("Created directory %s", path)
		if types.ExecOptions.Verbose {
			e.mu.Lock()
			fmt.Printf("    │ Created directory: %s\n", path)
			e.mu.Unlock()
		}
	}

	// 设置目录权限
	if mode != "" {
		chmodCmd := fmt.Sprintf("chmod %s '%s'", mode, path)
		if _, err := e.executeCommand(chmodCmd, false); err != nil {
			return output, fmt.Errorf("directory created but failed to set mode: %w", err)
		}
		output += fmt.Sprintf(" with mode %s", mode)
		if types.ExecOptions.Verbose {
			e.mu.Lock()
			fmt.Printf("    │ Set mode: %s\n", mode)
			e.mu.Unlock()
		}
	}

	return output, nil
}

func (e *Executor) executeSetFact(setFactTask *types.SetFactTask) (string, error) {
	for key, value := range setFactTask.Vars {
		strVal := fmt.Sprintf("%v", value)
		substituted := e.SubstituteVars(strVal)
		e.Variables[key] = substituted
		if types.ExecOptions.Verbose {
			fmt.Fprintf(e.OutputWriter, "    │ set_fact: %s = %s\n", key, substituted)
		}
	}
	return "set_fact completed", nil
}
