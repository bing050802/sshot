package executor

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sshot/pkg/types"
	"strings"
	"time"
)

func (e *Executor) pathExists(path string) (bool, error) {
	cmd := fmt.Sprintf("if [ -e '%s' ]; then echo exists; else echo notexists; fi", path)
	out, err := e.executeCommand(cmd, false)
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(out) == "exists", nil
}

func (e *Executor) executeRemoteSync(task *types.RemoteSyncTask) (string, error) {
	writer := e.OutputWriter
	if writer == nil {
		writer = os.Stdout
	}

	src := e.SubstituteVars(task.Src)
	dest := e.SubstituteVars(task.Dest)

	// 1. 备份目标目录（如果存在）
	backupPath := ""
	backuped := false
	if task.Backup != "" && task.Backup != "none" {
		exists, err := e.pathExists(dest)
		if err != nil {
			return "", fmt.Errorf("failed to check dest existence: %w", err)
		}
		if exists {
			timestamp := time.Now().Format("20060102150405")
			baseName := filepath.Base(dest)

			// 确定备份存放目录
			backupDir := task.BackupDir
			if backupDir == "" {
				backupDir = filepath.Dir(dest) // 默认与原目录同级
			} else {
				// 确保备份目录存在
				mkdirCmd := fmt.Sprintf("mkdir -p '%s'", backupDir)
				if _, err := e.executeCommand(mkdirCmd, false); err != nil {
					return "", fmt.Errorf("failed to create backup directory: %w", err)
				}
			}

			if task.Backup == "move" {
				// 移动备份到指定目录
				backupPath = filepath.Join(backupDir, baseName+".bak_"+timestamp)
				cmd := fmt.Sprintf("mv '%s' '%s'", dest, backupPath)
				if _, err := e.executeCommand(cmd, false); err != nil {
					return "", fmt.Errorf("failed to move dest to backup: %w", err)
				}
				fmt.Fprintf(writer, "    │ Moved existing %s to %s\n", dest, backupPath)
				backuped = true
			} else if task.Backup == "tar" {
				// 打包备份到指定目录
				backupFile := baseName + ".bak_" + timestamp + ".tar.gz"
				backupPath = filepath.Join(backupDir, backupFile)
				cmd := fmt.Sprintf("cd '%s' && tar -czf '%s' '%s'", filepath.Dir(dest), backupPath, baseName)
				if _, err := e.executeCommand(cmd, false); err != nil {
					return "", fmt.Errorf("failed to create tar backup: %w", err)
				}
				fmt.Fprintf(writer, "    │ Archived existing %s to %s\n", dest, backupPath)
				backuped = true
			}
		}
	}

	// 2. 清空目标目录（仅在 backup == none 且 clean_dest 为 true 时）
	if task.Backup == "none" && task.CleanDest {
		exists, _ := e.pathExists(dest)
		if exists {
			if _, err := e.executeCommand(fmt.Sprintf("rm -rf '%s'/* '%s'/.[!.]* 2>/dev/null", dest, dest), false); err == nil {
				fmt.Fprintf(writer, "    │ Emptied destination directory: %s\n", dest)
			}
		}
	}

	// 3. 处理源：解压 tar.gz 或校验普通目录
	var effectiveSrc string
	var tempDir string
	var err error
	if strings.HasSuffix(src, ".tar.gz") || strings.HasSuffix(src, ".tgz") || strings.HasSuffix(src, ".zip") {
		tempDir, effectiveSrc, err = e.prepareArchiveSource(src, dest, task.SrcSubdir, task.Flat, writer)
		if err != nil {
			// 如果已经备份了目标，需要回滚
			if backuped {
				e.rollbackSync(dest, backupPath, task.Backup, writer)
			}
			return "", err
		}
		defer e.cleanupTempDir(tempDir, writer)
	} else {
		// 普通目录或文件
		exists, err := e.pathExists(src)
		if err != nil || !exists {
			return "", fmt.Errorf("source path does not exist: %s", src)
		}
		effectiveSrc = src
		// 如果是目录，确保以 / 结尾（表示复制目录内容）
		isDir, _ := e.executeCommand(fmt.Sprintf("test -d '%s' && echo dir", src), false)
		if strings.TrimSpace(isDir) == "dir" && !strings.HasSuffix(effectiveSrc, "/") {
			effectiveSrc = effectiveSrc + "/"
		}
	}

	// 4. 执行同步
	syncErr := e.performSync(effectiveSrc, dest, task.RsyncFlags, task.Exclude, writer)
	if syncErr != nil {
		// 同步失败，回滚
		if backuped {
			e.rollbackSync(dest, backupPath, task.Backup, writer)
		}
		return "", fmt.Errorf("sync failed: %w", syncErr)
	}

	// 5. 删除备份（如果要求）
	if task.DeleteBackup && backuped && backupPath != "" {
		rmCmd := fmt.Sprintf("rm -rf '%s'", backupPath)
		if _, err := e.executeCommand(rmCmd, false); err != nil {
			fmt.Fprintf(writer, "    │ Warning: failed to delete backup %s: %v\n", backupPath, err)
		} else {
			fmt.Fprintf(writer, "    │ Deleted backup: %s\n", backupPath)
		}
	}

	return fmt.Sprintf("Synced %s to %s", src, dest), nil
}

// prepareArchiveSource 解压 tar.gz 或 zip 文件，返回临时目录和有效源路径
func (e *Executor) prepareArchiveSource(src, dest, srcSubdir string, flat bool, writer io.Writer) (tempDir string, effectiveSrc string, err error) {
	tempDir = fmt.Sprintf("/tmp/sshot_sync_%d", time.Now().UnixNano())
	if _, err := e.executeCommand(fmt.Sprintf("mkdir -p '%s'", tempDir), false); err != nil {
		return "", "", fmt.Errorf("failed to create temp dir: %w", err)
	}

	var extractCmd string
	if strings.HasSuffix(src, ".tar.gz") || strings.HasSuffix(src, ".tgz") {
		extractCmd = fmt.Sprintf("tar -xzf '%s' -C '%s'", src, tempDir)
	} else if strings.HasSuffix(src, ".zip") {
		// 检查 unzip 命令是否存在
		hasUnzip, _ := e.executeCommand("command -v unzip", false)
		if strings.TrimSpace(hasUnzip) == "" {
			e.executeCommand(fmt.Sprintf("rm -rf '%s'", tempDir), false)
			return "", "", fmt.Errorf("unzip command not found on remote host, please install unzip")
		}
		extractCmd = fmt.Sprintf("unzip -o '%s' -d '%s'", src, tempDir)
	} else {
		e.executeCommand(fmt.Sprintf("rm -rf '%s'", tempDir), false)
		return "", "", fmt.Errorf("unsupported archive format: %s", src)
	}

	if _, err := e.executeCommand(extractCmd, false); err != nil {
		e.executeCommand(fmt.Sprintf("rm -rf '%s'", tempDir), false)
		return "", "", fmt.Errorf("failed to extract archive: %w", err)
	}
	fmt.Fprintf(writer, "    │ Extracted %s to %s\n", src, tempDir)

	// 如果指定了 flat，直接使用临时目录作为源
	if flat {
		effectiveSrc = tempDir + "/"
		fmt.Fprintf(writer, "    │ Flat mode: syncing all contents of archive directly to destination\n")
		e.executeCommand(fmt.Sprintf("mkdir -p '%s'", dest), false)
		return tempDir, effectiveSrc, nil
	}

	// 非 flat 模式：分析顶层结构
	itemsOut, _ := e.executeCommand(fmt.Sprintf("ls -1A '%s'", tempDir), false)
	items := strings.Split(strings.TrimSpace(itemsOut), "\n")
	var dirs, files []string
	for _, name := range items {
		if name == "" {
			continue
		}
		full := filepath.Join(tempDir, name)
		isDir, _ := e.executeCommand(fmt.Sprintf("test -d '%s' && echo dir", full), false)
		if strings.TrimSpace(isDir) == "dir" {
			dirs = append(dirs, name)
		} else {
			files = append(files, name)
		}
	}

	destBase := filepath.Base(dest)

	if srcSubdir != "" {
		candidate := filepath.Join(tempDir, srcSubdir)
		exists, _ := e.pathExists(candidate)
		if !exists {
			e.executeCommand(fmt.Sprintf("rm -rf '%s'", tempDir), false)
			return "", "", fmt.Errorf("src_subdir '%s' not found in archive", srcSubdir)
		}
		effectiveSrc = candidate + "/"
		fmt.Fprintf(writer, "    │ Using specified subdirectory: %s\n", srcSubdir)
	} else if len(dirs) == 1 && len(files) == 0 {
		if dirs[0] == destBase {
			effectiveSrc = filepath.Join(tempDir, dirs[0]) + "/"
			fmt.Fprintf(writer, "    │ Archive has single directory '%s' matching dest basename, syncing its contents\n", dirs[0])
		} else {
			e.executeCommand(fmt.Sprintf("rm -rf '%s'", tempDir), false)
			return "", "", fmt.Errorf("archive contains directory '%s' but dest basename is '%s'. Please rename or use src_subdir", dirs[0], destBase)
		}
	} else if len(dirs) == 0 && len(files) > 0 {
		effectiveSrc = tempDir + "/"
		fmt.Fprintf(writer, "    │ Archive contains only files, syncing directly to destination\n")
	} else {
		e.executeCommand(fmt.Sprintf("rm -rf '%s'", tempDir), false)
		return "", "", fmt.Errorf("archive has multiple top-level items, please specify 'src_subdir' or use 'flat: true'")
	}

	e.executeCommand(fmt.Sprintf("mkdir -p '%s'", dest), false)
	return tempDir, effectiveSrc, nil
}
func (e *Executor) performSync(src, dest, rsyncFlags, exclude string, writer io.Writer) error {
	// 确保目标目录存在
	e.executeCommand(fmt.Sprintf("mkdir -p '%s'", dest), false)

	hasRsync, _ := e.executeCommand("command -v rsync", false)
	var syncCmd string
	if strings.TrimSpace(hasRsync) != "" {
		excludeFlag := ""
		if exclude != "" {
			excludeFlag = fmt.Sprintf("--exclude='%s'", exclude)
		}
		// 源末尾加 / 表示复制内容，不加 / 表示复制目录本身
		syncCmd = fmt.Sprintf("rsync -a %s %s %s %s/", rsyncFlags, excludeFlag, src, dest)
	} else {
		// 回退到 cp -a
		if strings.HasSuffix(src, "/") {
			syncCmd = fmt.Sprintf("cp -a %s. %s/", src, dest)
		} else {
			syncCmd = fmt.Sprintf("cp -a %s %s/", src, dest)
		}
	}
	if _, err := e.executeCommand(syncCmd, false); err != nil {
		return err
	}
	fmt.Fprintf(writer, "    │ Synchronized %s to %s\n", src, dest)
	return nil
}

func (e *Executor) rollbackSync(dest, backupPath, backupType string, writer io.Writer) {
	// 删除可能不完整的新目录
	e.executeCommand(fmt.Sprintf("rm -rf '%s'", dest), false)
	if backupType == "move" {
		// 恢复移动的备份（无论备份路径在哪，直接 mv 回来）
		e.executeCommand(fmt.Sprintf("mv '%s' '%s'", backupPath, dest), false)
		fmt.Fprintf(writer, "    │ Restored original destination from backup\n")
	} else if backupType == "tar" {
		// 解压 tar 备份到原位置（backupPath 可能是绝对路径）
		parentDir := filepath.Dir(dest)
		e.executeCommand(fmt.Sprintf("cd '%s' && tar -xzf '%s'", parentDir, backupPath), false)
		fmt.Fprintf(writer, "    │ Extracted backup to %s\n", dest)
	}
}

func (e *Executor) cleanupTempDir(tempDir string, writer io.Writer) {
	if tempDir != "" {
		e.executeCommand(fmt.Sprintf("rm -rf '%s'", tempDir), false)
		fmt.Fprintf(writer, "    │ Cleaned up temporary directory\n")
	}
}
