package executor

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"sshot/pkg/types"
)

// executeArchive 执行压缩或解压缩
func (e *Executor) executeArchive(archiveTask *types.ArchiveTask) (string, error) {
	writer := e.OutputWriter
	if writer == nil {
		writer = os.Stdout
	}

	src := e.SubstituteVars(archiveTask.Path)
	dest := e.SubstituteVars(archiveTask.Dest)
	state := archiveTask.State
	if state == "" {
		state = "present"
	}
	format := archiveTask.Format
	remove := archiveTask.Remove

	if types.ExecOptions.Verbose {
		e.mu.Lock()
		fmt.Fprintf(writer, "    │ Archive source: %s\n", src)
		fmt.Fprintf(writer, "    │ Archive dest: %s\n", dest)
		fmt.Fprintf(writer, "    │ State: %s\n", state)
		fmt.Fprintf(writer, "    │ Format: %s\n", format)
		e.mu.Unlock()
	}

	switch state {
	case "present":
		return e.compress(src, dest, format, remove, archiveTask.Flat)
	case "absent", "extract", "unarchive":
		return e.decompress(src, dest, format)
	default:
		return "", fmt.Errorf("unsupported state: %s (allowed: present, absent)", state)
	}
}

// compress 压缩文件或目录
// compress 压缩文件或目录
// flat 参数说明：
//   - 当源是目录时：
//     flat = false（默认）：进入目录，打包其内部所有内容（不包含目录本身）
//     flat = true：进入父目录，打包该目录本身（解压后会恢复顶层目录）
//   - 当源是文件时：flat 参数无效，直接打包该文件
func (e *Executor) compress(src, dest, format string, remove bool, flat bool) (string, error) {
	writer := e.OutputWriter
	if writer == nil {
		writer = os.Stdout
	}

	if format == "" {
		format = inferFormat(dest)
	}

	// 检查源是否存在
	checkCmd := fmt.Sprintf("if [ -e '%s' ]; then echo 'exists'; else echo 'notfound'; fi", src)
	out, err := e.executeCommand(checkCmd, false)
	if err != nil || strings.TrimSpace(out) != "exists" {
		return "", fmt.Errorf("source path does not exist: %s", src)
	}

	// 创建目标目录（如果父目录不存在）
	destDir := filepath.Dir(dest)
	mkdirCmd := fmt.Sprintf("mkdir -p '%s'", destDir)
	if _, err := e.executeCommand(mkdirCmd, false); err != nil {
		return "", fmt.Errorf("failed to create destination directory: %w", err)
	}

	// 判断源类型（文件/目录）
	typeCmd := fmt.Sprintf("if [ -f '%s' ]; then echo 'file'; elif [ -d '%s' ]; then echo 'dir'; else echo 'other'; fi", src, src)
	typeOut, _ := e.executeCommand(typeCmd, false)
	fileType := strings.TrimSpace(typeOut)

	if fileType == "other" {
		return "", fmt.Errorf("source is not a regular file or directory: %s", src)
	}

	var compressCmd string
	switch format {
	case "tar.gz", "tgz":
		if fileType == "file" {
			compressCmd = fmt.Sprintf("tar -czf '%s' -C '%s' '%s'", dest, filepath.Dir(src), filepath.Base(src))
		} else { // directory
			if flat {
				// 打包目录本身（保留顶层目录）
				parentDir := filepath.Dir(src)
				baseName := filepath.Base(src)
				compressCmd = fmt.Sprintf("tar -czf '%s' -C '%s' '%s'", dest, parentDir, baseName)
			} else {
				// 打包目录内容（不保留顶层目录）
				compressCmd = fmt.Sprintf("tar -czf '%s' -C '%s' .", dest, src)
			}
		}

	case "tar.bz2", "tbz2":
		if fileType == "file" {
			compressCmd = fmt.Sprintf("tar -cjf '%s' -C '%s' '%s'", dest, filepath.Dir(src), filepath.Base(src))
		} else {
			if flat {
				parentDir := filepath.Dir(src)
				baseName := filepath.Base(src)
				compressCmd = fmt.Sprintf("tar -cjf '%s' -C '%s' '%s'", dest, parentDir, baseName)
			} else {
				compressCmd = fmt.Sprintf("tar -cjf '%s' -C '%s' .", dest, src)
			}
		}

	case "tar.xz":
		if fileType == "file" {
			compressCmd = fmt.Sprintf("tar -cJf '%s' -C '%s' '%s'", dest, filepath.Dir(src), filepath.Base(src))
		} else {
			if flat {
				parentDir := filepath.Dir(src)
				baseName := filepath.Base(src)
				compressCmd = fmt.Sprintf("tar -cJf '%s' -C '%s' '%s'", dest, parentDir, baseName)
			} else {
				compressCmd = fmt.Sprintf("tar -cJf '%s' -C '%s' .", dest, src)
			}
		}

	case "zip":
		if fileType == "file" {
			compressCmd = fmt.Sprintf("zip -j '%s' '%s'", dest, src)
		} else {
			if flat {
				// 打包目录本身：进入父目录，打包整个目录
				parentDir := filepath.Dir(src)
				baseName := filepath.Base(src)
				compressCmd = fmt.Sprintf("cd '%s' && zip -r '%s' '%s'", parentDir, dest, baseName)
			} else {
				// 打包目录内容：进入目录，打包当前目录下所有内容
				compressCmd = fmt.Sprintf("cd '%s' && zip -r '%s' .", src, dest)
			}
		}

	default:
		return "", fmt.Errorf("unsupported archive format: %s", format)
	}

	if types.ExecOptions.Verbose {
		e.mu.Lock()
		fmt.Fprintf(writer, "    │ Running: %s\n", compressCmd)
		e.mu.Unlock()
	}

	output, err := e.executeCommand(compressCmd, false)
	if err != nil {
		return "", fmt.Errorf("compression failed: %w\n%s", err, output)
	}

	// 压缩成功后，如果需要删除源文件/目录
	if remove {
		rmCmd := fmt.Sprintf("rm -rf '%s'", src)
		if _, err := e.executeCommand(rmCmd, false); err != nil {
			return output, fmt.Errorf("compressed successfully but failed to remove source: %w", err)
		}
		output += "\n(Source removed)"
	}

	return fmt.Sprintf("Compressed %s → %s", src, dest), nil
}

// decompress 解压缩归档文件
func (e *Executor) decompress(src, dest, format string) (string, error) {
	// 如果 format 为空，从 src 扩展名推断
	writer := e.OutputWriter
	if writer == nil {
		writer = os.Stdout
	}

	if format == "" {
		format = inferFormat(src)
	}

	// 检查归档文件是否存在
	checkCmd := fmt.Sprintf("if [ -f '%s' ]; then echo 'exists'; else echo 'notfound'; fi", src)
	out, err := e.executeCommand(checkCmd, false)
	if err != nil || strings.TrimSpace(out) != "exists" {
		return "", fmt.Errorf("archive file does not exist: %s", src)
	}

	// 创建目标目录
	mkdirCmd := fmt.Sprintf("mkdir -p '%s'", dest)
	if _, err := e.executeCommand(mkdirCmd, false); err != nil {
		return "", fmt.Errorf("failed to create destination directory: %w", err)
	}

	var extractCmd string
	switch format {
	case "tar.gz", "tgz":
		extractCmd = fmt.Sprintf("tar -xzf '%s' -C '%s'", src, dest)
	case "tar.bz2", "tbz2":
		extractCmd = fmt.Sprintf("tar -xjf '%s' -C '%s'", src, dest)
	case "tar.xz":
		extractCmd = fmt.Sprintf("tar -xJf '%s' -C '%s'", src, dest)
	case "zip":
		extractCmd = fmt.Sprintf("unzip -o '%s' -d '%s'", src, dest)
	default:
		return "", fmt.Errorf("unsupported archive format: %s", format)
	}

	if types.ExecOptions.Verbose {
		e.mu.Lock()
		fmt.Fprintf(writer, "    │ Running: %s\n", extractCmd)
		e.mu.Unlock()
	}

	output, err := e.executeCommand(extractCmd, false)
	if err != nil {
		return "", fmt.Errorf("extraction failed: %w\n%s", err, output)
	}

	return fmt.Sprintf("Extracted %s → %s", src, dest), nil
}

// inferFormat 从文件名扩展名推断压缩格式
func inferFormat(filename string) string {
	ext := strings.ToLower(filepath.Ext(filename))
	switch ext {
	case ".gz":
		if strings.HasSuffix(filename, ".tar.gz") || strings.HasSuffix(filename, ".tgz") {
			return "tar.gz"
		}
		return "tar.gz" // 无法区分，默认 tar.gz
	case ".bz2":
		return "tar.bz2"
	case ".xz":
		return "tar.xz"
	case ".zip":
		return "zip"
	default:
		return "tar.gz"
	}
}

// srcFileType 辅助函数，判断远程路径类型（文件/目录）
func srcFileType(path string) string {
	// 简单用 ssh 执行命令
	// 但为减少重复，可以调用通用方法
	// 实际生产可提取为独立函数
	return "" // 略
}
