package executor

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"sshot/pkg/types"

	"github.com/pkg/sftp"
)

func (e *Executor) executeCopy(copyTask *types.CopyTask) (string, error) {
	writer := e.OutputWriter
	if writer == nil {
		writer = os.Stdout
	}

	// 本地源路径转换
	src := e.SubstituteVars(copyTask.Src)
	src = toLocalPath(src)

	// 获取源文件信息
	info, err := os.Stat(src)
	if err != nil {
		return "", fmt.Errorf("failed to stat source: %w", err)
	}

	// 远程目标路径转换
	dest := e.SubstituteVars(copyTask.Dest)
	dest = toRemotePath(dest)

	// 创建 SFTP 客户端
	sftpClient, err := sftp.NewClient(e.client)
	if err != nil {
		return "", fmt.Errorf("failed to create sftp client: %w", err)
	}
	defer sftpClient.Close()

	if info.IsDir() {
		// 目录复制
		if !copyTask.Recursive {
			return "", fmt.Errorf("source is a directory but recursive is false")
		}
		if strings.HasSuffix(dest, "/") {
			dest = dest + filepath.Base(src)
		}
		// 确保目标目录存在
		if err := sftpClient.MkdirAll(dest); err != nil {
			return "", fmt.Errorf("failed to create remote directory %s: %w", dest, err)
		}

		// 递归上传目录
		err := e.uploadDir(sftpClient, src, dest, copyTask.Mode, copyTask.Preserve, writer)
		if err != nil {
			return "", err
		}
		return dest, nil
	} else {
		// 单文件复制
		// 如果 dest 以 / 结尾，自动附加源文件名
		if strings.HasSuffix(dest, "/") {
			dest = dest + filepath.Base(src)
		}
		// 确保目标目录存在
		destDir := filepath.Dir(dest)
		if err := sftpClient.MkdirAll(destDir); err != nil {
			return "", fmt.Errorf("failed to create remote directory %s: %w", destDir, err)
		}

		// 上传文件
		file, err := os.Open(src)
		if err != nil {
			return "", fmt.Errorf("failed to open local file: %w", err)
		}
		defer file.Close()

		err = e.uploadFile(sftpClient, file, dest, info, copyTask.Mode, copyTask.Preserve, writer)
		if err != nil {
			return "", err
		}
		return dest, nil
	}
}

// uploadDir 递归上传目录
func (e *Executor) uploadDir(sftpClient *sftp.Client, localDir, remoteDir, mode string, preserve bool, writer io.Writer) error {
	// 遍历本地目录
	return filepath.Walk(localDir, func(localPath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// 计算相对路径
		relPath, err := filepath.Rel(localDir, localPath)
		if err != nil {
			return fmt.Errorf("failed to get relative path: %w", err)
		}
		// 跳过根目录
		if relPath == "." {
			return nil
		}

		// 构建远程路径
		remotePath := filepath.ToSlash(filepath.Join(remoteDir, relPath))

		if info.IsDir() {
			// 创建远程目录
			if err := sftpClient.MkdirAll(remotePath); err != nil {
				return fmt.Errorf("failed to create remote directory %s: %w", remotePath, err)
			}
			// 如果保留权限，设置目录权限
			if preserve {
				if err := sftpClient.Chmod(remotePath, info.Mode()); err != nil {
					// 仅警告，不中断
					fmt.Fprintf(writer, "    │ Warning: failed to chmod %s: %v\n", remotePath, err)
				}
			}
			return nil
		}

		// 文件处理
		// 注意：如果 mode 指定，则覆盖文件权限，否则保留原权限或继承
		// 打开本地文件
		file, err := os.Open(localPath)
		if err != nil {
			return fmt.Errorf("failed to open local file %s: %w", localPath, err)
		}
		defer file.Close()

		// 上传文件
		if err := e.uploadFile(sftpClient, file, remotePath, info, mode, preserve, writer); err != nil {
			return err
		}

		return nil
	})
}

// uploadFile 上传单个文件，并设置权限、时间，显示进度
func (e *Executor) uploadFile(sftpClient *sftp.Client, localFile *os.File, remotePath string, info os.FileInfo, mode string, preserve bool, writer io.Writer) error {
	// 创建远程文件
	remoteFile, err := sftpClient.Create(remotePath)
	if err != nil {
		return fmt.Errorf("failed to create remote file %s: %w", remotePath, err)
	}
	defer remoteFile.Close()

	fileSize := info.Size()
	var written int64
	lastReport := time.Now()
	reportInterval := 2 * time.Second

	// 包装进度
	progressWriter := &progressWriter{
		writer: remoteFile,
		total:  fileSize,
		onProgress: func(w int64) {
			now := time.Now()
			if types.ExecOptions.Progress && (now.Sub(lastReport) >= reportInterval || w == fileSize) {
				percent := float64(w) / float64(fileSize) * 100
				fmt.Fprintf(writer, "    │ Progress: %.1f%% (%d/%d bytes) - %s\n", percent, w, fileSize, filepath.Base(remotePath))
				lastReport = now
			}
		},
		written: &written,
	}

	// 复制内容
	_, err = io.Copy(progressWriter, localFile)
	if err != nil {
		return fmt.Errorf("failed to copy file content to %s: %w", remotePath, err)
	}

	if types.ExecOptions.Verbose {
		fmt.Fprintf(writer, "    │ Copied %s (%d bytes)\n", filepath.Base(remotePath), written)
	}

	// 设置权限（优先使用 mode，否则用原文件权限或保留）
	finalMode := info.Mode()
	if mode != "" {
		// 解析 mode 字符串为 os.FileMode
		perm, err := strconv.ParseUint(mode, 8, 32)
		if err != nil {
			return fmt.Errorf("invalid mode %s: %w", mode, err)
		}
		finalMode = os.FileMode(perm)
	}

	if err := sftpClient.Chmod(remotePath, finalMode); err != nil {
		// 权限设置失败不中断，只警告
		fmt.Fprintf(writer, "    │ Warning: failed to chmod %s: %v\n", remotePath, err)
	}

	// 保留时间（如果 preserve 为 true）
	if preserve {
		if err := sftpClient.Chtimes(remotePath, info.ModTime(), info.ModTime()); err != nil {
			fmt.Fprintf(writer, "    │ Warning: failed to set times for %s: %v\n", remotePath, err)
		}
	}

	return nil
}

// progressWriter 包装 io.Writer，在写入时更新进度
type progressWriter struct {
	writer     io.Writer
	total      int64
	onProgress func(int64)
	written    *int64
}

func (pw *progressWriter) Write(p []byte) (n int, err error) {
	n, err = pw.writer.Write(p)
	if n > 0 {
		*pw.written += int64(n)
		if pw.onProgress != nil {
			pw.onProgress(*pw.written)
		}
	}
	return
}
