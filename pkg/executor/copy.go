package executor

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
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
		return "", fmt.Errorf("failed to stat source file: %w", err)
	}
	if info.IsDir() {
		return "", fmt.Errorf("copy of directories is not supported (use sync or archive instead)")
	}

	// 远程目标路径转换
	dest := e.SubstituteVars(copyTask.Dest)
	// 如果 dest 以 / 结尾，自动附加源文件名
	if strings.HasSuffix(dest, "/") {
		baseName := filepath.Base(src)
		dest = dest + baseName
	}
	dest = toRemotePath(dest)

	if types.ExecOptions.Verbose {
		e.mu.Lock()
		fmt.Fprintf(writer, "    │ Copy src (local): %s\n", src)
		fmt.Fprintf(writer, "    │ Copy dest (remote): %s\n", dest)
		e.mu.Unlock()
	}

	// 打开本地文件
	localFile, err := os.Open(src)
	if err != nil {
		return "", fmt.Errorf("failed to open local file: %w", err)
	}
	defer localFile.Close()
	fileSize := info.Size()

	// 创建 SFTP 客户端
	sftpClient, err := sftp.NewClient(e.client)
	if err != nil {
		return "", fmt.Errorf("failed to create sftp client: %w", err)
	}
	defer sftpClient.Close()

	// 确保远程目录存在
	destDir := filepath.Dir(dest)
	if err := sftpClient.MkdirAll(destDir); err != nil {
		return "", fmt.Errorf("failed to create remote directory: %w", err)
	}

	// 创建远程文件
	remoteFile, err := sftpClient.Create(dest)
	if err != nil {
		return "", fmt.Errorf("failed to create remote file: %w", err)
	}
	defer remoteFile.Close()

	// 进度显示变量
	var written int64
	lastReport := time.Now()
	reportInterval := 2 * time.Second

	// 包装 writer 以便定期报告进度
	progressWriter := &progressWriter{
		writer: remoteFile,
		total:  fileSize,
		onProgress: func(w int64) {
			now := time.Now()
			if types.ExecOptions.Progress && (now.Sub(lastReport) >= reportInterval || w == fileSize) {
				percent := float64(w) / float64(fileSize) * 100
				fmt.Fprintf(writer, "    │ Progress: %.1f%% (%d/%d bytes)\n", percent, w, fileSize)
				lastReport = now
			}
		},
		written: &written,
	}

	// 流式复制
	_, err = io.Copy(progressWriter, localFile)
	if err != nil {
		return "", fmt.Errorf("failed to copy file via sftp: %w", err)
	}

	if types.ExecOptions.Verbose {
		fmt.Fprintf(writer, "    │ Copied %d bytes\n", written)
	}

	// 设置文件权限
	if copyTask.Mode != "" {
		_, err = e.executeCommand(fmt.Sprintf("chmod %s '%s'", copyTask.Mode, dest), false)
		if err != nil {
			return "", err
		}
	}

	return dest, nil
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
