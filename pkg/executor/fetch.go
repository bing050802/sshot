package executor

import (
	"crypto/md5"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sshot/pkg/types"
	"strconv"
	"strings"

	"github.com/pkg/sftp"
)

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

	checkCmd := fmt.Sprintf("if [ -d '%s' ]; then echo 'directory'; elif [ -f '%s' ]; then echo 'file'; else echo 'notfound'; fi", src, src)
	fileType, err := e.executeCommandWithNewSession(checkCmd)
	if err != nil {
		return "", err
	}
	fileType = strings.TrimSpace(fileType)

	// 如果是目录拉取，且需要保留源目录名
	if fetchTask.AddSourceDir && fileType == "directory" {
		fullDest = filepath.Join(fullDest, filepath.Base(src))
	}

	if err := os.MkdirAll(fullDest, 0755); err != nil {
		return "", fmt.Errorf("failed to create directory: %w", err)
	}
	if types.ExecOptions.Verbose {
		e.mu.Lock()
		fmt.Fprintf(writer, "    │ Fetch src: %s\n", src)
		fmt.Fprintf(writer, "    │ Fetch dest: %s\n", fullDest)
		fmt.Fprintf(writer, "    │ AddSourceDir: %v\n", fetchTask.AddSourceDir)
		fmt.Fprintf(writer, "    │ Flat: %v\n", fetchTask.Flat)
		e.mu.Unlock()
	}
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
