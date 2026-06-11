package executor

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"sshot/pkg/msql"
	"sshot/pkg/types"
)

// getDBConnectionWithFallback 尝试直连数据库，失败则通过 SSH 隧道连接
func (e *Executor) getDBConnectionWithFallback(driver, host string, port int, user, password, dbName string) (*sql.DB, error) {
	// 1. 尝试直连
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?timeout=5s", user, password, host, port, dbName)
	db, err := sql.Open(driver, dsn)
	if err == nil {
		if err = db.Ping(); err == nil {
			return db, nil
		}
		db.Close()
	}

	// 2. 直连失败，尝试 SSH 隧道（如果 SSH 客户端可用）
	if e.client == nil {
		return nil, fmt.Errorf("direct connection failed and no SSH client available")
	}

	localPort, err := e.startLocalPortForward(host, port)
	if err != nil {
		return nil, fmt.Errorf("failed to start SSH tunnel: %w", err)
	}

	tunnelDSN := fmt.Sprintf("%s:%s@tcp(127.0.0.1:%d)/%s?timeout=5s", user, password, localPort, dbName)
	db, err = sql.Open(driver, tunnelDSN)
	if err != nil {
		return nil, fmt.Errorf("failed to open database via tunnel: %w", err)
	}
	if err = db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("database ping via tunnel failed: %w", err)
	}
	return db, nil
}

// startLocalPortForward 启动本地端口转发，返回本地监听端口
func (e *Executor) startLocalPortForward(remoteHost string, remotePort int) (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	localPort := listener.Addr().(*net.TCPAddr).Port

	// 后台协程处理转发
	go func() {
		for {
			localConn, err := listener.Accept()
			if err != nil {
				return // 监听器关闭时退出
			}
			go func(local net.Conn) {
				defer local.Close()
				remoteAddr := fmt.Sprintf("%s:%d", remoteHost, remotePort)
				remoteConn, err := e.client.Dial("tcp", remoteAddr)
				if err != nil {
					return
				}
				defer remoteConn.Close()
				// 双向转发数据
				go func() { _, _ = io.Copy(remoteConn, local) }()
				_, _ = io.Copy(local, remoteConn)
			}(localConn)
		}
	}()

	return localPort, nil
}

func (e *Executor) executeDBExecFile(task *types.DBExecFileTask) (string, error) {
	writer := e.OutputWriter
	if writer == nil {
		writer = os.Stdout
	}

	restore, err := e.setupMsqlLoggerForTask(task.LogFile)
	if err != nil {
		return "", err
	}
	defer restore()

	// 检查 SQL 文件是否存在
	absPath := toLocalPath(task.Path)
	if _, err := os.Stat(absPath); err != nil {
		return "", fmt.Errorf("SQL file not found: %s", absPath)
	}

	// 获取数据库连接
	db, err := e.getDBConnectionWithFallback(task.Driver, task.Host, task.Port, task.User, task.Password, task.Database)
	if err != nil {
		return "", err
	}
	defer db.Close()

	// 使用 msql.DbSql 包装器执行文件
	dbWrapper := &msql.DbSql{
		Db:     db,
		Driver: task.Driver,
		DBName: task.Database,
	}

	success, err := dbWrapper.ExSqlFile2(absPath) // 复用你已有的方法
	if err != nil {
		return "", fmt.Errorf("execute SQL file failed: %w", err)
	}
	if success {
		return fmt.Sprintf("Successfully executed SQL file: %s", absPath), nil
	}
	return "Execution completed", nil
}

func (e *Executor) executeDBExecSQL(task *types.DBExecSQLTask) (string, error) {
	writer := e.OutputWriter
	if writer == nil {
		writer = os.Stdout
	}

	// 设置详细日志输出目标
	var detailWriter io.Writer = io.Discard
	var logFile *os.File
	if task.LogFile != "" {
		dir := filepath.Dir(task.LogFile)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return "", fmt.Errorf("failed to create log directory: %w", err)
		}
		var err error
		logFile, err = os.OpenFile(task.LogFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return "", fmt.Errorf("failed to open log file: %w", err)
		}
		defer logFile.Close()
		detailWriter = logFile
	}

	fmt.Fprintf(writer, "    │ Executing SQL statements...\n")

	// 获取数据库连接（支持直连或 SSH 隧道）
	db, err := e.getDBConnectionWithFallback(task.Driver, task.Host, task.Port, task.User, task.Password, task.Database)
	if err != nil {
		fmt.Fprintf(writer, "    │ ✗ Failed to connect to database: %v\n", err)
		return "", err
	}
	defer db.Close()

	// 分割 SQL 语句（按分号，忽略注释和空语句）
	statements := splitSQLStatements(task.Query)
	executed := 0
	var execErrors []string

	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}

		// 执行语句
		result, err := db.Exec(stmt)
		if err != nil {
			errMsg := fmt.Sprintf("statement failed: %s\nError: %v", stmt, err)
			execErrors = append(execErrors, errMsg)
			// 记录到详细日志文件
			fmt.Fprintf(detailWriter, "    │ ✗ %s\n", errMsg)
			// 控制台输出错误概要（可选，取决于详细程度）
			if types.ExecOptions.Verbose {
				fmt.Fprintf(writer, "    │ ✗ %s\n", errMsg)
			}
			continue
		}

		// 获取影响行数（如果支持）
		rowsAffected, _ := result.RowsAffected()
		execDetail := fmt.Sprintf("Executed: %s (rows affected: %d)", stmt, rowsAffected)
		// 写入详细日志文件
		fmt.Fprintf(detailWriter, "    │ %s\n", execDetail)

		executed++
	}

	if len(execErrors) > 0 {
		fmt.Fprintf(writer, "    │ ✗ %d statement(s) failed, %d executed successfully\n", len(execErrors), executed)
		return "", fmt.Errorf("SQL execution failed with %d errors", len(execErrors))
	}

	fmt.Fprintf(writer, "    │ ✓ Executed %d statement(s) successfully\n", executed)
	return "SQL statements executed successfully", nil
}

// splitSQLStatements 分割 SQL 语句（简单按分号分割，忽略注释，不处理字符串内的分号）
// 实际生产中建议使用更健壮的 SQL 解析器，此处简化。
func splitSQLStatements(sql string) []string {
	// 去除多行注释和单行注释（简单实现）
	// 先用正则移除注释，再按分号分割。
	// 这里为了简洁，直接按分号分割，生产环境需完善。
	parts := strings.Split(sql, ";")
	var result []string
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func (e *Executor) executeDBMigrate(task *types.DBMigrateTask) (string, error) {
	writer := e.OutputWriter
	if writer == nil {
		writer = os.Stdout
	}
	restore, err := e.setupMsqlLoggerForTask(task.LogFile)
	if err != nil {
		return "", err
	}
	defer restore()
	db, err := e.getDBConnectionWithFallback(task.Driver, task.Host, task.Port, task.User, task.Password, task.Database)
	if err != nil {
		return "", err
	}
	defer db.Close()

	dbWrapper := &msql.DbSql{
		Db:     db,
		Driver: task.Driver,
		DBName: task.Database,
	}

	targetVersion := task.TargetVersion
	if targetVersion == "" {
		targetVersion = "9.9.9.9"
	}

	// 日志回调：将升级过程中的消息输出到终端
	logCallback := func(msg string) {
		e.mu.Lock()
		fmt.Fprintf(writer, "    │ %s\n", msg)
		e.mu.Unlock()
		if types.ExecOptions.Verbose {
			log.Printf("[VERBOSE] DB migration: %s", msg)
		}
	}

	// 执行升级（使用你已有的 UpdateWebVersion2 方法）
	err = dbWrapper.UpdateWebVersion2(task.ScriptsPath, targetVersion, logCallback)
	if err != nil {
		return "", fmt.Errorf("database migration failed: %w", err)
	}
	return "Database migration completed successfully", nil
}

var msqlLoggerMutex sync.Mutex
var originalMsqlLogger *slog.Logger

// setupMsqlLoggerForTask 临时替换 msql 包的 Logger，用于当前任务。
// logFile: 日志文件路径，如果为空则使用丢弃日志的 Logger（不产生日志）。
// 返回恢复函数，调用后恢复原 Logger。
func (e *Executor) setupMsqlLoggerForTask(logFile string) (restore func(), err error) {
	msqlLoggerMutex.Lock()
	// 保存原始的 Logger（如果尚未保存）
	if originalMsqlLogger == nil {
		originalMsqlLogger = msql.Logger
	}

	var newLogger *slog.Logger
	if logFile == "" {
		// 丢弃所有日志
		newLogger = slog.New(slog.NewTextHandler(io.Discard, nil))
	} else {
		// 创建日志文件
		dir := filepath.Dir(logFile)
		if err := os.MkdirAll(dir, 0755); err != nil {
			msqlLoggerMutex.Unlock()
			return nil, err
		}
		file, err := os.OpenFile(logFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			msqlLoggerMutex.Unlock()
			return nil, err
		}
		handler := slog.NewTextHandler(file, &slog.HandlerOptions{Level: slog.LevelInfo})
		newLogger = slog.New(handler)
		// 恢复时也需要关闭文件
		restore = func() {
			file.Close()
			msql.SetLogger(originalMsqlLogger)
			msqlLoggerMutex.Unlock()
		}
	}

	msql.SetLogger(newLogger)
	if restore == nil {
		restore = func() {
			msql.SetLogger(originalMsqlLogger)
			msqlLoggerMutex.Unlock()
		}
	}
	return restore, nil
}
