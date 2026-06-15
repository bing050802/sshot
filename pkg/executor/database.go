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
	"strconv"
	"strings"
	"sync"
	"time"

	"sshot/pkg/msql"
	"sshot/pkg/types"

	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/olekukonko/tablewriter"
	"github.com/olekukonko/tablewriter/tw"
)

// getDBConnectionWithFallback 尝试直连数据库，失败则通过 SSH 隧道连接
func (e *Executor) getDBConnectionWithFallback(driver, host string, port string, user, password, dbName string) (*sql.DB, error) {
	// 1. 尝试直连
	writer := e.OutputWriter
	if writer == nil {
		writer = os.Stdout
	}

	cfg := mysql.NewConfig()
	cfg.User = user
	cfg.Passwd = password // 直接使用原始密码
	cfg.Net = "tcp"
	cfg.Addr = fmt.Sprintf("%s:%s", host, port)
	cfg.DBName = dbName
	cfg.Timeout = 5 * time.Second
	dsn := cfg.FormatDSN()
	if types.ExecOptions.Verbose {
		fmt.Fprintf(writer, "    │ dsn %s\n", dsn)
	}
	db, err := sql.Open(driver, dsn)
	if err == nil {
		if err = db.Ping(); err == nil {
			if types.ExecOptions.Verbose {
				fmt.Fprintf(writer, "    │ ✗ dsn%s %s\n", dsn, err)
			}
			return db, nil
		}
		db.Close()
	}

	// 2. 直连失败，尝试 SSH 隧道（如果 SSH 客户端可用）
	if e.client != nil {
		localPort, err := e.startLocalPortForward(port) // 注意：不再传递 host，固定 127.0.0.1
		if err != nil {
			return nil, fmt.Errorf("failed to start SSH tunnel: %w", err)
		}
		cfg.Addr = fmt.Sprintf("127.0.0.1:%s", localPort)
		tunnelDSN := cfg.FormatDSN()
		if types.ExecOptions.Verbose {
			fmt.Fprintf(writer, "    │ tunnelDSN %s\n", tunnelDSN)
		}

		db, err := sql.Open(driver, tunnelDSN)
		if err == nil {
			if types.ExecOptions.Verbose {
				fmt.Fprintf(writer, "    │  连接成功 %s\n", tunnelDSN)
			}
			if err = db.Ping(); err == nil {
				if types.ExecOptions.Verbose {
					fmt.Fprintf(writer, "    │ 连接成功 %s\n", dsn)
				}
				return db, nil
			}
			if types.ExecOptions.Verbose {
				fmt.Fprintf(writer, "    │ ✗ tunnelDSN %s %s\n", tunnelDSN, err)
			}
			db.Close()
		}
		if types.ExecOptions.Verbose {
			fmt.Fprintf(writer, "    │ ✗ %s\n", err)
		}

		return nil, fmt.Errorf("direct connection and SSH tunnel both failed")
	}
	return nil, err
}

// startLocalPortForward 启动本地端口转发，返回本地监听端口
func (e *Executor) startLocalPortForward(port string) (string, error) {
	writer := e.OutputWriter
	if writer == nil {
		writer = os.Stdout
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		if types.ExecOptions.Verbose {
			fmt.Fprintf(writer, "    │ ✗ %s\n", err)
		}
		return "", err
	}
	localPort := listener.Addr().(*net.TCPAddr).Port

	go func() {
		for {
			localConn, err := listener.Accept()
			if err != nil {
				if types.ExecOptions.Verbose {
					fmt.Fprintf(writer, "    │ ✗ %s\n", err)
				}
				continue
			}
			go func(local net.Conn) {
				defer local.Close()
				// 关键：始终连接远程主机的 127.0.0.1
				remoteConn, err := e.client.Dial("tcp", fmt.Sprintf("127.0.0.1:%s", port))
				if err != nil {
					if types.ExecOptions.Verbose {
						fmt.Fprintf(writer, "    │ ✗ %s\n", err)
					}
					return
				}
				defer remoteConn.Close()
				go func() { _, _ = io.Copy(remoteConn, local) }()
				_, _ = io.Copy(local, remoteConn)
			}(localConn)
		}
	}()
	return strconv.Itoa(localPort), nil
}
func (e *Executor) executeDBExecFile(task *types.DBExecFileTask) (string, error) {
	writer := e.OutputWriter
	if writer == nil {
		writer = os.Stdout
	}

	driver := e.SubstituteVars(task.Driver)
	host := e.SubstituteVars(task.Host)
	portStr := e.SubstituteVars(task.Port)
	user := e.SubstituteVars(task.User)
	password := e.SubstituteVars(task.Password)
	database := e.SubstituteVars(task.Database)
	scriptsPath := e.SubstituteVars(task.Path)

	logFile := e.SubstituteVars(task.LogFile)

	restore, err := e.setupMsqlLoggerForTask(logFile)
	if err != nil {
		return "", err
	}
	defer restore()

	// 检查 SQL 文件是否存在
	absPath := toLocalPath(scriptsPath)
	if _, err := os.Stat(absPath); err != nil {
		return "", fmt.Errorf("SQL file not found: %s", absPath)
	}

	// 获取数据库连接
	db, err := e.getDBConnectionWithFallback(driver, host, portStr, user, password, database)
	if err != nil {
		return "", err
	}
	defer db.Close()

	// 使用 msql.DbSql 包装器执行文件
	dbWrapper := &msql.DbSql{
		Db:     db,
		Driver: driver,
		DBName: database,
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

	driver := e.SubstituteVars(task.Driver)
	host := e.SubstituteVars(task.Host)
	portStr := e.SubstituteVars(task.Port)
	user := e.SubstituteVars(task.User)
	password := e.SubstituteVars(task.Password)
	database := e.SubstituteVars(task.Database)
	queryText := e.SubstituteVars(task.Query)

	logFile := e.SubstituteVars(task.LogFile)

	// 设置详细日志输出目标
	var detailWriter io.Writer = io.Discard
	var logFileHandle *os.File
	if logFile != "" {
		dir := filepath.Dir(logFile)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return "", fmt.Errorf("failed to create log directory: %w", err)
		}
		var err error
		logFileHandle, err = os.OpenFile(logFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return "", fmt.Errorf("failed to open log file: %w", err)
		}
		defer logFileHandle.Close()
		detailWriter = logFileHandle
	}

	fmt.Fprintf(writer, "    │ Executing SQL statements...\n")

	// 获取数据库连接
	db, err := e.getDBConnectionWithFallback(driver, host, portStr, user, password, database)
	if err != nil {
		fmt.Fprintf(writer, "    │ ✗ Failed to connect to database: %v\n", err)
		return "", err
	}
	defer db.Close()

	// 分割 SQL 语句
	statements := splitSQLStatements(queryText)
	executed := 0
	var execErrors []string

	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}

		// 判断是否为查询语句 (SELECT, SHOW, EXPLAIN, DESCRIBE 等)
		upperStmt := strings.ToUpper(stmt)
		isQuery := strings.HasPrefix(upperStmt, "SELECT") ||
			strings.HasPrefix(upperStmt, "SHOW") ||
			strings.HasPrefix(upperStmt, "EXPLAIN") ||
			strings.HasPrefix(upperStmt, "DESCRIBE")

		if isQuery {
			// 执行查询并输出结果
			rows, err := db.Query(stmt)
			if err != nil {
				errMsg := fmt.Sprintf("query failed: %s\nError: %v", stmt, err)
				execErrors = append(execErrors, errMsg)
				fmt.Fprintf(detailWriter, "    │ ✗ %s\n", errMsg)
				if types.ExecOptions.Verbose {
					fmt.Fprintf(writer, "    │ ✗ %s\n", errMsg)
				}
				continue
			}
			// 输出结果集到 detailWriter
			printQueryResults(rows, io.MultiWriter(detailWriter, writer))

			rows.Close()
			executed++
		} else {
			// 非查询语句（INSERT, UPDATE, DELETE, CREATE, ALTER 等）
			result, err := db.Exec(stmt)
			if err != nil {
				errMsg := fmt.Sprintf("statement failed: %s\nError: %v", stmt, err)
				execErrors = append(execErrors, errMsg)
				fmt.Fprintf(detailWriter, "    │ ✗ %s\n", errMsg)
				if types.ExecOptions.Verbose {
					fmt.Fprintf(writer, "    │ ✗ %s\n", errMsg)
				}
				continue
			}
			rowsAffected, _ := result.RowsAffected()
			execDetail := fmt.Sprintf("Executed: %s (rows affected: %d)", stmt, rowsAffected)
			fmt.Fprintf(detailWriter, "    │ %s\n", execDetail)
			if types.ExecOptions.Verbose {
				fmt.Fprintf(writer, "    │ %s\n", execDetail)
			}
			executed++
		}
	}

	if len(execErrors) > 0 {
		fmt.Fprintf(writer, "    │ ✗ %d statement(s) failed, %d executed successfully\n", len(execErrors), executed)
		return "", fmt.Errorf("SQL execution failed with %d errors", len(execErrors))
	}

	fmt.Fprintf(writer, "    │ ✓ Executed %d statement(s) successfully\n", executed)
	return "SQL statements executed successfully", nil
}

// printQueryResults 将 sql.Rows 的结果集格式化为表格输出到 writer
func printQueryResults(rows *sql.Rows, writer io.Writer) {
	columns, err := rows.Columns()
	if err != nil {
		fmt.Fprintf(writer, "    │ Failed to get columns: %v\n", err)
		return
	}

	// 打印列名（用制表符分隔）
	fmt.Fprintf(writer, "    │ ")
	for i, col := range columns {
		if i > 0 {
			fmt.Fprintf(writer, "\t")
		}
		fmt.Fprintf(writer, "%s", col)
	}
	fmt.Fprintf(writer, "\n")

	// 准备扫描缓冲区
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	for rows.Next() {
		err := rows.Scan(valuePtrs...)
		if err != nil {
			fmt.Fprintf(writer, "    │ Error scanning row: %v\n", err)
			continue
		}
		fmt.Fprintf(writer, "    │ ")
		for i, val := range values {
			if i > 0 {
				fmt.Fprintf(writer, "\t")
			}
			// 处理 NULL 值
			if val == nil {
				fmt.Fprintf(writer, "NULL")
				continue
			}
			// 处理 []byte 类型（转为字符串）
			switch v := val.(type) {
			case []byte:
				fmt.Fprintf(writer, "%s", string(v))
			default:
				fmt.Fprintf(writer, "%v", v)
			}
		}
		fmt.Fprintf(writer, "\n")
	}
	if err := rows.Err(); err != nil {
		fmt.Fprintf(writer, "    │ Row iteration error: %v\n", err)
	}
}

// renderQueryResultWithTableWriter 将 sql.Rows 格式化为表格并写入 io.Writer
func renderQueryResultWithTableWriter(rows *sql.Rows, writer io.Writer) {
	// 1. 获取列名
	columns, err := rows.Columns()
	if err != nil {
		fmt.Fprintf(writer, "    │ Failed to get columns: %v\n", err)
		return
	}

	// 2. 扫描所有行数据到内存（二维字符串切片）
	colCount := len(columns)
	values := make([]interface{}, colCount)
	valuePtrs := make([]interface{}, colCount)
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	var data [][]string
	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			fmt.Fprintf(writer, "    │ Error scanning row: %v\n", err)
			continue
		}
		row := make([]string, colCount)
		for i, val := range values {
			if val == nil {
				row[i] = "NULL"
				continue
			}
			switch v := val.(type) {
			case []byte:
				row[i] = string(v)
			default:
				row[i] = fmt.Sprintf("%v", v)
			}
		}
		data = append(data, row)
	}
	if err := rows.Err(); err != nil {
		fmt.Fprintf(writer, "    │ Row iteration error: %v\n", err)
		return
	}

	if len(data) == 0 {
		fmt.Fprintf(writer, "    │ (no rows)\n")
		return
	}

	// 3. 创建 tablewriter 实例，配置样式（左对齐，无边框，简洁）
	table := tablewriter.NewTable(writer,
		tablewriter.WithConfig(tablewriter.Config{
			Header: tw.CellConfig{Alignment: tw.CellAlignment{Global: tw.AlignLeft}},
			Row:    tw.CellConfig{Alignment: tw.CellAlignment{Global: tw.AlignLeft}},
		}),
		// 可选：使用无边框渲染器（默认有边框，可修改为无边框）
		// tablewriter.WithRenderer(renderer.NewBlueprint(tw.Rendition{Borders: tw.BorderNone})),
	)

	// 4. 设置表头和数据
	table.Header(columns)
	// 将 [][]string 转换为 [][]any（因为 Bulk 要求 [][]any）
	anyData := make([][]any, len(data))
	for i, row := range data {
		anyRow := make([]any, len(row))
		for j, val := range row {
			anyRow[j] = val
		}
		anyData[i] = anyRow
	}
	table.Bulk(anyData)
	table.Render()
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

	driver := e.SubstituteVars(task.Driver)
	host := e.SubstituteVars(task.Host)
	portStr := e.SubstituteVars(task.Port)
	user := e.SubstituteVars(task.User)
	password := e.SubstituteVars(task.Password)
	database := e.SubstituteVars(task.Database)
	scriptsPath := e.SubstituteVars(task.ScriptsPath)

	logFile := e.SubstituteVars(task.LogFile)

	restore, err := e.setupMsqlLoggerForTask(logFile)
	if err != nil {
		return "", err
	}
	defer restore()

	absPath := toLocalPath(scriptsPath)
	if _, err := os.Stat(absPath); err != nil {
		return "", fmt.Errorf("SQL file not found: %s", absPath)
	}

	db, err := e.getDBConnectionWithFallback(driver, host, portStr, user, password, database)
	if err != nil {
		return "", err
	}
	defer db.Close()

	dbWrapper := &msql.DbSql{
		Db:     db,
		Driver: driver,
		DBName: database,
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
	err = dbWrapper.UpdateWebVersion2(absPath, targetVersion, logCallback)
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
