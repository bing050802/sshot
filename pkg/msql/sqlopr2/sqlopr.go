package sqlopr2

// Package sqlopr provides a simple way to execute SQL files with transaction support
// 核心特性：
// 1. 支持读取单个SQL文件、多个文件或目录下所有.sql文件
// 2. 事务化执行，确保SQL语句原子性（要么全成功，要么全回滚）
// 3. 自动拆分SQL语句（支持分号分隔），过滤注释
// 4. 支持显式指定数据库名，解决事务会话独立导致的「No database selected」问题
// 5. 详细的执行日志和耗时统计

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/xwb1989/sqlparser"
)

// Logger 全局日志实例，可外部初始化配置
var Logger *slog.Logger

// SqlFile 封装SQL文件和解析后的SQL语句集合
type SqlFile struct {
	files   []string // 已加载的SQL文件路径列表
	queries []string // 解析后的SQL语句列表（去注释、按分号拆分）
}

// New 创建SqlFile实例
func New() *SqlFile {
	return &SqlFile{}
}

// QueriesString 返回解析后的SQL语句列表（同Queries，兼容旧接口）
func (s *SqlFile) QueriesString() []string {
	return s.queries
}

// Queries 返回解析后的SQL语句列表
func (s *SqlFile) Queries() []string {
	return s.queries
}

// parseSQLContent 解析SQL内容：去注释、按分号拆分语句
// 依赖 sqlparser 处理复杂SQL语法（避免简单字符串拆分导致的语法错误）
func parseSQLContent(inputStr string) ([]string, error) {
	// 第一步：过滤注释（保留字符串内的分号和注释符号）
	//cleanedSQL := excludeComment(inputStr)

	// 第二步：用sqlparser拆分SQL语句（支持复杂场景，如字符串内分号）
	statements, err := sqlparser.SplitStatementToPieces(inputStr)
	if err != nil {
		return nil, fmt.Errorf("parse sql failed: %w", err)
	}

	// 第三步：过滤空语句（拆分后可能存在空字符串元素）
	var validQueries []string
	for _, stmt := range statements {
		trimmed := strings.TrimSpace(stmt)
		if trimmed != "" {
			validQueries = append(validQueries, trimmed)
		}
	}

	return validQueries, nil
}

// File 加载单个SQL文件并解析
func (s *SqlFile) File(filePath string) error {
	// 读取文件内容
	sqlData, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("read file failed: %w", err)
	}

	// 解析SQL语句
	queries, err := parseSQLContent(string(sqlData))
	if err != nil {
		return fmt.Errorf("parse file %s failed: %w", filePath, err)
	}

	// 记录文件路径和解析后的语句
	s.files = append(s.files, filePath)
	s.queries = append(s.queries, queries...)

	return nil
}

// Content 直接加载SQL字符串内容（无需文件）
func (s *SqlFile) Content(content string, name string) error {
	queries, err := parseSQLContent(content)
	if err != nil {
		return fmt.Errorf("parse content [%s] failed: %w", name, err)
	}

	s.files = append(s.files, name) // 记录内容名称（用于日志）
	s.queries = append(s.queries, queries...)

	return nil
}

// Files 加载多个SQL文件
func (s *SqlFile) Files(filePaths ...string) error {
	for _, filePath := range filePaths {
		if err := s.File(filePath); err != nil {
			return err
		}
	}
	return nil
}

// Directory 加载指定目录下所有.sql文件（递归忽略子目录）
func (s *SqlFile) Directory(dirPath string) error {
	// 读取目录内容
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return fmt.Errorf("read directory %s failed: %w", dirPath, err)
	}

	// 遍历文件，只处理.sql后缀的文件
	for _, entry := range entries {
		if entry.IsDir() {
			continue // 跳过子目录
		}

		fileName := entry.Name()
		if !strings.HasSuffix(fileName, ".sql") {
			continue // 只处理.sql文件
		}

		// 拼接完整文件路径
		filePath := fmt.Sprintf("%s/%s", dirPath, fileName)
		if err := s.File(filePath); err != nil {
			return fmt.Errorf("load file %s failed: %w", filePath, err)
		}
	}

	return nil
}

// Exec 执行所有解析后的SQL语句（事务化执行）
// db: 数据库连接实例
// dbName: 目标数据库名（用于解决事务会话独立导致的「No database selected」问题）
// 返回值：每个SQL语句的执行结果，或错误信息
func (s *SqlFile) Exec(db *sql.DB, dbName string) ([]sql.Result, error) {
	// 1. 检查必要参数
	if db == nil {
		return nil, fmt.Errorf("database connection is nil")
	}
	if len(s.queries) == 0 {
		return nil, fmt.Errorf("no SQL queries to execute")
	}

	// 2. 开启事务（事务会话独立，需显式指定数据库）
	tx, err := db.Begin()
	if err != nil {
		return nil, fmt.Errorf("begin transaction failed: %w", err)
	}

	// 3. 事务收尾处理（ defer 确保无论成功失败都处理提交/回滚）
	defer func() {
		if r := recover(); r != nil {
			// 捕获panic，回滚事务并重新抛出
			_ = tx.Rollback()
			panic(r)
		}
	}()

	// 4. 关键步骤：在事务内显式选中数据库（解决会话独立问题）
	if dbName != "" {
		useSQL := fmt.Sprintf("USE `%s`", dbName)
		if err := executeSingleQuery(tx, useSQL); err != nil {
			_ = tx.Rollback()
			return nil, fmt.Errorf("select database %s failed: %w", dbName, err)
		}
	}

	// 5. 批量执行SQL语句
	var results []sql.Result
	for idx, query := range s.queries {
		Logger.Info(fmt.Sprintf("Executing SQL (index: %d): %s", idx+1, query))

		// 执行单个SQL并记录耗时
		startTime := time.Now()
		result, err := tx.Exec(query)
		elapsed := time.Since(startTime)

		if err != nil {
			// ===== 新增：判断是否是表已存在错误（1050），并获取表创建时间 =====
			var mysqlErr *mysql.MySQLError
			if errors.As(err, &mysqlErr) && mysqlErr.Number == 1050 {
				// 提取表名（从CREATE TABLE语句中解析，适配常见写法）
				tableName := extractTableNameFromCreateSQL(query)
				// 查询表的创建时间
				createTime, getTimeErr := getTableCreateTime(tx, dbName, tableName)
				timeStr := "获取失败"
				if getTimeErr == nil {
					timeStr = createTime.Format("2006-01-02 15:04:05")
				}

				// 打印警告日志，包含表创建时间，忽略该错误继续执行
				Logger.Warn(fmt.Sprintf(
					"[忽略错误] 表已存在 (index: %d) [表名: %s, 创建时间: %s, 耗时: %v]: %v\nSQL: %s",
					idx+1, tableName, timeStr, elapsed, err, query,
				))
				continue // 跳过当前错误SQL，执行下一个
			}
			// ==============================================

			// 其他错误：正常回滚事务并返回错误
			rollbackErr := tx.Rollback()
			if rollbackErr != nil {
				Logger.Error(fmt.Sprintf("Rollback failed after SQL error: %v failed [elapsed: %v] ", rollbackErr, err.Error()))
			}
			return nil, fmt.Errorf(
				"execute SQL (index: %d) failed [elapsed: %v]: %w\nSQL: %s",
				idx+1, elapsed, err, query,
			)
		}

		// 记录执行结果和耗时
		results = append(results, result)
		Logger.Info(fmt.Sprintf("SQL executed successfully (index: %d), elapsed: %v", idx+1, elapsed))
	}

	// 6. 所有语句执行成功，提交事务
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit transaction failed: %w", err)
	}

	Logger.Info(fmt.Sprintf("All %d SQL queries executed successfully (transaction committed)", len(s.queries)))
	return results, nil
}

// extractTableNameFromCreateSQL 从CREATE TABLE语句中提取表名（适配常见写法）
func extractTableNameFromCreateSQL(sql string) string {
	// 简化版解析：匹配 "CREATE TABLE [IF NOT EXISTS] `table`" 或 "CREATE TABLE table" 格式
	// 如需更严谨，可使用SQL解析库（如github.com/xwb1989/sqlparser）
	sql = strings.TrimSpace(strings.ToUpper(sql))
	parts := strings.Fields(sql)
	tableName := "未知表名"

	// 遍历字段找表名（CREATE TABLE 后紧跟的是表名，可能跳过IF NOT EXISTS）
	for i, part := range parts {
		if part == "TABLE" && i+1 < len(parts) {
			nextPart := parts[i+1]
			// 跳过 IF NOT EXISTS
			if nextPart == "IF" && i+2 < len(parts) {
				nextPart = parts[i+2]
			}
			// 去除反引号/引号
			tableName = strings.Trim(nextPart, "`'\"")
			break
		}
	}
	return tableName
}

// getTableCreateTime 查询MySQL表的创建时间
func getTableCreateTime(tx *sql.Tx, dbName, tableName string) (time.Time, error) {
	if tableName == "未知表名" || dbName == "" {
		return time.Time{}, fmt.Errorf("表名或数据库名为空")
	}

	// 查询information_schema获取表创建时间
	query := `
		SELECT CREATE_TIME 
		FROM information_schema.TABLES 
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
	`
	var createTime time.Time
	err := tx.QueryRow(query, dbName, tableName).Scan(&createTime)
	if err != nil {
		return time.Time{}, fmt.Errorf("查询表创建时间失败: %w", err)
	}
	return createTime, nil
}

// executeSingleQuery 执行单个SQL语句（内部辅助函数）
func executeSingleQuery(tx *sql.Tx, query string) error {
	startTime := time.Now()
	_, err := tx.Exec(query)
	elapsed := time.Since(startTime)

	if err != nil {
		return fmt.Errorf("query failed [elapsed: %v]: %w\nSQL: %s", elapsed, err, query)
	}

	Logger.Info(fmt.Sprintf("Query executed successfully [elapsed: %v]: %s", elapsed, query))
	return nil
}

// saveTx 事务保存辅助函数（兼容旧代码，可保留备用）
func saveTx(tx *sql.Tx, err *error) {
	if p := recover(); p != nil {
		_ = tx.Rollback()
		panic(p)
	} else if *err != nil {
		_ = tx.Rollback()
	} else {
		commitErr := tx.Commit()
		*err = commitErr
		if commitErr != nil {
			Logger.Error(fmt.Sprintf("Transaction commit failed: %v", commitErr))
		} else {
			Logger.Info("Transaction committed successfully")
		}
	}
}
