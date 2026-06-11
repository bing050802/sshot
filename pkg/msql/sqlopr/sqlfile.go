// Package sqlfile provides a way to execute sql file easily
//
// For more usage see https://github.com/tanimutomo/sqlfile
package sqlopr

import (

	"database/sql"
	"fmt"
	"github.com/xwb1989/sqlparser"
	"io/ioutil"
	"log/slog"
	"os"
	"strings"
	"time"
)
var Logger *slog.Logger
// SqlFile represents a queries holder
type SqlFile struct {
	files   []string
	queries []string
}

// New create new SqlFile object
func New() *SqlFile {
	return &SqlFile{}
}

func (s *SqlFile) QueriesString() []string  {
	return s.queries
}
//func splitSQLUsingMySQLParser(inputStr string) ([]string, error) {
//	parser := sqlparser.New()
//	stmtList, err := parser.Parse(inputStr)
//	if err != nil {
//		fmt.Printf("Parse error: %v\n", err)
//		return
//	}
//
//	// 提取拆分后的语句
//	var statements []string
//	for _, stmt := range stmtList {
//		statements = append(statements, stmt.String())
//	}
//
//}
//

func parseSQLContent(inputStr string) ([]string, error) {
   //return splitSQLUsingMySQLParser(inputStr)

	//reader := strings.NewReader(string(inputStr))
	//
	//// 创建一个 bufio.Scanner 对象，以 strings.Reader 作为输入
	//scanner := bufio.NewScanner(reader)
	//var sqlContent string
	//for scanner.Scan() {
	//	sqlContent += scanner.Text() + "\n"
	//}
	//
	//if err := scanner.Err(); err != nil {
	//	return nil, err
	//}

	statements, err := sqlparser.SplitStatementToPieces(inputStr)
	//fmt.Println(statements)
	if err != nil {
		return nil, err
	}

	return statements, nil
}

// File add and load queries from input file
func (s *SqlFile) File(file string) error {

	sqlData, err := os.ReadFile(file)
	if err != nil {
		return  err
	}
	queries,err :=parseSQLContent(string(sqlData))

	if err != nil {
		return err
	}

	s.files = append(s.files, file)
	s.queries = append(s.queries, queries...)

	return nil
}
func (s *SqlFile) Queries() []string {
	return s.queries
}
func (s *SqlFile) Content(file string) error {
	queries, err := loadContent(file)
	if err != nil {
		return err
	}

	s.files = append(s.files, file)
	s.queries = append(s.queries, queries...)

	return nil
}

// Files add and load queries from multiple input files
func (s *SqlFile) Files(files ...string) error {
	for _, file := range files {
		if err := s.File(file); err != nil {
			return err
		}
	}
	return nil
}

// Directory add and load queries from *.sql files in specified directory
func (s *SqlFile) Directory(dir string) error {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return err
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		name := file.Name()
		if name[len(name)-3:] != "sql" {
			continue
		}

		if err := s.File(dir + "/" + name); err != nil {
			return err
		}
	}

	return nil
}

// Exec execute SQL statements written int the specified sql file
func (s *SqlFile) Exec(db *sql.DB) (res []sql.Result, err error) {
	tx, err := db.Begin()
	if err != nil {
		return res, err
	}
	//defer saveTx(tx, &err)
	//fmt.Println(s.queries)
	var rs []sql.Result
	for _, q := range s.queries {
		Logger.Info(fmt.Sprintf("sql 语句:%s",q))
		fmt.Println(fmt.Sprintf("sql 语句:%s",q))
		start := time.Now()
		r, err := tx.Exec(q)
		if err != nil {
			 err = fmt.Errorf(err.Error() + " : when executing > " + q)
			var err1 error
			saveTx(tx, &err1)
			if err1 != nil {
				Logger.Error(err1.Error())
			}
			return nil, err
		}
		elapsed := time.Since(start)

		// 输出执行结果和执行时间
		Logger.Info(fmt.Sprintf("执行sql 语句时间:%v",elapsed))
		rs = append(rs, r)
	}

	return rs, err
}

//// Load load sql file from path, and return SqlFile pointer
//func load(path string) (qs []string, err error) {
//	ls, err := readFileByLine(path)
//	if err != nil {
//		return qs, err
//	}
//
//	//var ncls []string
//	//for _, l := range ls {
//	//	ncl := excludeComment(l)
//	//	ncls = append(ncls, ncl)
//	//}
//	//
//	//l := strings.Join(ncls, "")
//	//qs = strings.Split(l, ";")
//	//qs = qs[:len(qs)-1]
//
//	var ncls []string
//	for _, l := range ls {
//		ncl := excludeComment(l)
//		ncls = append(ncls, ncl)
//	}
//
//	// 拼接行时保留换行符
//	l := strings.Join(ncls, "\n")
//	qs = strings.Split(l, ";")
//	// 去除最后一个空元素
//	for i := len(qs) - 1; i >= 0; i-- {
//		if strings.TrimSpace(qs[i]) == "" {
//			qs = append(qs[:i], qs[i+1:]...)
//		}
//	}
//
//	return qs, nil
//}
func loadContent(content string) (qs []string, err error) {

	statements, err := parseSQLContent(content)
	if err != nil {
		fmt.Printf("解析 SQL 出错: %v\n", err)
		return
	}
	return statements,nil


}


//func readFileByLine(path string) (ls []string, err error) {
//	f, err := os.ReadFile(path)
//	if err != nil {
//		return ls, err
//	}
//	statements, err := sqleton.Parse(string(sqlData))
//	if err != nil {
//		fmt.Printf("解析 SQL 出错: %v\n", err)
//		return
//	}
//	return statements,nil
//	ls = strings.Split(string(f), "\n")
//	return ls, nil
//}
//
//func readContentByLine(content string) (ls []string) {
//	ls = strings.Split(content, "\n")
//	return ls
//}

func excludeComment(line string) string {
	d := "\""
	s := "'"
	c := "--"

	var nc string
	ck := line
	mx := len(line) + 1

	for {
		if len(ck) == 0 {
			return nc
		}

		di := strings.Index(ck, d)
		si := strings.Index(ck, s)
		ci := strings.Index(ck, c)

		if di < 0 {
			di = mx
		}
		if si < 0 {
			si = mx
		}
		if ci < 0 {
			ci = mx
		}

		var ei int

		if di < si && di < ci {
			nc += ck[:di+1]
			ck = ck[di+1:]
			ei = strings.Index(ck, d)
		} else if si < di && si < ci {
			nc += ck[:si+1]
			ck = ck[si+1:]
			ei = strings.Index(ck, s)
		} else if ci < di && ci < si {
			return nc + ck[:ci]
		} else {
			return nc + ck
		}

		nc += ck[:ei+1]
		ck = ck[ei+1:]
	}
}

//func excludeComment(line string) string {
//	var result strings.Builder
//	inSingleQuote := false
//	inDoubleQuote := false
//	inBlockComment := false
//
//	for i := 0; i < len(line); i++ {
//		if inSingleQuote {
//			result.WriteByte(line[i])
//			if line[i] == '\'' {
//				inSingleQuote = false
//			}
//			continue
//		}
//		if inDoubleQuote {
//			result.WriteByte(line[i])
//			if line[i] == '"' {
//				inDoubleQuote = false
//			}
//			continue
//		}
//		if inBlockComment {
//			if i+1 < len(line) && line[i] == '*' && line[i+1] == '/' {
//				inBlockComment = false
//				i++
//			}
//			continue
//		}
//		if i+1 < len(line) && line[i] == '/' && line[i+1] == '*' {
//			inBlockComment = true
//			i++
//			continue
//		}
//		if i+1 < len(line) && line[i] == '-' && line[i+1] == '-' {
//			break
//		}
//		if line[i] == '\'' {
//			inSingleQuote = true
//		} else if line[i] == '"' {
//			inDoubleQuote = true
//		}
//		result.WriteByte(line[i])
//	}
//	return result.String()
//}

//func excludeComment(line string) string {
//	var result strings.Builder
//	inSingleQuote := false
//	inDoubleQuote := false
//	inBlockComment := false
//	for i := 0; i < len(line); i++ {
//		if inSingleQuote {
//			result.WriteByte(line[i])
//			if line[i] == '\'' {
//				inSingleQuote = false
//			}
//			continue
//		}
//		if inDoubleQuote {
//			result.WriteByte(line[i])
//			if line[i] == '"' {
//				inDoubleQuote = false
//			}
//			continue
//		}
//		if inBlockComment {
//			if i+1 < len(line) && line[i] == '*' && line[i+1] == '/' {
//				inBlockComment = false
//				i++
//			}
//			continue
//		}
//		if i+1 < len(line) && line[i] == '/' && line[i+1] == '*' {
//			inBlockComment = true
//			i++
//			continue
//		}
//		if i+1 < len(line) && line[i] == '-' && line[i+1] == '-' {
//			break
//		}
//		if line[i] == '\'' {
//			inSingleQuote = true
//		} else if line[i] == '"' {
//			inDoubleQuote = true
//		}
//		result.WriteByte(line[i])
//	}
//	return result.String()
//}
func saveTx(tx *sql.Tx, err *error) {
	if p := recover(); p != nil {
		tx.Rollback()
		panic(p)
	} else if *err != nil {
		tx.Rollback()
	} else {
		e := tx.Commit()
		err = &e
		//fmt.Println("commit")
	}
}
