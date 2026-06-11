package msql

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"sshot/pkg/msql/sqlopr"
	"sshot/pkg/msql/sqlopr2"

	"github.com/melbahja/goph"
	"github.com/pkg/sftp"

	"github.com/team-ide/go-driver/db_dm"
	"github.com/team-ide/go-driver/db_postgresql"

	//"encoding/json"
	"fmt"
	"sshot/pkg/msql/gauss"

	"io"

	//_ "github.com/go-sql-driver/mysql"
	"github.com/team-ide/go-driver/db_kingbase_v8r6"

	//"github.com/jmoiron/sqlx"
	"github.com/team-ide/go-driver/db_mysql"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"
	//"go.mod/msql/sqlopr"
	"io/ioutil"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

var Logger *slog.Logger

type DbSql struct {
	Db     *sql.DB
	Driver string `json:"driver"`
	DBName string `json:"db_name"`
}

type DataBaseDSN struct {
	User     string `json:"userName"`
	Pwd      string `json:"password"`
	Port     int    `json:"port"`
	Host     string `json:"host"`
	Database string `json:"database"`
	Driver   string `json:"driver"`
	SSHPort  int    `json:"ssh_port"`
}

const DrivernameMyql = "mysql"
const DrivernameKingbase = "kingbase"
const DrivernameDm = "dm"
const DrivernamePostgre = "postgre"
const DrivernameGauss = "opengauss"
const DrivernamePS = "ps"
const DrivernameSQLServer = "SQLServer"

func init() {

}

func SetLogger(l *slog.Logger) {
	Logger = l
	sqlopr.Logger = l
	sqlopr2.Logger = l
}
func postGresqlConnect(d DataBaseDSN) (*sql.DB, error) {

	dsn := db_postgresql.GetDSN(d.User, d.Pwd, d.Host, d.Port, d.Driver)
	return db_postgresql.Open(dsn)

}

func kingbaseConnect(d DataBaseDSN) (*sql.DB, error) {

	dsn := db_kingbase_v8r6.GetDSN(d.User, d.Pwd, d.Host, d.Port, d.Database)
	return db_kingbase_v8r6.Open(dsn)

}
func dmConnect(d DataBaseDSN) (*sql.DB, error) {

	dsn := db_dm.GetDSN(d.User, d.Pwd, d.Host, d.Port, "")
	return db_dm.Open(dsn)

}

func opengaussConnect(d DataBaseDSN) (*sql.DB, error) {
	dsn := gauss.GetDSN(d.User, d.Pwd, d.Host, d.Port, d.Database)
	return gauss.Open(dsn)
}

func mysqlConnect(d DataBaseDSN) (*sql.DB, error) {

	dsn := db_mysql.GetDSN(d.User, d.Pwd, d.Host, d.Port, d.Database)
	return db_mysql.Open(dsn)

}

func NewDbDriver(dsn DataBaseDSN) (*DbSql, error) {
	var DB *sql.DB
	var err error
	driverName := dsn.Driver
	if driverName == DrivernameMyql {
		DB, err = mysqlConnect(dsn)
	} else if driverName == DrivernameKingbase {
		DB, err = kingbaseConnect(dsn)
	} else if driverName == DrivernameDm {
		DB, err = dmConnect(dsn)
	} else if driverName == DrivernamePostgre {
		DB, err = postGresqlConnect(dsn)
	} else if driverName == DrivernameGauss {
		DB, err = opengaussConnect(dsn)
	} else {
		DB, err = mysqlConnect(dsn)
	}
	//fmt.Println(DB,err)
	if err != nil {
		return &DbSql{}, err
	}

	err = DB.Ping()
	if err != nil {
		return &DbSql{}, err
	}
	return &DbSql{Db: DB, Driver: driverName}, nil

}

func crate() {

}

func CreateDataBase(dbUser, dbPwd, dbHost string, dbPort int, dbName, webfolderPath, driver string) error {
	dbDriverBase, err := NewDbDriver(DataBaseDSN{
		User:     dbUser,
		Pwd:      dbPwd,
		Port:     dbPort,
		Host:     dbHost,
		Database: dbName,
		Driver:   driver,
	})

	if err != nil {
		return err
	}
	err = dbDriverBase.CreateDataBase(dbName)
	if err != nil {
		return err
	}
	dbDriverBase.Db.Close()

	_, err = dbDriverBase.Setting()

	if err != nil { //创建数据库

		if strings.Contains(err.Error(), "Error 1146") {
			_, err = dbDriverBase.CreateTables(webfolderPath)
			if err != nil {
				return err
			}
		} else {
			return err
		}

	}

	return nil
}

//func (s *DbSql) TableExists(name string) error {
//
//	_, err := s.Db.Exec(fmt.Sprintf("SELECT COUNT(*) FROM information_schema.TABLES WHERE table_name ='%v';", name))
//    return err
//}

//func (s *DbSql) CreateDataBase(name string) error {
//
//	_, err := s.Db.Exec(fmt.Sprintf("CREATE DATABASE %v", name))
//	if err != nil && !strings.Contains(err.Error(), "Error 1007") {
//		log.Println(err)
//		return err
//	}
//
//	return nil
//}

func (s *DbSql) CreateUserAndGrants(user, pwd, frivileges string) error {
	cquery := fmt.Sprintf(`CREATE USER '%s'@'%%' IDENTIFIED BY '%s';`, user, pwd)
	_, err := s.Db.Exec(cquery)
	if err != nil && !strings.Contains(err.Error(), "Error 1396") {
		log.Println(err)
		return err
	}
	query := fmt.Sprintf(`Grant all on %v to '%v'@'%%'  with grant option;`, frivileges, user)
	//fmt.Println(query)
	_, err = s.Db.Exec(query)
	if err != nil {
		log.Println("授权失败", err)
		return err
	}
	return nil
}

func (s *DbSql) LockOtherAccountsExcept(names ...string) error {
	maxCons, exError := s.Db.Query("SELECT  user,host  FROM mysql.user;")
	if exError != nil {
		log.Println("LockOtherAccountsExcept", exError.Error())
		return exError

	}
	defer maxCons.Close()

	for maxCons.Next() {
		var (
			u sql.NullString
			h sql.NullString
		)
		if err := maxCons.Scan(&u, &h); err != nil {
			fmt.Println("\tLockOtherAccountsExcept：", err.Error())
			continue
		}
		var inExcept = false
		for _, value := range names {
			if value == u.String {
				inExcept = true
				break
			}

		}
		if inExcept {
			continue
		}

		query := fmt.Sprintf("ALTER USER '%v'@'%v' ACCOUNT LOCK;", u.String, h.String)
		//fmt.Println(query)
		_, err := s.Db.Exec(query)
		if err != nil {
			log.Println(err.Error())
		}

		//fmt.Println("\t", u, h)

	}
	return nil
}

func tableExists(db *sql.DB, dbType string, tableName string) (bool, error) {
	var query string
	var exists bool
	var err error

	// 处理 Kingbase 表名大小写问题
	if dbType == DrivernameKingbase {
		tableName = strings.ToUpper(tableName)
	}

	switch dbType {
	case DrivernamePostgre, DrivernamePS, DrivernameGauss, DrivernameKingbase:
		query = `
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = current_schema()
                AND table_name = $1
            )
        `
		err = db.QueryRow(query, tableName).Scan(&exists)
	case DrivernameMyql:
		query = `
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = DATABASE()
                AND table_name = ?
            )
        `
		err = db.QueryRow(query, tableName).Scan(&exists)
	case "SQLServer":
		query = `
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = SCHEMA_NAME()
                AND table_name = ?
            )
        `
		err = db.QueryRow(query, tableName).Scan(&exists)
	case DrivernameDm:
		query = `
            SELECT COUNT(*) > 0 
            FROM all_tables 
            WHERE table_name = ?
        `
		err = db.QueryRow(query, tableName).Scan(&exists)
	default:
		return false, fmt.Errorf("不支持的数据库类型: %s", dbType)
	}

	if err != nil {
		log.Printf("检查 %s 表是否存在时出错: %v", tableName, err)
		return false, err
	}
	return exists, nil
}

func (s *DbSql) setting(table string) (res map[string]string, err error) {

	res = map[string]string{}

	maxCons, exerror := s.Db.Query(fmt.Sprintf("select * from %v limit 0,1000", table))
	if exerror != nil {
		log.Println("无法找到setting表失败", exerror.Error())
		err = exerror
		return res, err
	}
	defer maxCons.Close()

	for maxCons.Next() {
		var (
			a sql.NullString
			b sql.NullString
			c sql.NullString
		)
		if err := maxCons.Scan(&a, &b, &c); err != nil {
			log.Println("\tsetting：", err.Error())
			continue
		}
		res[b.String] = c.String
		//fmt.Print("\t",a,"\t",b,"\t",c)

	}
	return res, nil
}

func (s *DbSql) setting2(table string) (res map[string]string, err error) {
	res = make(map[string]string)
	var query string
	switch s.Driver {
	case DrivernameMyql:
		query = fmt.Sprintf("SELECT * FROM %s LIMIT 0, 1000", table)
	case DrivernameKingbase, DrivernamePostgre, DrivernameGauss, DrivernamePS:
		query = fmt.Sprintf("SELECT * FROM %s LIMIT 1000 OFFSET 0", table)
	case DrivernameDm:
		query = fmt.Sprintf("SELECT * FROM (SELECT ROWNUM RN, t.* FROM %s t) WHERE RN BETWEEN 1 AND 1000", table)
	default:
		return res, fmt.Errorf("不支持的数据库驱动: %s", s.Driver)
	}

	rows, err := s.Db.Query(query)
	if err != nil {
		log.Printf("无法查询 %s 表: %v", table, err)
		return res, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			a sql.NullString
			b sql.NullString
			c sql.NullString
			d sql.NullString
		)
		if err := rows.Scan(&a, &b, &c, &d); err != nil {
			log.Printf("扫描 %s 表的行数据时出错: %v", table, err)
			continue
		}
		if c.Valid {
			res[c.String] = d.String
		}
	}

	if err := rows.Err(); err != nil {
		log.Printf("遍历 %s 表的结果集时出错: %v", table, err)
		return res, err
	}

	return res, nil
}

// func (s *DbSql) setting2(table string) (res map[string]string, err error) {
//
//		res = map[string]string{}
//
//		maxCons, exerror := s.Db.Query(fmt.Sprintf("select * from %v limit 0,1000",table))
//		if exerror != nil {
//			log.Println("无法找到setting表失败", exerror.Error())
//			err = exerror
//			return res, err
//		}
//		defer maxCons.Close()
//
//		for maxCons.Next() {
//			var (
//				a sql.NullString
//				b sql.NullString
//				c sql.NullString
//				d sql.NullString
//			)
//			if err := maxCons.Scan(&a, &b, &c,&d); err != nil {
//				log.Println("\tsetting2：", err.Error())
//				continue
//			}
//			res[c.String] = d.String
//			//fmt.Print("setting2\t",a,"\t",b,"\t",c,d,"\t",e,"\t",f,"\t",g)
//
//		}
//		return res, nil
//	}
func (s *DbSql) ClientPreviousVersion() (res map[string]string, err error) {

	res = map[string]string{}

	maxCons, exerror := s.Db.Query(fmt.Sprintf("select * from %v limit 0,1000", "cf_client_previous_version"))
	if exerror != nil {
		log.Println("无法找到setting表失败", exerror.Error())
		err = exerror
		return res, err
	}
	defer maxCons.Close()

	for maxCons.Next() {
		var (
			a sql.NullString
			b sql.NullString
			c sql.NullString
			d sql.NullString
			e sql.NullString
			f sql.NullString
			g sql.NullString
		)
		if err := maxCons.Scan(&a, &b, &c, &d, &e, &f, &g); err != nil {
			log.Println("\tClientPreviousVersion：", err.Error())
			continue
		}
		res[c.String] = d.String
		//fmt.Print("ClientPreviousVersion\t",a,"\t",b,"\t",c,d,"\t",e,"\t",f,"\t",g)

	}
	return res, nil
}

func (s *DbSql) patchRecord() (res []string, err error) {
	var query string
	table := "patch_record"
	if s.Driver == DrivernameKingbase {
		table = strings.ToUpper(table)
	}
	switch s.Driver {
	case DrivernameMyql:
		query = fmt.Sprintf("SELECT * FROM %s LIMIT 0, 1000", table)
	case DrivernameKingbase, DrivernamePostgre, DrivernameGauss, DrivernamePS:
		query = fmt.Sprintf("SELECT * FROM %s LIMIT 1000 OFFSET 0", table)
	case DrivernameDm:
		query = fmt.Sprintf("SELECT * FROM (SELECT ROWNUM RN, t.* FROM %s t) WHERE RN BETWEEN 1 AND 1000", table)
	default:
		return res, fmt.Errorf("不支持的数据库驱动: %s", s.Driver)
	}

	rows, err := s.Db.Query(query)
	if err != nil {
		log.Printf("无法查询 %s 表: %v", table, err)
		return res, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			a sql.NullString
			b sql.NullString
		)
		if err := rows.Scan(&a, &b); err != nil {
			log.Printf("扫描 %s 表的行数据时出错: %v", table, err)
			continue
		}
		if b.Valid {
			res = append(res, b.String)
		}
	}

	if err := rows.Err(); err != nil {
		log.Printf("遍历 %s 表的结果集时出错: %v", table, err)
		return res, err
	}

	return res, nil
}
func (s *DbSql) patchRecord2() (res []string, err error) {

	maxCons, exerror := s.Db.Query(fmt.Sprintf("select * from %v limit 0,1000", "patch_record"))
	if exerror != nil {
		log.Println("无法找到setting表失败", exerror.Error())
		err = exerror
		return res, err
	}
	defer maxCons.Close()

	for maxCons.Next() {
		var (
			a sql.NullString
			b sql.NullString
		)
		if err := maxCons.Scan(&a, &b); err != nil {
			fmt.Println("\tpatchRecord：", err.Error())
		}
		res = append(res, b.String)
		//fmt.Print("\t",a,"\t",b,"\t",c,d)

	}
	return res, nil
}

func (s *DbSql) Setting() (res map[string]string, err error) {

	table2 := "setting2"
	if s.Driver == DrivernameKingbase {
		table2 = strings.ToUpper(table2)
	}

	res, err = s.setting2(table2)
	//log.Println(res,err)
	if err == nil {
		return
	}
	return nil, err

}
func Utf8ToGbk(s []byte) ([]byte, error) {
	reader := transform.NewReader(bytes.NewReader(s), simplifiedchinese.GBK.NewEncoder())
	d, e := io.ReadAll(reader)
	if e != nil {
		return nil, e
	}
	return d, nil
}
func (s *DbSql) ExSqlFile(path string) (bool, error) {

	f := sqlopr.New()

	// Load input file and store queries written in the file
	err := f.File(path)
	if err != nil {
		return false, err
	}
	_, err = f.Exec(s.Db)
	if err != nil {
		return false, err
	}
	//log.Printf("%+v",result)
	return true, nil

	requests, b, err2 := SqlContent(path)
	if err2 != nil {
		return b, err2
	}

	for _, request := range requests {
		//fmt.Println("ss",request)
		if strings.TrimSpace(request) == "" {
			continue
		}
		data := request + ";"
		//dat,err :=Utf8ToGbk([]byte(data))
		//if err != nil {
		//	return false,  fmt.Errorf("无法读取内容:%v", err.Error())
		//}
		fmt.Println(data)
		_, err := s.Db.Exec(data)
		//fmt.Println(result,err)
		if err != nil {

			return false, err
		}
		// do whatever you need with result and error
	}

	return true, nil
	//_,err = s.Db.Exec(query)
	//if err != nil {
	//	return false,fmt.Errorf("执行失败%v",err.Error())
	//}
	//return true,nil
}

func Replace(request string) string {
	reg := regexp.MustCompile(`//[\s\S]*?\n`)
	str := reg.ReplaceAllString(request, "") //将空格替换为@字符

	reg = regexp.MustCompile(`-- [\s\S]*?\n`)
	str = reg.ReplaceAllString(str, "") //将空格替换为@字符
	reg = regexp.MustCompile(`#[\s\S]*?\n`)
	str = reg.ReplaceAllString(str, "") //将空格替换为@字符
	return str
}

func SqlContent(path string) ([]string, bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, false, fmt.Errorf("无法读取内容:%v", err.Error())
	}

	// Do something with stmt or err.

	//log.Println("执行sql脚本[", filepath.Base(path), "]")
	//fmt.Println(string(data))
	//query := fmt.Sprintf("source  %v ;",path)
	//fmt.Println(query)
	str := string(data)
	//fmt.Println(str)
	reg := regexp.MustCompile(`/\*{1,2}[\s\S]*?\*/`)
	str = reg.ReplaceAllString(str, "")
	str = Replace(str)
	//str=sqlopr.TrimSqlString(str)
	//fmt.Println(str)
	requests := strings.Split(str, ";")

	//fmt.Println("执行内容:",requests)

	return requests, false, nil
}

func (s *DbSql) CreateTables(path string) (currentVersion string, err error) {
	list := findWebfolderAllSqlFiles(path)
	if len(list) == 0 {
		return "-1", fmt.Errorf("未找到webfolder-all-**.sql %v", path)
	}

	max := list[0]
	for _, fi := range list {
		num := ExtractVersionNum(fi)
		maxN := ExtractVersionNum(max)
		if num > maxN {
			max = fi
		}
	}

	log.Println(max)
	p := filepath.Join(path, max)
	_, err = s.ExSqlFile2(p)
	return max, err

}

func (s *DbSql) UpdateWebVersion(path string, upAt string, f func(name string)) error {
	// 第一步：检查 setting 表
	res, err := s.Setting()
	if err != nil {
		Logger.Error(fmt.Sprintf("获取数据库Setting时出错: %v", err))
		table2 := "setting2"
		if s.Driver == DrivernameKingbase {
			table2 = strings.ToUpper(table2)
		}

		exists, _ := tableExists(s.Db, s.Driver, table2)

		if !exists {
			if _, err := s.CreateTables(path); err != nil {
				Logger.Error(fmt.Sprintf("创建数据表 %s 失败: %v", table2, err))
				return err
			}
		}
		f(fmt.Sprintf("数据库不存在:%s,任务是空库，导入WebfolderAll创建表", table2))
		// 重新获取设置
		res, err = s.Setting()
		if err != nil {
			Logger.Error(fmt.Sprintf("重新获取数据库设置时出错: %v", err))
			return err
		}
	}

	// 第二步：获取补丁记录
	patchs, err := s.patchRecord()
	if err != nil {
		Logger.Error(fmt.Sprintf("获取补丁记录时出错: %v", err))
		return err
	}
	Logger.Warn(fmt.Sprintf("List record of patches in database: %v", patchs))

	databaseVersion := res["database_version"]
	f(fmt.Sprintf("当前数据库版本:%s", databaseVersion))
	Logger.Warn(fmt.Sprintf("Current version of Server database: %s", databaseVersion))
	fmt.Println(fmt.Sprintf("Current version of Server database: %s", databaseVersion))
	// 第三步：获取当前数据库版本
	list := FindSqlFiles(path)
	aList := GetSqlFiles(path, upAt, databaseVersion, patchs, list)

	// 第七步：执行 SQL 文件
	for _, sqlFile := range aList {
		Logger.Warn(fmt.Sprintf("execute the script: %s", sqlFile))
		fmt.Println(fmt.Sprintf("execute the script: %s", sqlFile))
		p := filepath.Join(path, sqlFile)
		f(fmt.Sprintf("执行文件%s", p))
		if _, err := s.ExSqlFile(p); err != nil {
			Logger.Error(fmt.Sprintf("Failed to execute the script %s: %v", sqlFile, err))
			return err
		}
	}

	return nil
}

func GetSqlFiles(path string, upAt, databaseVersion string, patchs, list []string) []string {

	// 第四步：查找 SQL 文件并过滤已应用的补丁

	appliedPatchMap := make(map[string]bool)
	for _, patch := range patchs {
		appliedPatchMap[patch] = true
	}

	unappliedFiles := make([]string, 0)
	for _, file := range list {
		versionParts := versionNums(file)
		if len(versionParts) == 4 {
			versionStr := strings.Join(versionParts, ".")
			if !appliedPatchMap[versionStr] {
				unappliedFiles = append(unappliedFiles, file)
			}
		} else {
			unappliedFiles = append(unappliedFiles, file)
		}

	}

	// 第五步：根据版本号筛选 SQL 文件
	current := ExtractVersionNum(databaseVersion)
	at := ExtractVersionNum(upAt)
	var aList []string
	for _, file := range unappliedFiles {
		num := ExtractVersionNum(file)
		if num > current {
			if at > 0 && num <= at {
				aList = append(aList, file)
			} else if at <= 0 {
				aList = append(aList, file)
			}
		} else {
			if len(versionNums(file)) == 4 {
				aList = append(aList, file)
			}
		}
	}

	// 第六步：按版本号排序 SQL 文件
	sort.SliceStable(aList, func(i, j int) bool {
		return ExtractVersionNum(aList[i]) < ExtractVersionNum(aList[j])
	})
	Logger.Warn(fmt.Sprintf("List of scripts: %v", aList))
	return aList
}
func (s *DbSql) UpdateClient(path string) error {
	clients := findClientSqlFiles(path)
	log.Println("UpdateClients", clients)
	for _, s1 := range clients {
		p := filepath.Join(path, s1)
		log.Println("执行客户端脚本", filepath.Base(p))
		_, err := s.ExSqlFile(p)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *DbSql) UpdatePreviewClient(path string) error {
	_, err := os.Stat(path)
	if err != nil {
		return nil
	}

	_, err = s.ExSqlFile(path)
	if err != nil {
		return err
	}

	return nil
}

func findClientSqlFiles(dirname string) []string {

	list := make([]string, 0)
	fileInfos, err := os.ReadDir(dirname)
	if err != nil {
		log.Fatal(err)
		return list
	}
	for _, fi := range fileInfos {
		filename := dirname + "/" + fi.Name()
		fmt.Printf("%s\n", filename)
		if !fi.IsDir() {
			//继续遍历fi这个目录
			filename := fi.Name()
			if strings.Contains(filename, "do_not_execute") || strings.Contains(filename, "不执行") {
				continue
			}

			if strings.HasSuffix(filename, ".sql") {
				list = append(list, fi.Name())
			}
		}
	}
	return list
}
func isHasRecord(str, version string) bool {
	re := "INSERT" + `.*` + "`patch_record`" + `.*` + fmt.Sprintf("'%v'", version)

	fmt.Println(re)
	regx := regexp.MustCompile(re)
	match := regx.MatchString(str)
	if match {
		r := regx.FindAllString(str, -1)
		fmt.Println(r)
	}
	fmt.Println(re, match)
	return match
}

func doNotExecute(name string) bool {

	if strings.Contains(name, "do_not_execute") || strings.Contains(name, "不执行") {
		return true
	}
	return false
}
func FindSqlFiles(dirname string) []string {

	list := make([]string, 0)
	fileInfos, err := os.ReadDir(dirname)
	if err != nil {
		log.Println(err.Error())
		log.Fatal(err)
	}
	for _, fi := range fileInfos {
		//filename1 := dirname + "/" + fi.Name()
		//fmt.Printf("%s\n", filename1)
		filename := fi.Name()
		if doNotExecute(filename) {
			continue
		}
		if !fi.IsDir() {
			//继续遍历fi这个目录
			filename := strings.ToLower(fi.Name())
			if (!strings.Contains(filename, "all") && strings.HasSuffix(filename, ".sql")) && isMySqlFile(filename) {
				list = append(list, fi.Name())
			}
		}
	}
	return list
}

func isMySqlFile(filename string) bool {
	if strings.HasPrefix(filename, "webfolder") || strings.HasPrefix(filename, "preview") {
		return true
	}
	return false
}

func findWebfolderAllSqlFiles(dirname string) []string {

	list := make([]string, 0)
	fileInfos, err := ioutil.ReadDir(dirname)
	if err != nil {
		fmt.Println(err.Error())
		log.Fatal(err)
	}
	for _, fi := range fileInfos {
		//filename := dirname + "/" + fi.Name()
		filename := fi.Name()
		if doNotExecute(filename) {
			continue
		}
		if !fi.IsDir() {
			//继续遍历fi这个目录
			filename := strings.ToLower(fi.Name())
			if (strings.Contains(filename, "all") && strings.HasSuffix(filename, ".sql")) && isMySqlFile(filename) {
				list = append(list, fi.Name())
			}
		}
	}
	return list
}
func exponent(a, n uint64) uint64 {
	result := uint64(1)
	for i := n; i > 0; i >>= 1 {
		if i&1 != 0 {
			result *= a
		}
		a *= a
	}
	return result
}
func myReverse(l []string) {
	for i := 0; i < int(len(l)/2); i++ {
		li := len(l) - i - 1
		fmt.Println(i, "<=>", li)
		l[i], l[li] = l[li], l[i]
	}
}

func versionNums(ver string) []string {
	re := regexp.MustCompile(`\d+`)
	return re.FindAllString(ver, -1)
}

// ExtractVersionNum 将版本号字符串转换为 uint64 类型的数字
func ExtractVersionNum(ver string) uint64 {
	nums := versionNums(ver)
	var num uint64
	const digits = 4

	// 补零使版本号长度达到 4 位
	for len(nums) < digits {
		nums = append(nums, "0")
	}

	for i, v := range nums {
		n, _ := strconv.Atoi(v)
		// 避免使用 math.Pow10 以防止精度问题
		for j := 0; j < digits-1-i; j++ {
			n *= 10
		}
		num += uint64(n)
	}
	return num
}

//func versonsNums(ver string)(nums []string)  {
//	versNums :=strings.Split(ver,".")
//	for _,v := range versNums{
//		numv := make([]string, 0)
//		for _, ch := range []byte(v) {
//			//fmt.Println(ch,)
//			_, er := strconv.Atoi(string(ch))
//			if er == nil {
//				numv = append(numv, string(ch))
//			}
//
//		}
//		if len(numv) > 0 {
//			nums = append(nums, strings.Join(numv,""))
//		}
//
//
//	}
//	return
//}

//func ExtractVersionNum(ver string) uint64 {
//	nums := versonsNums(ver)
//
//
//
//	var num uint64
//	//fmt.Println(nums)
//
//	const digits = 4
//
//	for len(nums) <= digits {
//
//		nums = append(nums, "0")
//	}
//	//fmt.Println(nums)
//	p :=len(nums)
//	for i,v := range nums {
//		n,_ :=strconv.Atoi(v)
//		u := uint64(n) * uint64(math.Pow10(p-i))
//		//fmt.Println(u,n)
//		num +=  u
//	}
//   //fmt.Println(num)
//
//	return uint64(num)
//}

func (s *DbSql) ExecQuery(query string) (result []map[string]interface{}, columns []string, rows *sql.Rows, err error) {
	// Execute the query and retrieve the result set
	rows, err = s.Db.Query(query)
	if err != nil {
		log.Printf("执行查询出错: %v", err)
		return nil, nil, nil, err
	}

	// Initialize the result with empty slice
	result = make([]map[string]interface{}, 0)

	// Retrieve the column names & types from the result set
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		log.Printf("获取列类型出错: %v", err)
		rows.Close()
		return nil, nil, nil, err
	}
	columns, err = rows.Columns()
	if err != nil {
		log.Printf("获取列名出错: %v", err)
		rows.Close()
		return nil, nil, nil, err
	}

	// Create a slice interface to be used for raw scan
	values := make([]interface{}, len(columns))
	for i := range values {
		values[i] = new(interface{})
		if columns[i] == "" {
			columns[i] = fmt.Sprintf("column-%d", i+1)
		}
	}

	// Loop through the result set and build row map and add it to the results
	for rows.Next() {
		// Initialize a raw map
		rowMap := make(map[string]interface{})

		// Scan the values from the result set into the slice
		err = rows.Scan(values...)
		if err != nil {
			log.Printf("扫描行数据出错: %v", err)
			rows.Close()
			return nil, nil, nil, err
		}

		// Loop through the columns and set the key/value pairs in the map
		for i, colName := range columns {
			val := *(values[i].(*interface{}))

			// Handle decimal type and convert it to json number
			byteSlice, ok := val.([]uint8)
			var dbType string
			if s.Driver == DrivernameMyql || s.Driver == DrivernameSQLServer {
				dbType = "DECIMAL"
			} else if s.Driver == DrivernamePostgre || s.Driver == DrivernameKingbase || s.Driver == DrivernameGauss {
				dbType = "NUMERIC"
			} else if s.Driver == DrivernameDm {
				dbType = "DECIMAL" // 达梦数据库的 DECIMAL 类型处理
			}
			if ok && columnTypes[i].DatabaseTypeName() == dbType {
				rowMap[colName] = json.Number(byteSlice)
			} else {
				rowMap[colName] = val
			}
		}

		// Append the row map to the results slice
		result = append(result, rowMap)
	}
	if err := rows.Err(); err != nil {
		log.Printf("遍历结果集出错: %v", err)
		rows.Close()
		return nil, nil, nil, err
	}

	return result, columns, rows, nil
}

func uint8B(s []uint8) bool {
	for v, _ := range s {
		if v < 0 || v > 255 {
			return false
		}
	}
	return true
}
func PatchRecord(file string) (res []string) {
	f := sqlopr.New()

	// Load input file and store queries written in the file
	err := f.File(file)
	if err != nil {
		return
	}

	return extractPatchVersions(f.QueriesString())

}

// extractPatchVersions 从 SQL 插入语句数组中提取 patch_record 表的版本号
func extractPatchVersions(statements []string) []string {
	var versions []string
	// 预编译正则表达式以提高性能
	insertKeywordRegex := regexp.MustCompile(`(?i)^INSERT\s+INTO\s+`)
	tableNameRegex := regexp.MustCompile(`(?i)patch_record`)
	versionRegex := regexp.MustCompile(`'(\d+\.\d+\.\d+\.\d+)'`)

	for _, stmt := range statements {
		// 第一步：判断是否为 INSERT 语句
		if !insertKeywordRegex.MatchString(stmt) {
			continue
		}
		// 第二步：判断是否是针对 patch_record 表的操作
		if !tableNameRegex.MatchString(stmt) {
			continue
		}
		// 第三步：查找版本号
		matches := versionRegex.FindStringSubmatch(stmt)
		if len(matches) > 1 {
			versions = append(versions, matches[1])
		}
	}
	return versions
}

// FindMergeFiles 查找需要合并的 SQL 文件
func FindMergeFiles(path string) []string {
	webfolderFiles := findWebfolderAllSqlFiles(path)
	var result []string

	// 找到 webfolder 中版本号最大的文件
	if len(webfolderFiles) > 0 {
		maxFile := webfolderFiles[0]
		for _, file := range webfolderFiles {
			if ExtractVersionNum(file) > ExtractVersionNum(maxFile) {
				maxFile = file
			}
		}
		result = append(result, maxFile)
	}

	allFiles := FindSqlFiles(path)
	if len(allFiles) > 0 {
		maxFile := "webfolder0.0.0.0.sql"
		var records []string
		if len(result) > 0 {
			maxFile = result[0]
			records = PatchRecord(filepath.Join(path, maxFile))
		}

		currentVersion := ExtractVersionNum(maxFile)
		var candidateFiles []string

		for _, file := range allFiles {
			fileVersion := ExtractVersionNum(file)
			if fileVersion > currentVersion {

				candidateFiles = append(candidateFiles, file)

			} else {
				if len(records) == 0 {
					continue
				}
				versionParts := versionNums(file)
				if len(versionParts) == 4 {
					versionStr := strings.Join(versionParts, ".")
					found := false
					for _, record := range records {
						fmt.Println("record", record, versionStr)
						if versionStr == record {
							found = true
							break
						}
					}
					if !found {
						candidateFiles = append(candidateFiles, file)
					}

				}
			}
		}

		sort.SliceStable(candidateFiles, func(i, j int) bool {
			return ExtractVersionNum(candidateFiles[i]) < ExtractVersionNum(candidateFiles[j])
		})
		result = append(result, candidateFiles...)
	}

	return result
}

// CreateMySQLDatabaseIfNotExists 检查 MySQL 数据库是否存在，如果不存在则创建
func CreateMySQLDatabaseIfNotExists(dsn *DataBaseDSN) error {
	// 先尝试连接到 MySQL 服务器，不指定数据库
	dsnWithoutDB := DataBaseDSN{
		User:     dsn.User,
		Pwd:      dsn.Pwd,
		Port:     dsn.Port,
		Host:     dsn.Host,
		Database: "", // 不指定数据库
		Driver:   dsn.Driver,
	}

	// 连接到 MySQL 服务器
	db, err := NewDbDriver(dsnWithoutDB)
	if err != nil {
		return fmt.Errorf("无法连接到 MySQL 服务器: %w", err)
	}
	defer db.Db.Close()

	// 检查数据库是否存在
	query := fmt.Sprintf("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '%s'", dsn.Database)
	var dbName string
	err = db.Db.QueryRow(query).Scan(&dbName)
	if err != nil {
		if err == sql.ErrNoRows {
			// 数据库不存在，创建数据库
			err = db.CreateDataBase(dsn.Database)
			if err != nil {
				return fmt.Errorf("创建数据库失败: %w", err)
			}
			log.Printf("数据库 %s 已创建", dsn.Database)
		} else {
			return fmt.Errorf("检查数据库是否存在时出错: %w", err)
		}
	} else {
		log.Printf("数据库 %s 已存在", dsn.Database)
	}

	return nil
}

// CreateDataBase 创建数据库
func (s *DbSql) CreateDataBase(name string) error {
	if name == "" {
		return fmt.Errorf("数据库名不能为空")
	}

	sqlQuery := fmt.Sprintf("CREATE DATABASE `%s`", name)
	log.Printf("执行的 SQL 语句: %s", sqlQuery)

	_, err := s.Db.Exec(sqlQuery)
	if err != nil && !strings.Contains(err.Error(), "Error 1007") {
		log.Println(err)
		return err
	}

	return nil
}
func findMAXWebfolderAllSqlFile(c *sftp.Client, dirname string) []string {

	list := make([]string, 0)
	fileInfos, err := c.ReadDir(dirname)
	if err != nil {
		fmt.Println(err.Error())
		log.Fatal(err)
	}
	for _, fi := range fileInfos {
		//filename := dirname + "/" + fi.Name()
		filename := fi.Name()
		if doNotExecute(filename) {
			continue
		}
		if !fi.IsDir() {
			//继续遍历fi这个目录
			filename := strings.ToLower(fi.Name())
			if (strings.Contains(filename, "all") && strings.HasSuffix(filename, ".sql")) && isMySqlFile(filename) {
				list = append(list, fi.Name())
			}
		}
	}
	return list
}

func (s *DbSql) UpdateRemoteWebVersion(client *goph.Client, mysqlCfg MySQLConfig, path string, upAt string, f func(f string)) error {
	// 第一步：检查 setting 表
	res, err := s.Setting()
	if err != nil {

		table2 := "setting2"
		if s.Driver == DrivernameKingbase {
			table2 = strings.ToUpper(table2)
		}
		exists, err := tableExists(s.Db, s.Driver, table2)
		if err != nil {

			return err
		}
		f(fmt.Sprintf("数据库不存在:%s,任务是空库，导入WebfolderAll创建表", table2))
		if !exists {
			file, err := s.ExWebfolderAllFile(client, mysqlCfg, path, func(fs string) {
				f(fmt.Sprintf("执行文件%s", fs))
			})
			if err != nil {
				return fmt.Errorf("ExWebfolderAllFile%s", err.Error())
			}
			f(file)

		}
		// 重新获取设置
		res, err = s.Setting()
		if err != nil {
			return err
		}
	}

	// 第二步：获取补丁记录
	patchs, err := s.patchRecord()
	if err != nil {
		return err
	}

	databaseVersion := res["database_version"]

	f(fmt.Sprintf("当前数据库版本:%s", databaseVersion))
	// 第三步：获取当前数据库版本
	remoteFiles, err := FindRemoteSqlFiles(client, path)
	if err != nil {
		return err
	}
	aList := GetSqlFiles(path, upAt, databaseVersion, patchs, remoteFiles)

	// 第七步：执行 SQL 文件
	for _, sqlFile := range aList {
		p := filepath.Join(path, sqlFile)
		f(fmt.Sprintf("导入文件%s", p))
		if err = ImportSQLToMySQL(client, mysqlCfg, p); err != nil {
			return err
		}
	}

	return nil
}

func (s *DbSql) ExWebfolderAllFile(clnt *goph.Client, mysqlCfg MySQLConfig, path string, f func(f string)) (file string, err error) {
	c, err := clnt.NewSftp()
	if err != nil {
		return "", err
	}
	defer c.Close()
	list, err := findRemoteMaxWebfolderAllSqlFile(c, path)
	if err != nil {
		return "", err
	}
	f(list)
	return list, ImportSQLToMySQL(clnt, mysqlCfg, list)

}

func findRemoteMaxWebfolderAllSqlFile(sftp *sftp.Client, dirname string) (string, error) {
	var list []string
	fileInfos, err := sftp.ReadDir(dirname)
	if err != nil {
		return "", fmt.Errorf("读取目录 %s 时出错: %v", dirname, err)
	}
	for _, fi := range fileInfos {
		filename := fi.Name()
		if doNotExecute(filename) {
			continue
		}
		if !fi.IsDir() {
			filename = strings.ToLower(filename)
			if strings.Contains(filename, "all") && strings.HasSuffix(filename, ".sql") && isMySqlFile(filename) {
				list = append(list, fi.Name())
			}
		}
	}
	log.Println("findRemoteMaxWebfolderAllSqlFile", list)
	if len(list) == 0 {
		return "", fmt.Errorf("未找到脚本")
	}

	max := list[0]
	for _, fi := range list {
		num := ExtractVersionNum(fi)
		maxN := ExtractVersionNum(max)
		if num > maxN {
			max = fi
		}
	}
	p := filepath.Join(dirname, max)
	return p, nil
}

func FindRemoteSqlFiles(c *goph.Client, dirname string) ([]string, error) {
	ftp, err := c.NewSftp()
	if err != nil {
		return nil, err
	}
	var list []string
	fileInfos, err := ftp.ReadDir(dirname)
	if err != nil {
		return nil, fmt.Errorf("读取目录 %s 时出错: %v", dirname, err)
	}
	for _, fi := range fileInfos {
		filename := fi.Name()
		if doNotExecute(filename) {
			continue
		}
		if !fi.IsDir() {
			filename = strings.ToLower(filename)
			if !strings.Contains(filename, "all") && strings.HasSuffix(filename, ".sql") && isMySqlFile(filename) {
				list = append(list, fi.Name())
			}
		}
	}
	return list, nil
}

type MySQLConfig struct {
	User     string // MySQL 用户名
	Password string // MySQL 密码
	DBName   string // 数据库名
}

func ImportSQLToMySQL(client *goph.Client, mysqlCfg MySQLConfig, remoteSQLPath string) error {
	// 构建 MySQL 导入命令

	mysqlPath, err := FindMySQLPath(client, "mysql")
	if err != nil {
		return fmt.Errorf("查找 MySQL 路径失败: %w", err)
	}

	importCmd := fmt.Sprintf(
		"%s -u %s -p'%s' %s < %s",
		mysqlPath, mysqlCfg.User, mysqlCfg.Password, mysqlCfg.DBName, filepath.ToSlash(remoteSQLPath),
	)
	Logger.Info("importCmd", importCmd)
	// 执行命令
	output, err := client.Run(importCmd)
	if err != nil {
		fmt.Printf("执行 MySQL 导入失败: %w\n命令输出: %s", err, string(output))
		return fmt.Errorf("执行 MySQL 导入失败: %w\n命令输出: %s", err, string(output))
	}

	fmt.Printf("MySQL 导入成功，命令输出: %s\n", string(output))
	return nil
}
func SourceSQLDataToMySQL(client *goph.Client, mysqlCfg MySQLConfig, remoteSQLPath string) error {
	// 构建 MySQL 导入命令

	mysqlPath, err := FindMySQLPath(client, "mysql")
	if err != nil {
		return fmt.Errorf("查找 MySQL 路径失败: %w", err)
	}

	importCmd := fmt.Sprintf(
		"%s -u %s -p'%s' %s --execute=\"SET autocommit=0; SET unique_checks=0; SET foreign_key_checks=0; SOURCE %s; COMMIT;\"",
		mysqlPath, mysqlCfg.User, mysqlCfg.Password, mysqlCfg.DBName, filepath.ToSlash(remoteSQLPath),
	)

	// 执行命令
	output, err := client.Run(importCmd)
	if err != nil {
		fmt.Printf("执行 MySQL 导入失败: %w\n命令输出: %s", err, string(output))
		return fmt.Errorf("执行 MySQL 导入失败: %w\n命令输出: %s", err, string(output))
	}

	fmt.Printf("MySQL 导入成功，命令输出: %s\n", string(output))
	return nil
}

func FindMySQLPath(client *goph.Client, my string) (string, error) {

	cmd := fmt.Sprintf(`bash -lc 'which %s'`, my)

	output, err := client.Run(cmd)
	if err != nil {
		return "", fmt.Errorf("未找到 MySQL 命令，请确保已安装: %w\n输出: %s", err, string(output))
	}

	mysqlPath := strings.TrimSpace(string(output))
	if mysqlPath == "" {
		return "", fmt.Errorf("未找到 MySQL 命令，请确保已安装")
	}

	return filepath.ToSlash(mysqlPath), nil
}

func ExportMySQLDatabase(client *goph.Client, mysqlCfg MySQLConfig, remoteOutputPath string) error {

	mysqlPath, err := FindMySQLPath(client, "mysqldump")
	if err != nil {
		return fmt.Errorf("查找 MySQL 路径失败: %w", err)
	}
	// 构建 mysqldump 命令
	cmd := fmt.Sprintf(
		"%s --opt --single-transaction --quick --order-by-primary --set-gtid-purged=OFF --default-character-set=utf8mb4 -u %s --password='%s' %s > '%s'",
		mysqlPath, mysqlCfg.User, mysqlCfg.Password, mysqlCfg.DBName, filepath.ToSlash(remoteOutputPath),
	)
	Logger.Info("ExportMySQLDatabase", cmd)
	// 执行命令
	output, err := client.Run(cmd)
	if err != nil {
		return fmt.Errorf("执行 mysqldump 失败: %w\n输出: %s", err, string(output))
	}
	fmt.Printf("MySQL 导出成功，命令输出: %s\n", string(output))
	return nil
}

func (s *DbSql) UpdateWebVersion2(path string, upAt string, f func(name string)) error {
	// 第一步：检查 setting 表
	res, err := s.Setting()
	if err != nil {
		Logger.Error(fmt.Sprintf("获取数据库Setting时出错: %v", err))
		table2 := "setting2"
		if s.Driver == DrivernameKingbase {
			table2 = strings.ToUpper(table2)
		}

		exists, _ := tableExists(s.Db, s.Driver, table2)

		if !exists {
			if _, err := s.CreateTables(path); err != nil {
				Logger.Error(fmt.Sprintf("创建数据表 %s 失败: %v", table2, err))
				return err
			}
		}
		f(fmt.Sprintf("数据库不存在:%s,任务是空库，导入WebfolderAll创建表", table2))
		// 重新获取设置
		res, err = s.Setting()
		if err != nil {
			Logger.Error(fmt.Sprintf("重新获取数据库设置时出错: %v", err))
			return err
		}
	}

	// 第二步：获取补丁记录
	patchs, err := s.patchRecord()
	if err != nil {
		Logger.Error(fmt.Sprintf("获取补丁记录时出错: %v", err))
		return err
	}
	Logger.Warn(fmt.Sprintf("List record of patches in database: %v", patchs))

	databaseVersion := res["database_version"]
	f(fmt.Sprintf("当前数据库版本:%s", databaseVersion))
	Logger.Warn(fmt.Sprintf("Current version of Server database: %s", databaseVersion))
	fmt.Println(fmt.Sprintf("Current version of Server database: %s", databaseVersion))
	// 第三步：获取当前数据库版本
	list := FindSqlFiles(path)
	aList := GetSqlFiles(path, upAt, databaseVersion, patchs, list)

	// 第七步：执行 SQL 文件
	for _, sqlFile := range aList {
		Logger.Warn(fmt.Sprintf("execute the script: %s", sqlFile))
		fmt.Println(fmt.Sprintf("execute the script: %s", sqlFile))
		p := filepath.Join(path, sqlFile)
		f(fmt.Sprintf("执行文件%s", p))
		if _, err := s.ExSqlFile2(p); err != nil {
			Logger.Error(fmt.Sprintf("Failed to execute the script %s: %v", sqlFile, err))
			return err
		}
	}

	return nil
}

func (s *DbSql) ExSqlFile2(path string) (bool, error) {

	f := sqlopr2.New()

	// Load input file and store queries written in the file
	err := f.File(path)
	if err != nil {
		return false, err
	}
	_, err = f.Exec(s.Db, s.DBName)
	if err != nil {
		return false, err
	}
	//log.Printf("%+v",result)
	return true, nil

}
