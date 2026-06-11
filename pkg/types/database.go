package types

// DBExecFileTask 执行本地 SQL 文件
type DBExecFileTask struct {
	Driver   string `yaml:"driver"` // mysql, kingbase, postgres, etc.
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
	Path     string `yaml:"path"` // SQL 文件的本地路径
	LogFile  string `yaml:"log_file,omitempty"`
}

// DBExecSQLTask 执行 SQL 语句
type DBExecSQLTask struct {
	Driver   string `yaml:"driver"`
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
	Query    string `yaml:"query"` // SQL 语句，多条用 ; 分隔
	LogFile  string `yaml:"log_file,omitempty"`
}

// DBMigrateTask 执行数据库版本升级（扫描目录并按版本号执行未应用的 SQL 文件）
type DBMigrateTask struct {
	Driver        string `yaml:"driver"`
	Host          string `yaml:"host"`
	Port          int    `yaml:"port"`
	User          string `yaml:"user"`
	Password      string `yaml:"password"`
	Database      string `yaml:"database"`
	ScriptsPath   string `yaml:"scripts_path"`   // 存放升级脚本的目录（本地）
	TargetVersion string `yaml:"target_version"` // 目标版本，如 "9.9.9.9"
	LogFile       string `yaml:"log_file,omitempty"`
}
