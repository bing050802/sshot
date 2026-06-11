package types

import (
	"sync"
)

var RunOnceTasks = struct {
	sync.RWMutex
	Executed map[string]bool
}{
	Executed: make(map[string]bool),
}

var ExecOptions ExecutionOptions

type Config struct {
	Inventory Inventory `yaml:"inventory"`
	Playbook  Playbook  `yaml:"playbook"`
}

// InventoryConfig represents a standalone inventory file
type InventoryConfig struct {
	Hosts     []Host     `yaml:"hosts,omitempty"`
	Groups    []Group    `yaml:"groups,omitempty"`
	SSHConfig *SSHConfig `yaml:"ssh_config,omitempty"`
}

type FactCollector struct {
	Name    string `yaml:"name"`
	Command string `yaml:"command"`
	Sudo    bool   `yaml:"sudo,omitempty"`
}

type FactsConfig struct {
	Collectors []FactCollector `yaml:"collectors,omitempty"`
}

// PlaybookConfig represents a standalone playbook file
type PlaybookConfig struct {
	Name     string      `yaml:"name"`
	Parallel bool        `yaml:"parallel,omitempty"`
	Facts    FactsConfig `yaml:"facts,omitempty"`
	Tasks    []Task      `yaml:"tasks"`
	Handlers []Task      `yaml:"handlers,omitempty"` // 关键
}

type ExecutionOptions struct {
	DryRun        bool
	Verbose       bool
	Progress      bool
	NoColor       bool
	FullOutput    bool
	InventoryFile string
}

type Inventory struct {
	Hosts     []Host     `yaml:"hosts,omitempty"`
	Groups    []Group    `yaml:"groups,omitempty"`
	SSHConfig *SSHConfig `yaml:"ssh_config,omitempty"`
}

type SSHConfig struct {
	User               string `yaml:"user,omitempty"`
	Password           string `yaml:"password,omitempty"`
	KeyFile            string `yaml:"key_file,omitempty"`
	KeyPassword        string `yaml:"key_password,omitempty"`
	UseAgent           bool   `yaml:"use_agent,omitempty"`
	Port               int    `yaml:"port,omitempty"`
	StrictHostKeyCheck *bool  `yaml:"strict_host_key_check,omitempty"`
}

type Group struct {
	Name      string   `yaml:"name"`
	Hosts     []Host   `yaml:"hosts"`
	Parallel  bool     `yaml:"parallel,omitempty"`
	Order     int      `yaml:"order,omitempty"`
	DependsOn []string `yaml:"depends_on,omitempty"`
}

type Host struct {
	Name               string                 `yaml:"name"`
	Address            string                 `yaml:"address,omitempty"`
	Hostname           string                 `yaml:"hostname,omitempty"`
	Port               int                    `yaml:"port"`
	User               string                 `yaml:"user"`
	Password           string                 `yaml:"password,omitempty"`
	KeyFile            string                 `yaml:"key_file,omitempty"`
	KeyPassword        string                 `yaml:"key_password,omitempty"`
	UseAgent           bool                   `yaml:"use_agent,omitempty"`
	StrictHostKeyCheck *bool                  `yaml:"strict_host_key_check,omitempty"`
	Vars               map[string]interface{} `yaml:"vars,omitempty"`

	// 新增 become/sudo配置，对标ansible become_user、become_pass
	BecomeUser string `yaml:"become_user,omitempty"` // 需要切换到的用户(root)
	BecomePass string `yaml:"become_pass,omitempty"` // 登录用户的sudo密码
}

type Playbook struct {
	Name     string      `yaml:"name"`
	Parallel bool        `yaml:"parallel,omitempty"`
	Facts    FactsConfig `yaml:"facts,omitempty"`
	Tasks    []Task      `yaml:"tasks"`
	Handlers []Task      `yaml:"handlers,omitempty"` // 关键
}

type Task struct {
	Name             string                 `yaml:"name"`
	Command          string                 `yaml:"command,omitempty"`
	Script           string                 `yaml:"script,omitempty"`
	Copy             *CopyTask              `yaml:"copy,omitempty"`
	Shell            string                 `yaml:"shell,omitempty"`
	Sudo             bool                   `yaml:"sudo,omitempty"`
	When             string                 `yaml:"when,omitempty"`
	Register         string                 `yaml:"register,omitempty"`
	OnlyGroups       []string               `yaml:"only_groups,omitempty"`
	SkipGroups       []string               `yaml:"skip_groups,omitempty"`
	LocalAction      string                 `yaml:"local_action,omitempty"`
	DelegateTo       string                 `yaml:"delegate_to,omitempty"`
	RunOnce          bool                   `yaml:"run_once,omitempty"`
	IgnoreError      bool                   `yaml:"ignore_error,omitempty"`
	Vars             map[string]interface{} `yaml:"vars,omitempty"`
	DependsOn        []string               `yaml:"depends_on,omitempty"`
	WaitFor          string                 `yaml:"wait_for,omitempty"`
	Retries          int                    `yaml:"retries,omitempty"`
	RetryDelay       int                    `yaml:"retry_delay,omitempty"`
	Timeout          int                    `yaml:"timeout,omitempty"`
	UntilSuccess     bool                   `yaml:"until_success,omitempty"`
	AllowedExitCodes []int                  `yaml:"allowed_exit_codes,omitempty"`

	Fetch   *FetchTask   `yaml:"fetch,omitempty"`   // 新增 fetch 字段
	File    *FileTask    `yaml:"file,omitempty"`    // 新增
	Systemd *SystemdTask `yaml:"systemd,omitempty"` // 新增
	Archive *ArchiveTask `yaml:"archive,omitempty"`

	Notify    []string `yaml:"notify,omitempty"`     // 新增：触发的 handler 名称列表
	OnFailure []string `yaml:"on_failure,omitempty"` // 失败时触发
	Always    []string `yaml:"always,omitempty"`     // 无论成败都触发

	Debug       *DebugTask   `yaml:"debug,omitempty"`
	SetFactTask *SetFactTask `yaml:"set_fact,omitempty"`
}

type CopyTask struct {
	Src       string `yaml:"src"`                 // 本地源文件或目录路径
	Dest      string `yaml:"dest"`                // 远程目标路径
	Mode      string `yaml:"mode,omitempty"`      // 文件权限模式
	Recursive bool   `yaml:"recursive,omitempty"` // 是否递归复制目录
	Preserve  bool   `yaml:"preserve,omitempty"`  // 是否保留文件属性（权限、时间）
}

// 新增 FetchTask 结构体
type FetchTask struct {
	Src  string `yaml:"src"`  // 远程源文件或目录路径
	Dest string `yaml:"dest"` // 本地目标路径
	Flat bool   `yaml:"flat"` // 是否扁平化（不保留目录结构）
}

type HostResult struct {
	Host    Host
	Success bool
	Error   error
	Output  string
}

// FileTask 文件操作任务
type FileTask struct {
	Path  string `yaml:"path"`           // 文件路径
	State string `yaml:"state"`          // 状态: touch(创建空文件), absent(删除文件), directory(创建目录)
	Mode  string `yaml:"mode,omitempty"` // 文件权限模式，如 "0644"
}

// SystemdTask systemd.go 服务管理任务
type SystemdTask struct {
	Name    string `yaml:"name"`              // 服务名称
	State   string `yaml:"state"`             // started, stopped, restarted, reloaded
	Enabled bool   `yaml:"enabled,omitempty"` // 是否设置开机自启
	Daemon  bool   `yaml:"daemon,omitempty"`  // 是否执行 daemon-reload
}

// ArchiveTask 压缩/解压缩任务
type ArchiveTask struct {
	Path   string `yaml:"path"`             // 源路径（要压缩的文件/目录，或要解压的归档文件）
	Dest   string `yaml:"dest"`             // 目标路径（压缩：生成的归档文件路径；解压：解压到的目录）
	State  string `yaml:"state,omitempty"`  // "present" (压缩) 或 "absent" (解压)，默认 present
	Format string `yaml:"format,omitempty"` // 压缩格式: "tar.gz", "tar.bz2", "tar.xz", "zip"。留空则从 dest 扩展名推断
	Remove bool   `yaml:"remove,omitempty"` // 压缩后是否删除源文件/目录（仅当 state=present 时有效）
}

type DebugTask struct {
	Msg string `yaml:"msg,omitempty"` // 要输出的消息（支持变量替换）
	Var string `yaml:"var,omitempty"` // 要输出的变量名称
}

type SetFactTask struct {
	Vars map[string]interface{} `yaml:",inline"`
}
