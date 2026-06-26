package types

// RemoteSyncTask 远程同步任务
type RemoteSyncTask struct {
	Src          string `yaml:"src"`                     // 源路径（远程），可以是目录或 .tar.gz
	Dest         string `yaml:"dest"`                    // 目标路径（远程），必须是目录
	Backup       string `yaml:"backup,omitempty"`        // 备份方式: "move", "tar", "none" (默认 none)
	DeleteBackup bool   `yaml:"delete_backup,omitempty"` // 同步后删除备份
	CleanDest    bool   `yaml:"clean_dest,omitempty"`    // 同步前清空目标目录（仅 backup=none 时有效）
	SrcSubdir    string `yaml:"src_subdir,omitempty"`    // 压缩包内要使用的子目录（可选）
	Exclude      string `yaml:"exclude,omitempty"`       // rsync 排除模式
	RsyncFlags   string `yaml:"rsync_flags,omitempty"`   // 额外 rsync 标志

	Flat      bool   `yaml:"flat,omitempty"`       // 新增：扁平化解压，不检查顶层目录匹配
	BackupDir string `yaml:"backup_dir,omitempty"` // 备份文件存放目录（可选）
}
