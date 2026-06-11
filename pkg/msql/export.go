package msql

import (
	"encoding/json"
	"errors"
	"github.com/team-ide/go-dialect/dialect"
	"github.com/team-ide/go-dialect/worker"
	"strings"
)


func DoExport(data DataBaseDSN,exportDialect,exportDir string,ExportStruct,ExportData bool,f func(progress *worker.TaskProgress))error {

	//if *exportOwner == "" {
	//	println("请输入 导出 库或表所属者")
	//	return
	//}

	Db, err := NewDbDriver(data)
	if err != nil {
		return err
	}
	dia, err := dialect.NewDialect(data.Driver)
	if err != nil {
        return err
	}
	if Db == nil || dia == nil {
		return errors.New("sourceDialect [" + data.Driver + "] not support")
	}

	exportDia, err := dialect.NewDialect(exportDialect)
	if err != nil {
		return err
	}
	dataSourceType := worker.GetDataSource("sql")

	var owners = getExportOwners(data.Database)
	bs, _ := json.Marshal(owners)
	Logger.Info("owners:", string(bs))

	skipOwnerStr := ""
	skipOwnerStr = strings.TrimSpace(skipOwnerStr)

	task := worker.NewTaskExport(Db.Db, dia, exportDia, &worker.TaskExportParam{
		SkipOwnerNames:  strings.Split(skipOwnerStr, ","),
		Owners:          owners,
		ExportStruct:    ExportStruct,
		ExportData:      ExportData,
		AppendOwnerName: false,
		Dir:             exportDir,
		ExportBatchSql:  true,
		FormatIndexName: func(ownerName string, tableName string, index *dialect.IndexModel) string {
			return tableName + "_" + index.IndexName
		},
		DataSourceType: dataSourceType,
		BatchNumber:    1000,
		ErrorContinue:  true,
		OnProgress: f,
	})
	err = task.Start()
	if err != nil {
		return err
	}
	Logger.Info("导出成功")
	return nil
}

func getExportOwners(ownerInfoStr string) (owners []*worker.TaskExportOwner) {
	ownerInfoStr = strings.TrimSpace(ownerInfoStr)
	ownerStrList := strings.Split(ownerInfoStr, ",")
	for _, ownerStr := range ownerStrList {
		ownerStr = strings.TrimSpace(ownerStr)
		if ownerStr == "" {
			continue
		}
		ss := strings.Split(ownerStr, "=")
		if len(ss) > 1 {
			owners = append(owners, &worker.TaskExportOwner{
				SourceName: strings.TrimSpace(ss[0]),
				TargetName: strings.TrimSpace(ss[1]),
			})
		} else if len(ss) > 0 {
			owners = append(owners, &worker.TaskExportOwner{
				SourceName: strings.TrimSpace(ss[0]),
			})
		}
	}
	return
}
