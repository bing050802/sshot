package gauss

import (
	_ "gitee.com/opengauss/openGauss-connector-go-pq"
	"database/sql"
	"fmt"
)

func GetDriverName() string {
	return "opengauss"
}

func GetDialect() string {
	return "opengauss"
}

func GetDSN(user string, password string, host string, port int, database string) string {
	dsn := fmt.Sprintf(  "host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",host, port, user, password,  database)
	return dsn
}

func Open(dsn string) (db *sql.DB, err error) {
	db, err = sql.Open(GetDriverName(), dsn)
	if err != nil {
		return
	}
	return
}

