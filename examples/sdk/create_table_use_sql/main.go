package main

import (
	"log"
	"os"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
)

func main() {
	conf, err := odps.NewConfigFromIni(os.Args[1])
	if err != nil {
		log.Fatalf("%+v", err)
	}

	aliAccount := account.NewAliyunAccount(conf.AccessId, conf.AccessKey)
	odpsIns := odps.NewOdps(aliAccount, conf.Endpoint)
	odpsIns.SetDefaultProjectName(conf.ProjectName)

	sql := "CREATE TABLE IF NOT EXISTS data_type_demo (" +
		"ti TINYINT, " +
		"si SMALLINT, " +
		"i INT, " +
		"bi BIGINT, " +
		"b BINARY, " +
		"f FLOAT, " +
		"d DOUBLE, " +
		"dc DECIMAL(38,18), " +
		"vc VARCHAR(1000), " +
		"c CHAR(100), " +
		"s STRING, " +
		"da DATE, " +
		"dat DATETIME, " +
		"t TIMESTAMP, " +
		"bl BOOLEAN, " +
		"st STRUCT<arr:ARRAY<STRING>, name:STRING> " +
		") " +
		"partitioned by (p1 int, p2 string); "

	// 如果project的数据类型版本是1.0，需要通过下面的hints使用mc 2.0数据类型
	//hints := make(map[string]string)
	//hints["odps.sql.type.system.odps2"] = "true"
	//hints["odps.sql.decimal.odps2"] = "true"
	//ins, err := odpsIns.ExecSQlWithHints(sql, hints)

	ins, err := odpsIns.ExecSQl(sql)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	err = ins.WaitForSuccess()
	if err != nil {
		log.Fatalf("%+v", err)
	}
}
