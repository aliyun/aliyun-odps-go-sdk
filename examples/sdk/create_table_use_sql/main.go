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

	sql := "create table if not exists all_types_demo (" +
		"    tiny_int_type         tinyint," +
		"    small_int_type        smallint," +
		"    int_type              int," +
		"    bigint_type           bigint," +
		"    binary_type           binary," +
		"    float_type            float," +
		"    double_type           double," +
		"    decimal_type          decimal(10, 8)," +
		"    varchar_type          varchar(500)," +
		"    char_type             varchar(254)," +
		"    string_type           string," +
		"    date_type             date," +
		"    datetime_type         datetime," +
		"    timestamp_type        timestamp," +
		"    boolean_type          boolean," +
		"    map_type              map<string, bigint>," +
		"    array_type            array< string>," +
		"    struct_type           struct<arr:ARRAY<STRING>, name:STRING>" +
		") " +
		"partitioned by (p1 bigint, p2 string);"

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
