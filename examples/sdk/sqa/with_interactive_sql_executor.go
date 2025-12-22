package main

import (
	"fmt"
	"log"
	"os"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/sqa"
)

func main() {
	conf, err := odps.NewConfigFromIni(os.Args[1])
	if err != nil {
		log.Fatalf("%+v", err)
	}

	aliAccount := account.NewAliyunAccount(conf.AccessId, conf.AccessKey)
	odpsIns := odps.NewOdps(aliAccount, conf.Endpoint)
	odpsIns.SetDefaultProjectName(conf.ProjectName)
	sql := `select 1;`
	//
	paramInfo := sqa.SQLExecutorQueryParam{
		OdpsIns:        odpsIns,
		TaskName:       "test_mcqa",
		ServiceName:    sqa.DEFAULT_SERVICE,
		RunningCluster: "",
	}
	ie := sqa.NewInteractiveSQLExecutor(&paramInfo)
	err = ie.Run(sql, nil)
	if err != nil {
		log.Fatalf("%+v", err)
	}
	//
	records, err := ie.GetResult(0, 10, 10000, true)
	if err != nil {
		log.Fatalf("%+v", err)
	}
	//
	for _, record := range records {
		for i, d := range record {
			if d == nil {
				fmt.Printf("null")
			} else {
				fmt.Printf("%s", d.Sql())
			}

			if i < record.Len()-1 {
				fmt.Printf(", ")
			} else {
				fmt.Println()
			}
		}
	}
	err = ie.Close()
	if err != nil {
		log.Fatalf("%+v", err)
	}
}
