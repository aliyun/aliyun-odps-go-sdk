package main

import (
	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"log"
)

func main() {
	accessId := ""
	accessKey := ""
	endpoint := ""
	projectName := ""

	aliAccount := account.NewAliyunAccount(accessId, accessKey)
	odpsIns := odps.NewOdps(aliAccount, endpoint)
	odpsIns.SetDefaultProjectName(projectName)

	table := odpsIns.Table("user_test")
	err := table.AddPartitionAndWait(true, "age=10, hometown='ningbo'")
	if err != nil {
		log.Fatalf("%+v", err)
	}
}
