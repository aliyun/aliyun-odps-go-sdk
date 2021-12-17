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

	sql := "create table if not exists user_test (" +
		"name string,score int,birthday datetime,addresses array<string>" +
		") " +
		"partitioned by (age int,hometown string) " +
		"lifecycle 2;"
	ins, err := odpsIns.ExecSQl(sql)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	err = ins.WaitForSuccess()
	if err != nil {
		log.Fatalf("%+v", err)
	}
}
