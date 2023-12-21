package main

import (
	"fmt"
	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/security"
	"log"
	"os"
)

func main() {
	conf, err := odps.NewConfigFromIni(os.Args[1])
	if err != nil {
		log.Fatalf("%+v", err)
	}

	aliAccount := account.NewAliyunAccount(conf.AccessId, conf.AccessKey)
	odpsIns := odps.NewOdps(aliAccount, conf.Endpoint)
	var restClient = odpsIns.RestClient()

	sm := security.NewSecurityManager(restClient, conf.ProjectName)
	result, err := sm.RunQuery("install package aa.bb;", true, "")

	if err != nil {
		log.Fatalf("%+v", err)
	}

	println(fmt.Sprintf("ok: %s", result))
}
