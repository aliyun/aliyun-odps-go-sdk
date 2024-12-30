package main

import (
	"log"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/options"
)

func main() {
	// Specify the ini file path
	configPath := "config.ini"
	conf, err := odps.NewConfigFromIni(configPath)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	aliAccount := account.NewAliyunAccount(conf.AccessId, conf.AccessKey)
	odpsIns := odps.NewOdps(aliAccount, conf.Endpoint)
	// Set the Default Maxcompute project used By Odps instance
	odpsIns.SetDefaultProjectName(conf.ProjectName)
	odpsIns.SetCurrentSchemaName("default")

	ins1, err := odpsIns.ExecSQl("select 1;")
	if err != nil {
		log.Fatalf("%+v", err)
	}
	err = ins1.WaitForSuccess()
	if err != nil {
		log.Fatalf("%+v", err)
	}
	println(ins1.Id())

	instanceOption := options.NewCreateInstanceOptions()
	instanceOption.UniqueIdentifyID = "123456"

	option := options.NewSQLTaskOptions()
	option.InstanceOption = instanceOption

	ins2, err := odpsIns.ExecSQlWithOption("select 1;", option)
	if err != nil {
		log.Fatalf("%+v", err)
	}
	println(ins2.Id())

	ins3, err := odpsIns.ExecSQlWithOption("select 2;", option)
	if err != nil {
		log.Fatalf("%+v", err)
	}
	println(ins3.Id())

	println(ins2.Id() == ins3.Id())
}
