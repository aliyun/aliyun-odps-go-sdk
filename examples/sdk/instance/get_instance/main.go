package main

import (
	"fmt"
	"log"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
)

func main() {
	// Specify the ini file path
	configPath := "./config.ini"
	conf, err := odps.NewConfigFromIni(configPath)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	aliAccount := account.NewAliyunAccount(conf.AccessId, conf.AccessKey)
	odpsIns := odps.NewOdps(aliAccount, conf.Endpoint)
	// Set the Default Maxcompute project used By Odps instance
	odpsIns.SetDefaultProjectName(conf.ProjectName)

	ins := odpsIns.Instances().Get("2024101103485529ghlbgu6i4gg")
	err = ins.Load()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	fmt.Printf("owner=%s\n", ins.Owner())
	fmt.Printf("status=%s\n", ins.Status())
	fmt.Printf("startTime=%s\n", ins.StartTime())
	fmt.Printf("endTime=%s\n", ins.EndTime())
	fmt.Printf("result=%+v\n", ins.TaskResults())
}
