package main

import (
	"fmt"
	"log"
	"time"

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

	timeFormat := "2006-01-02 15:04:05"
	startTime, _ := time.Parse(timeFormat, "2024-10-11 02:15:30")
	endTime, _ := time.Parse(timeFormat, "2024-10-13 06:22:02")

	var f = func(i *odps.Instance) {
		if err != nil {
			log.Fatalf("%+v", err)
		}

		println(
			fmt.Sprintf(
				"%s, %s, %s, %s, %s",
				i.Id(), i.Owner(), i.StartTime().Format(timeFormat), i.EndTime().Format(timeFormat), i.Status(),
			))
	}

	instances := odpsIns.Instances()
	instances.List(
		f,
		odps.InstanceFilter.TimeRange(startTime, endTime),
		odps.InstanceFilter.Status(odps.InstanceTerminated),
	)
}
