package main

import (
	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"log"
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

	project := odpsIns.Project(conf.ProjectName)
	ts := project.Tables()

	ts.List(
		func(t *odps.Table, err error) {
			if err != nil {
				log.Fatalf("%+v", err)
			}

			println(t.Name())
		},
		// Filter tables by name prefix
		odps.TableFilter.NamePrefix("test_cluster"),
		// Filter tables by table type. Other table types are: VirtualView, ExternalTable
		odps.TableFilter.Type(odps.ManagedTable),
	)
}
