package main

import (
	"fmt"
	"log"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
)

func main() {
	// Specify the ini file path
	configPath := "./config.ini"
	conf, err := odps.NewConfigFromIni(configPath)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	aliAccount := account.NewApsaraAccount(conf.AccessId, conf.AccessKey)
	odpsIns := odps.NewOdps(aliAccount, conf.Endpoint)
	// Set the Default Maxcompute project used By Odps instance
	odpsIns.SetDefaultProjectName(conf.ProjectName)

	sql := "select * from all_types_demo where p1 = 20 and p2 = 'hangzhou';"

	// The flags used by sql engine, such as odps.sql.skewjoin
	var hints map[string]string = nil

	sqlTask := odps.NewSqlTask("select_demo", sql, hints)

	// Run the sql with the quota associated with a project
	projectName := odpsIns.DefaultProjectName()

	ins, err := sqlTask.Run(odpsIns, projectName)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	err = ins.WaitForSuccess()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	// Generate the logView for job detail information
	lv := odpsIns.LogView()
	lvUrl, err := lv.GenerateLogView(ins, 10)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	println(lvUrl)

	project := odpsIns.DefaultProject()
	tunnelEndpoint, err := project.GetTunnelEndpoint()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	// Create a Tunnel instance
	tunnelIns := tunnel.NewTunnel(odpsIns, tunnelEndpoint)
	session, err := tunnelIns.CreateInstanceResultDownloadSession(project.Name(), ins.Id())
	if err != nil {
		log.Fatalf("%+v", err)
	}

	start := 0
	step := 200000
	recordCount := session.RecordCount()
	schema := session.Schema()
	total := 0

	// Iterate the reader step by step
	for start < recordCount {
		reader, err := session.OpenRecordReader(start, step, 0, nil)
		if err != nil {
			log.Fatalf("%+v", err)
		}

		count := 0
		err = reader.Iterator(func(record data.Record, _err error) {
			count += 1

			if _err != nil {
				return
			}

			for i, d := range record {
				if d == nil {
					fmt.Printf("%s=null", schema.Columns[i].Name)
				} else {
					fmt.Printf("%s=%s", schema.Columns[i].Name, d.Sql())
				}

				if i < record.Len()-1 {
					fmt.Printf(", ")
				} else {
					fmt.Println()
				}
			}
		})
		if err != nil {
			log.Fatalf("%+v", err)
		}

		start += count
		total += count
		log.Println(count)
		if err = reader.Close(); err != nil {
			log.Fatalf("%+v", err)
		}
	}

	println("total count ", total)
}
