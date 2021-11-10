package odps_test

import (
	"fmt"
	odps "github.com/aliyun/aliyun-odps-go-sdk"
)

func ExampleTables_List() {
	tables := odps.NewTables(odpsIns)
	c := make(chan odps.Table)

	go func() {
		err := tables.List(c, true, "", "")
		if err != nil {
			println(err.Error())
		}
	}()

	for t := range c {
		println(fmt.Sprintf("%s, %s, %s", t.Name(), t.Owner(), t.Type()))
	}

	// Output:
}

func ExampleTables_BatchLoadTables() {
	tablesIns := odps.NewTables(odpsIns)
	tableNames := []string {
		"jet_mr_input",
		"jet_smode_test",
		"odps_smoke_table",
		"user",
	}

	tables, err := tablesIns.BatchLoadTables(tableNames)
	if err != nil {
		println(err.Error())
	} else {
		for _, table := range tables {
			println(fmt.Sprintf("%s, %s, %s", table.Name(), table.TableID(), table.Type()))
		}

		schema, err := tables[len(tables) - 1].GetSchema()
		if err != nil {
			println(err.Error())
		} else {
			for _, c := range schema.Columns {
				println(fmt.Sprintf("%s, %s, %t, %s",  c.Name, c.Type, c.IsNullable, c.Comment))
			}
		}
	}

	// Output:
}