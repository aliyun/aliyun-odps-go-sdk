package odps_test

import (
	"fmt"
	odps "github.com/aliyun/aliyun-odps-go-sdk"
	"github.com/aliyun/aliyun-odps-go-sdk/datatype"
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


func ExampleTables_Create() {
	c1 := odps.Column{
		Name:    "name",
		Type:    datatype.NewStringType(),
		Comment: "name of user",
	}

	c2 := odps.Column{
		Name:    "age",
		Type:    datatype.NewIntType(),
		Comment: "how old is the user",
	}

	p1 := odps.Column{
		Name:    "region",
		Type:    datatype.NewStringType(),
		Comment: "居住区域",
	}

	p2 := odps.Column{
		Name: "code",
		Type: datatype.NewIntType(),
	}

	hints := make(map[string]string)
	hints["odps.sql.preparse.odps2"] = "lot"
	hints["odps.sql.planner.mode"] = "lot"
	hints["odps.sql.planner.parser.odps2"] = "true"
	hints["odps.sql.ddl.odps2"] = "true"
	hints["odps.compiler.output.format"] = "lot,pot"

	builder := odps.NewTableSchemaBuilder()
	builder.Name("user").
		Comment("这就是一条注释").
		Columns(c1, c2).
		PartitionColumns(p1, p2).
		Lifecycle(2)

	schema := builder.Build()
	sql, _ := schema.ToSQLString("project_1", false)
	println(sql)
	tables := odps.NewTables(odpsIns, "project_1")
	instance, err := tables.Create(schema, true, hints, nil)
	if err != nil {
		println(err.Error())
	} else {
		err := instance.WaitForSuccess()
		if err != nil {
			println(err.Error())
		}
	}

	// Output:
}

func ExampleTables_DeleteAndWait() {
	//ExampleTables_Create()
	tables := odps.NewTables(odpsIns, "project_1")
	err := tables.DeleteAndWait("user1", false)
	if err != nil {
		println(err.Error())
	}

	// Output:
}