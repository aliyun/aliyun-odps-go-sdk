package odps_test

import odps "github.com/aliyun/aliyun-odps-go-sdk"

func ExampleTableSchema_ToSQLString() {
	c1 := odps.Column{
		Name:    "name",
		Type:    odps.STRING,
		Comment: "name of user",
	}

	c2 := odps.Column{
		Name:    "age",
		Type:    odps.INT,
		Comment: "how old is the user",
	}

	p1 := odps.Column{
		Name:    "region",
		Type:    odps.STRING,
		Comment: "居住区域",
	}

	p2 := odps.Column{
		Name: "code",
		Type: odps.INT,
	}

	serdeProperties := make(map[string]string)
	serdeProperties["odps.sql.preparse.odps2"] = "lot"
	serdeProperties["odps.sql.planner.mode"] = "lot"
	serdeProperties["odps.sql.planner.parser.odps2"] = "true"
	serdeProperties["odps.sql.ddl.odps2"] = "true"
	serdeProperties["odps.compiler.output.format"] = "lot,pot"

	jars := []string{"odps-udf-example.jar", "another.jar"}

	builder := odps.NewTableSchemaBuilder()
	builder.Name("user").
		Comment("这就是一条注释").
		Columns(c1, c2).
		PartitionColumns(p1, p2).
		Lifecycle(2).
		StorageHandler("com.aliyun.odps.CsvStorageHandler").
		Location("MOCKoss://full/uri/path/to/oss/directory/")

	schema := builder.Build()

	sql, _ := schema.ToSQLString("project_1", true)
	println(sql)
	println("\n")

	externalSql, err := schema.ToExternalSQLString(
		"project_1",
		true,
		serdeProperties,
		jars,
	)

	if err != nil {
		println(err.Error())
	} else {
		println(externalSql)
	}
	// Output:
}


func ExampleTables_Create() {
	c1 := odps.Column{
		Name:    "name",
		Type:    odps.STRING,
		Comment: "name of user",
	}

	c2 := odps.Column{
		Name:    "age",
		Type:    odps.INT,
		Comment: "how old is the user",
	}

	p1 := odps.Column{
		Name:    "region",
		Type:    odps.STRING,
		Comment: "居住区域",
	}

	p2 := odps.Column{
		Name: "code",
		Type: odps.INT,
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