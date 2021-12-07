package odps_test

import (
	"fmt"
	odps "github.com/aliyun/aliyun-odps-go-sdk"
	"github.com/aliyun/aliyun-odps-go-sdk/datatype"
	"io"
	"log"
)

func ExampleTableSchema_ToSQLString() {
	c1 := odps.Column{
		Name:    "name",
		Type:    datatype.StringType,
		Comment: "name of user",
	}

	c2 := odps.Column{
		Name:    "age",
		Type:    datatype.IntType,
		Comment: "how old is the user",
	}

	p1 := odps.Column{
		Name:    "region",
		Type:    datatype.StringType,
		Comment: "居住区域",
	}

	p2 := odps.Column{
		Name: "code",
		Type: datatype.IntType,
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

func ExampleTable_Load() {
	table := odps.NewTable(odpsIns, "project_1", "has_struct")
	err := table.Load()
	if err != nil {
		println(err.Error())
	} else {
		schema := table.Schema()
		println(fmt.Sprintf("%+v", schema.Columns))
	}

	// Output:
}

func ExampleTable_AddPartition() {
	table := odps.NewTable(odpsIns, "project_1", "sale_detail")
	instance, err := table.AddPartition(true, "sale_date='202111',region='hangzhou'")
	if err != nil {
		println(err.Error())
	}

	err = instance.WaitForSuccess()
	if err != nil {
		println(err.Error())
	}

	results := instance.TaskResults()
	for _, result := range results {
		println(fmt.Sprintf("%+v", result))
	}

	// Output:
}

func ExampleTable_GetPartitions() {
	table := odps.NewTable(odpsIns, "project_1", "sale_detail")
	c := make(chan odps.Partition)

	go func() {
		err := table.GetPartitions(c, "")

		if err != nil {
			println(err.Error())
		}
	}()

	for p := range c {
		println(fmt.Sprintf("Name: %s", p.Name()))
		println(fmt.Sprintf("Create time: %s", p.CreatedTime()))
		println(fmt.Sprintf("Last DDL time: %s", p.LastDDLTime()))
		println(fmt.Sprintf("Last Modified time: %s", p.LastModifiedTime()))
		println("")
	}

	// Output:
}

func ExampleTable_ExecSql() {
	//table := odps.NewTable(odpsIns, "project_1", "sale_detail")
	table := odps.NewTable(odpsIns, "project_1", "has_struct")
	//instance, err := table.ExecSql("SelectSale_detail", "select * from sale_detail;")
	instance, err := table.ExecSql("Select_has_struct", "select * from has_struct;")
	if err != nil {
		println(err.Error())
	} else {
		err = instance.WaitForSuccess()
		if err != nil {
			println(err.Error())
		}

		results, err := instance.GetResult()
		if err != nil {
			println(err.Error())
		} else if len(results) > 0 {
			println(fmt.Sprintf("%+v", results[0]))
		}
	}

	// Output:
}

func ExampleTable_Read() {
	table := odps.NewTable(odpsIns, "project_1", "sale_detail")
	columns := []string {
		"shop_name", "customer_id", "total_price", "sale_date", "region",
	}
	reader, err := table.Read("", columns, -1, "")
	if err != nil {
		println(err.Error())
	} else {
		for {
			record, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatal(err)
			}

			println(fmt.Sprintf("%+v", record))
		}
	}

	// Output:
}