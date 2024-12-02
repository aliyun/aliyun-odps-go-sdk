package tableschema

import (
	"fmt"
	"log"
	"testing"

	datatype2 "github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
)

func TestTable_CreateNormal(t *testing.T) {
	tableSchema := NewSchemaBuilder().Name("newTable").
		Column(Column{
			Name:    "col1",
			Type:    datatype2.BigIntType,
			Comment: "I'm col1",
		}).
		Column(Column{
			Name:    "col2",
			Type:    datatype2.BigIntType,
			Comment: "I'm col2",
		}).
		Comment("This's table comment").
		PartitionColumn(Column{
			Name:    "p1",
			Type:    datatype2.StringType,
			Comment: "I'm p1",
		}).
		TblProperties(map[string]string{"transactional": "false"}).Lifecycle(10).Build()

	sqlString, err := tableSchema.ToSQLString("project", "schema", true)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	expect := "create table if not exists project.`schema`.`newTable` (\n    `col1` BIGINT comment 'I\\'m col1',\n    `col2` BIGINT comment 'I\\'m col2'\n)\ncomment 'This\\'s table comment'\npartitioned by (`p1` STRING comment 'I\\'m p1')\nTBLPROPERTIES ('transactional'='false')\nlifecycle 10;"
	if sqlString != expect {
		t.Errorf("Expected SQL:\n %s, but got:\n %s", expect, sqlString)
	}
}

func TestTable_CreateExternal(t *testing.T) {
	tableSchema := NewSchemaBuilder().Name("newTable").
		Column(Column{
			Name:    "col1",
			Type:    datatype2.BigIntType,
			Comment: "I'm col1",
		}).
		Column(Column{
			Name:    "col2",
			Type:    datatype2.BigIntType,
			Comment: "I'm col2",
		}).
		Comment("This's table comment").
		PartitionColumn(Column{
			Name:    "p1",
			Type:    datatype2.StringType,
			Comment: "I'm p1",
		}).
		TblProperties(map[string]string{"transactional": "false"}).Lifecycle(10).
		Location("MOCKoss://full/uri/path/to/oss/directory/").
		StorageHandler("com.aliyun.odps.udf.example.text.TextStorageHandler").
		Lifecycle(10).
		Build()

	// 定义 properties 映射
	serDeProperties := map[string]string{
		"odps.text.option.delimiter": "|",
		"my.own.option":              "value",
	}
	jars := []string{
		"odps-udf-example.jar",
		"another.jar",
	}

	sqlString, err := tableSchema.ToExternalSQLString("project", "schema", true, serDeProperties, jars)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	log.Print(sqlString)
}

func TestTable_CreateAutoPartition(t *testing.T) {
	tableSchema := NewSchemaBuilder().Name("newTable").
		Column(Column{
			Name:    "col1",
			Type:    datatype2.TimestampType,
			Comment: "I'm col1",
		}).
		Column(Column{
			Name:    "col2",
			Type:    datatype2.BigIntType,
			Comment: "I'm col2",
		}).
		Comment("This's table comment").
		PartitionColumn(Column{
			Name:               "p1",
			Type:               datatype2.StringType,
			GenerateExpression: NewTruncTime("col1", DAY),
		}).
		Lifecycle(10).Build()

	sqlString, err := tableSchema.ToSQLString("project", "schema", true)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	println(sqlString)
	// create table if not exists project.`schema`.`newTable` (
	//    `col1` TIMESTAMP comment 'I\'m col1',
	//    `col2` BIGINT comment 'I\'m col2'
	//)
	//comment 'This\'s table comment'
	//auto partitioned by (trunc_time(col1, 'day') AS p1)
	//lifecycle 10;
}

func TestToViewSQLString(t *testing.T) {
	sb := NewSchemaBuilder()
	columns := []Column{
		{
			Name: "test1",
			Type: datatype2.IntType,
		},
		{
			Name: "test2",
			Type: datatype2.BooleanType,
		},
	}

	schema := sb.Name("test").
		Columns(columns...).
		ViewText("select * from test").
		Comment("view create test").
		IsVirtualView(true).
		Build()

	viewSQL, err := schema.ToViewSQLString("test", "", true, false, true)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	fmt.Println(viewSQL)
}

func TestVirtualViewTemplateParse(t *testing.T) {
	columns := []Column{
		{
			Name:    "shop_name",
			Type:    datatype2.StringType,
			Comment: "shop_name",
		},
		{
			Name:    "customer_id",
			Type:    datatype2.StringType,
			Comment: "",
		},
	}
	sb := NewSchemaBuilder()

	schema := sb.Name("sale_detail_view").
		Columns(columns...).
		ViewText("select shop_name,customer_id from sale_detail").
		Comment("virtual view create test").
		IsVirtualView(true).
		Build()

	viewSQL, err := schema.ToViewSQLString("", "", true, true, false)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	fmt.Println(viewSQL)
}

func TestMaterializedViewTemplateParse(t *testing.T) {
	columns := []Column{
		{
			Name:    "key",
			Type:    datatype2.BigIntType,
			Comment: "unique id",
		},
		{
			Name:    "value",
			Type:    datatype2.BigIntType,
			Comment: "input value",
		},
		{
			Name:    "ds",
			Type:    datatype2.StringType,
			Comment: "partition",
		},
	}
	partitionColumns := []Column{
		{
			Name:    "ds",
			Type:    datatype2.StringType,
			Comment: "partition",
		},
	}

	mvProperties := map[string]string{
		"enable_auto_substitute":   "true",
		"enable_auto_refresh":      "true",
		"refresh_interval_minutes": "120",
	}

	sb := NewSchemaBuilder()

	schema := sb.Name("mf_mv_blank_pts").
		Columns(columns...).
		IsMaterializedView(true).
		ViewText("select key,value,ds from mf_blank_pts").
		PartitionColumns(partitionColumns...).
		MvProperties(mvProperties).
		ClusterType(CLUSTER_TYPE.Hash).
		ClusterColumns([]string{"value"}).
		ClusterSortColumns([]SortColumn{{Name: "value", Order: "asc"}}).
		ClusterBucketNum(1024).
		Lifecycle(7).
		Build()

	viewSQL, err := schema.ToViewSQLString("", "", false, true, false)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	fmt.Println(viewSQL)
}
