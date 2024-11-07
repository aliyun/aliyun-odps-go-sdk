package tableschema

import (
	"fmt"
	"testing"

	datatype2 "github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
)

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
