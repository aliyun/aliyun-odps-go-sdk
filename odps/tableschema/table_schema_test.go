package tableschema

import (
	"fmt"
	"testing"

	datatype2 "github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
)

func TestToViewSQLString(t *testing.T) {
	schema := TableSchema{
		TableName: "test",
		Columns: []Column{
			{
				Name:    "test1",
				Type:    datatype2.IntType,
				Comment: "test1",
			},
			{
				Name:    "test2",
				Type:    datatype2.BooleanType,
				Comment: "test2",
			},
		},
		ViewText:      "select * from test",
		Comment:       "view create test",
		IsVirtualView: true,
	}
	viewSQL, err := schema.ToViewSQLString("test", "", true, false, true)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	fmt.Println(viewSQL)
}

func TestVirtualViewTemplateParse(t *testing.T) {
	schema := TableSchema{
		TableName: "sale_detail_view",
		Columns: []Column{
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
		},
		ViewText:                         "select shop_name,customer_id from sale_detail",
		Comment:                          "virtual view create test",
		IsVirtualView:                    true,
		IsMaterializedViewRewriteEnabled: true,
	}
	viewSQL, err := schema.ToViewSQLString("", "", true, true, true)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	fmt.Println(viewSQL)

}

func TestMaterializedViewTemplateParse(t *testing.T) {
	schema := TableSchema{
		TableName: "mf_mv_blank_pts",
		Columns: []Column{
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
		},
		PartitionColumns: []Column{
			{
				Name:    "ds",
				Comment: "partitiion",
			},
		},
		IsMaterializedView:               true,
		IsMaterializedViewRewriteEnabled: true,
		ViewText:                         "select key,value,ds from mf_blank_pts",
		Lifecycle:                        7,
	}

	schema.MvProperties = map[string]string{
		"enable_auto_substitute":   "true",
		"enable_auto_refresh":      "true",
		"refresh_interval_minutes": "120",
	}

	schema.ClusterInfo = ClusterInfo{
		ClusterType: CLUSTER_TYPE.Hash,
		// ClusterCols: []string{"key"},
		SortCols: []SortColumn{
			{
				Name:  "value",
				Order: "asc",
			},

			// {
			// 	Name: "job",
			// 	Order: "desc",
			// },
		},
		BucketNum: 1024,
	}
	viewSQL, err := schema.ToViewSQLString("", "", false, true, false)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	fmt.Println(viewSQL)
}
