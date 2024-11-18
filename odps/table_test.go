package odps

import (
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
)

func TestTable_AddColumns(t *testing.T) {
	table := NewTable(nil, "project", "schema", "table")

	c1 := tableschema.Column{
		Name:    "c1",
		Type:    datatype.BigIntType,
		Comment: "c1 is bigint",
	}

	c2 := tableschema.Column{
		Name:    "c2",
		Type:    datatype.StringType,
		Comment: "c2 is string",
	}

	c3 := tableschema.Column{
		Name: "c3",
		Type: datatype.DecimalType{
			Precision: 18,
			Scale:     8,
		},
		Comment: "c3 is decimal(18,8)",
	}

	c4 := tableschema.Column{
		Name: "c4",
		Type: datatype.VarcharType{
			Length: 100,
		},
		Comment: "c4 is varchar(100)",
	}

	c5 := tableschema.Column{
		Name:    "c5",
		Type:    datatype.DateTimeType,
		Comment: "c5 is datetime",
	}

	c6 := tableschema.Column{
		Name: "c6",
		Type: datatype.MapType{
			KeyType:   datatype.StringType,
			ValueType: datatype.BigIntType,
		},
		Comment: "c6 is map(string,bigint)",
	}

	newColumns := []tableschema.Column{c1, c2, c3, c4, c5, c6}

	actualSQL := table.generateAddColumnsSQL(newColumns, true)
	expectedSQL := "alter table project.schema.table add columns if not exists (c1 BIGINT comment 'c1 is bigint', c2 STRING comment 'c2 is string', c3 DECIMAL(18,8) comment 'c3 is decimal(18,8)', c4 VARCHAR(100) comment 'c4 is varchar(100)', c5 DATETIME comment 'c5 is datetime', c6 MAP<STRING,BIGINT> comment 'c6 is map(string,bigint)');"
	if actualSQL != expectedSQL {
		t.Errorf("Expected SQL: %s, but got: %s", expectedSQL, actualSQL)
	}
}

func TestTable_Rename(t *testing.T) {
	table := NewTable(nil, "project", "schema", "table")
	actualSQL := table.generateRenameTableSQL("project_new")
	expectedSQL := "alter table project.schema.table rename to 'project_new';"

	if actualSQL != expectedSQL {
		t.Errorf("Expected SQL: %s, but got: %s", expectedSQL, actualSQL)
	}
}

func TestTable_DropColumns(t *testing.T) {
	table := NewTable(nil, "project", "schema", "table")
	actualSQL := table.generateDropColumnsSQL([]string{"c1", "c2", "c3", "c4"})
	expectedSQL := "alter table project.schema.table drop columns c1, c2, c3, c4;"

	if actualSQL != expectedSQL {
		t.Errorf("Expected SQL: %s, but got: %s", expectedSQL, actualSQL)
	}
}

func TestTable_ChangeOwner(t *testing.T) {
	table := NewTable(nil, "project", "schema", "table")

	expectedSQL := "alter table project.schema.table changeowner to 'ALIYUN$xxx@aliyun.com';"
	actualSQL := table.generateChangeOwnerSQL("ALIYUN$xxx@aliyun.com")

	if actualSQL != expectedSQL {
		t.Errorf("Expected SQL: %s, but got: %s", expectedSQL, actualSQL)
	}
}

func TestTable_ChangeComment(t *testing.T) {
	table := NewTable(nil, "project", "schema", "table")

	expectedSQL := "alter table project.schema.table set comment 'This\\'s comment.';"
	actualSQL := table.generateChangeCommentSQL("This's comment.")

	if actualSQL != expectedSQL {
		t.Errorf("Expected SQL: %s, but got: %s", expectedSQL, actualSQL)
	}
}

func TestTable_Touch(t *testing.T) {
	table := NewTable(nil, "project", "schema", "table")

	expectedSQL := "alter table project.schema.table touch;"
	actualSQL := table.generateTouchTableSQL()

	if actualSQL != expectedSQL {
		t.Errorf("Expected SQL: %s, but got: %s", expectedSQL, actualSQL)
	}
}

// TestGenerateChangeClusterInfoSQL_Hash_NoSortNoBucket 测试 Hash 类型且没有排序列和桶的情况
func TestGenerateChangeClusterInfoSQL_Hash_NoSortNoBucket(t *testing.T) {
	// 准备
	table := NewTable(nil, "project", "schema", "table")
	clusterInfo := tableschema.ClusterInfo{
		ClusterType: tableschema.CLUSTER_TYPE.Hash,
		ClusterCols: []string{"col1"},
		SortCols:    nil,
		BucketNum:   0,
	}

	// 执行
	sql := table.generateChangeClusterInfoSQL(clusterInfo)

	// 验证
	expectedSQL := "alter table project.schema.table clustered by (col1);"
	if sql != expectedSQL {
		t.Errorf("Expected SQL: '%s', got: '%s'", expectedSQL, sql)
	}
}

// TestGenerateChangeClusterInfoSQL_Range_Sort_Bucket 测试 Range 类型且有排序列和桶的情况
func TestGenerateChangeClusterInfoSQL_Range_Sort_Bucket(t *testing.T) {
	// 准备
	table := NewTable(nil, "project", "schema", "table")
	clusterInfo := tableschema.ClusterInfo{
		ClusterType: tableschema.CLUSTER_TYPE.Range,
		ClusterCols: []string{"col1"},
		SortCols: []tableschema.SortColumn{
			{Name: "sort_col1", Order: tableschema.SORT_ORDER.ASC},
			{Name: "sort_col2", Order: tableschema.SORT_ORDER.DESC},
		},
		BucketNum: 10,
	}

	// 执行
	sql := table.generateChangeClusterInfoSQL(clusterInfo)

	// 验证
	expectedSQL := "alter table project.schema.table range clustered by (col1) sorted by (sort_col1 asc, sort_col2 desc) into 10 buckets;"
	if sql != expectedSQL {
		t.Errorf("Expected SQL: '%s', got: '%s'", expectedSQL, sql)
	}
}

// TestGenerateChangeClusterInfoSQL_Hash_Sort_Bucket 测试 Hash 类型且有排序列和桶的情况
func TestGenerateChangeClusterInfoSQL_Hash_Sort_Bucket(t *testing.T) {
	// 准备
	table := NewTable(nil, "project", "schema", "table")
	clusterInfo := tableschema.ClusterInfo{
		ClusterType: tableschema.CLUSTER_TYPE.Hash,
		ClusterCols: []string{"col1"},
		SortCols: []tableschema.SortColumn{
			{Name: "sort_col1", Order: tableschema.SORT_ORDER.ASC},
			{Name: "sort_col2", Order: tableschema.SORT_ORDER.DESC},
		},
		BucketNum: 10,
	}

	// 执行
	sql := table.generateChangeClusterInfoSQL(clusterInfo)

	// 验证
	expectedSQL := "alter table project.schema.table clustered by (col1) sorted by (sort_col1 asc, sort_col2 desc) into 10 buckets;"
	if sql != expectedSQL {
		t.Errorf("Expected SQL: '%s', got: '%s'", expectedSQL, sql)
	}
}

// TestGenerateChangeClusterInfoSQL_EmptySortCols 测试空排序列的情况
func TestGenerateChangeClusterInfoSQL_EmptySortCols(t *testing.T) {
	// 准备
	table := NewTable(nil, "project", "schema", "table")
	clusterInfo := tableschema.ClusterInfo{
		ClusterType: tableschema.CLUSTER_TYPE.Range,
		ClusterCols: []string{"col1"},
		SortCols:    nil,
		BucketNum:   10,
	}

	// 执行
	sql := table.generateChangeClusterInfoSQL(clusterInfo)

	// 验证
	expectedSQL := "alter table project.schema.table range clustered by (col1) into 10 buckets;"
	if sql != expectedSQL {
		t.Errorf("Expected SQL: '%s', got: '%s'", expectedSQL, sql)
	}
}

// TestGenerateChangeClusterInfoSQL_ZeroBucketNum 测试零桶数的情况
func TestGenerateChangeClusterInfoSQL_ZeroBucketNum(t *testing.T) {
	table := NewTable(nil, "project", "schema", "table")
	clusterInfo := tableschema.ClusterInfo{
		ClusterType: tableschema.CLUSTER_TYPE.Range,
		ClusterCols: []string{"col1"},
		SortCols: []tableschema.SortColumn{
			{Name: "sort_col1", Order: tableschema.SORT_ORDER.ASC},
			{Name: "sort_col2", Order: tableschema.SORT_ORDER.DESC},
		},
		BucketNum: 0,
	}

	// 执行
	sql := table.generateChangeClusterInfoSQL(clusterInfo)

	// 验证
	expectedSQL := "alter table project.schema.table range clustered by (col1) sorted by (sort_col1 asc, sort_col2 desc);"
	if sql != expectedSQL {
		t.Errorf("Expected SQL: '%s', got: '%s'", expectedSQL, sql)
	}
}
