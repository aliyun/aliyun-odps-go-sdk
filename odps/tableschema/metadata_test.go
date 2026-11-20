// Licensed under the Apache License, Version 2.0.

package tableschema

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
)

func TestTableSchemaDecodesServiceMetadata(t *testing.T) {
	for _, tc := range []struct {
		name   string
		fields map[string]string
		want   bool
		err    string
	}{
		{"boolean flag", map[string]string{"reserved": `{"Transactional":true,"PrimaryKey":["id"],"BucketNum":4}`}, true, ""},
		{"string flag", map[string]string{"reserved": `{"Transactional":"TrUe","PrimaryKey":["id"],"BucketNum":4}`}, true, ""},
		{"omitted flag", map[string]string{"reserved": `{"PrimaryKey":["id"],"BucketNum":4}`}, false, ""},
		{"invalid flag", map[string]string{"reserved": `{"Transactional":1}`}, false, "expected boolean or string"},
		{"invalid reserved", map[string]string{"reserved": "{"}, false, "unexpected end"},
		{"invalid SerDe properties", map[string]string{"serDeProperties": "["}, false, "unexpected end"},
		{"invalid view properties", map[string]string{"mvProperties": "["}, false, "unexpected end"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fields := map[string]string{
				"serDeProperties": `{"separator":","}`, "mvProperties": `{"refresh":"manual"}`,
				"isMaterializedViewRewriteEnabled": "TRUE", "isMaterializedViewOutdated": "FALSE",
			}
			for k, v := range tc.fields {
				fields[k] = v
			}
			body, err := json.Marshal(fields)
			require.NoError(t, err)
			var schema TableSchema
			err = json.Unmarshal(body, &schema)
			if tc.err != "" {
				require.ErrorContains(t, err, tc.err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, schema.Transactional)
			require.Equal(t, []string{"id"}, schema.PrimaryKeys)
			require.Equal(t, 4, schema.ClusterInfo.BucketNum)
			require.Equal(t, ",", schema.SerDeProperties["separator"])
			require.Equal(t, "manual", schema.MvProperties["refresh"])
			require.True(t, schema.IsMaterializedViewRewriteEnabled)
			require.False(t, schema.IsMaterializedViewOutdated)
		})
	}
}

func TestColumnDecodesGeneratedPartition(t *testing.T) {
	for _, tc := range []struct {
		expr string
		want string
		err  string
	}{
		{`[{"leafExprDesc":{"reference":{"name":"event_time"}}},{"leafExprDesc":{"constant":"YEAR"}},{"functionCall":{"name":"TRUNC_TIME"}}]`, "year", ""},
		{`[{"leafExprDesc":{"reference":{"name":"event_time"}}},{"leafExprDesc":{"constant":"MONTH"}},{"functionCall":{"name":"trunc_time"}}]`, "month", ""},
		{`[{"leafExprDesc":{"reference":{"name":"event_time"}}},{"leafExprDesc":{"constant":"day"}},{"functionCall":{"name":"trunc_time"}}]`, "day", ""},
		{`[{"leafExprDesc":{"reference":{"name":"event_time"}}},{"leafExprDesc":{"constant":"hour"}},{"functionCall":{"name":"trunc_time"}}]`, "hour", ""},
		{"", "", ""},
		{"[", "", "unexpected end"},
		{"[]", "", "no expressions"},
		{`[{}]`, "", "unknown generate expression: null"},
		{`[{"functionCall":{"name":"custom"}}]`, "", "unknown generate expression: custom"},
		{`[{"functionCall":{"name":"trunc_time"}}]`, "", "requires a column and a date part"},
		{`[{},{},{"functionCall":{"name":"trunc_time"}}]`, "", "unknown date part"},
		{`[{},{"leafExprDesc":{"constant":"day"}},{"functionCall":{"name":"trunc_time"}}]`, "", "dateColumnName cannot be empty"},
	} {
		t.Run(fmt.Sprintf("expr=%q", tc.expr), func(t *testing.T) {
			raw, err := json.Marshal(map[string]any{
				"Name": "bucket", "Type": "string", "IsNullable": false, "Label": "S2",
				"HasGenerateExpression": true, "GenerateExpression": tc.expr,
			})
			require.NoError(t, err)
			var column Column
			err = json.Unmarshal(raw, &column)
			if tc.err != "" {
				require.ErrorContains(t, err, tc.err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, "bucket", column.Name)
			require.Equal(t, "S2", column.Label)
			require.True(t, column.NotNull)
			if tc.want == "" {
				require.Nil(t, column.GenerateExpression)
				return
			}
			require.Equal(t, "trunc_time(`event_time`, '"+tc.want+"')", column.GenerateExpression.String())
		})
	}
	var column Column
	require.Error(t, json.Unmarshal([]byte(`{"Type":"unknown type"}`), &column))
	require.Error(t, json.Unmarshal([]byte(`{"Type":17}`), &column))
}

func TestGeneratePartitionSpecWithNullableAndShortRows(t *testing.T) {
	columns := []Column{{Name: "id", Type: datatype.BigIntType}, {Name: "event", Type: datatype.TimestampNtzType}}
	expr := NewTruncTime("event", DAY)
	schema := NewSchemaBuilder().Columns(columns...).
		PartitionColumn(Column{Name: "static", Type: datatype.StringType}).
		PartitionColumn(Column{Name: "day", GenerateExpression: expr}).
		PartitionColumn(Column{Name: "year", GenerateExpression: NewTruncTime("event", YEAR)}).Build()
	for _, tc := range []struct {
		record data.Record
		want   string
		err    string
	}{
		{data.Record{data.BigInt(1), data.TimestampNtz(time.Unix(-1, 999000000).UTC())}, "day=1969-12-31/year=1969", ""},
		{data.Record{data.BigInt(1), data.Date(time.Date(2500, 1, 1, 0, 0, 0, 0, time.UTC))}, "day=2500-01-01/year=2500", ""},
		{data.Record{data.BigInt(1), data.Null}, "day=__NULL__/year=__NULL__", ""},
		{data.Record{data.BigInt(1), data.String("wrong")}, "", "unknown data type"},
		{data.Record{data.BigInt(1)}, "", "missing column"},
	} {
		actual, err := schema.GeneratePartitionSpec(&tc.record)
		if tc.err != "" {
			require.ErrorContains(t, err, tc.err)
		} else {
			require.NoError(t, err)
		}
		require.Equal(t, tc.want, actual)
	}
	_, err := schema.GeneratePartitionSpec(nil)
	require.ErrorContains(t, err, "missing record")

	reordered := schema
	reordered.Columns = []Column{columns[1], columns[0]}
	record := data.Record{data.TimestampNtz(time.Unix(0, 0)), data.BigInt(1)}
	got, err := reordered.GeneratePartitionSpec(&record)
	require.NoError(t, err)
	require.Equal(t, "day=1970-01-01/year=1970", got)

	disconnected := schema
	disconnected.Columns = columns[:1]
	got, err = disconnected.GeneratePartitionSpec(&record)
	require.NoError(t, err)
	require.Equal(t, "day=__NULL__/year=__NULL__", got)
}

func TestDDLPropertiesAndInvalidSpecifications(t *testing.T) {
	for _, cluster := range []ClusterType{CLUSTER_TYPE.Hash, CLUSTER_TYPE.Range} {
		schema := NewSchemaBuilder().Name("events").
			Column(Column{Name: "id", Type: datatype.BigIntType, NotNull: true}).
			Column(Column{Name: "label", Type: datatype.StringType}).
			PrimaryKeys([]string{"id", "label"}).
			ClusterType(cluster).ClusterColumns([]string{"id", "label"}).
			ClusterSortColumns([]SortColumn{{Name: "id", Order: SORT_ORDER.ASC}, {Name: "label", Order: SORT_ORDER.DESC}}).
			ClusterBucketNum(8).Lifecycle(30).TblProperties(map[string]string{"level": "one", "source": "fixture"}).Build()
		sql, err := schema.ToSQLString("project", "analytics", true)
		require.NoError(t, err)
		require.Contains(t, sql, "project.`analytics`.`events`")
		require.Contains(t, strings.Join(strings.Fields(sql), " "), "primary key( `id`, `label`)")
		require.Contains(t, sql, "clustered by (`id`, `label`)")
		require.Equal(t, cluster == CLUSTER_TYPE.Range, strings.Contains(sql, "range clustered by"))
		require.Contains(t, sql, "sorted by (`id` asc, `label` desc)")
		require.Contains(t, sql, "INTO 8 BUCKETS")
		require.Contains(t, sql, "'level'='one'")
		require.Contains(t, sql, "'source'='fixture'")
		require.Contains(t, sql, "\nlifecycle 30;")
	}

	for _, tc := range []struct {
		schema TableSchema
		err    string
	}{
		{TableSchema{}, "table name"},
		{TableSchema{TableName: "empty"}, "table columns"},
		{TableSchema{TableName: "malformed", Columns: []Column{{Name: "missing_type"}}}, "render table DDL"},
	} {
		_, err := tc.schema.ToSQLString("project", "", false)
		require.ErrorContains(t, err, tc.err)
	}
	for _, tc := range []struct {
		schema TableSchema
		err    string
	}{
		{TableSchema{}, "view name"},
		{TableSchema{TableName: "v", IsVirtualView: true}, "view text"},
		{TableSchema{TableName: "v", IsVirtualView: true, ViewText: "select 1", PartitionColumns: []Column{{Name: "ds"}}}, "can not have partition columns"},
		{TableSchema{TableName: "v", IsVirtualView: true, IsMaterializedView: true, ViewText: "select 1"}, "can not be both set"},
		{TableSchema{TableName: "v"}, "either virtual view or materialized"},
	} {
		_, err := tc.schema.ToViewSQLString("project", "", false, false, false)
		require.ErrorContains(t, err, tc.err)
	}
}
