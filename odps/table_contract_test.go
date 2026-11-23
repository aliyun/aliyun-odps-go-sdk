// Licensed under the Apache License, Version 2.0.

package odps_test

import (
	"encoding/xml"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
)

func TestTableMetadataLabelsAndMaskPolicies(t *testing.T) {
	masked := `{"columnMaskInfoList":[{"Name":"sensitive","PolicyNameList":["redact","hash"]}]}`
	for _, tc := range []struct {
		name     string
		schema   string
		masks    string
		maxLabel string
		maxEx    string
	}{
		{"lower labels do not change max", `{"TableLabel":"S4","columns":[{"Name":"sensitive","Type":"string","Label":"B2"}],"ExtendedLabel":["c4","B1"]}`, masked, "s4", "c4"},
		{"multiple categories at max", `{"TableLabel":"1","columns":[{"Name":"sensitive","Type":"string","Label":"B3"}],"ExtendedLabel":["C3","C1","", "invalid"]}`, "{", "b3", "L3"},
		{"absent mask and label", `{"TableLabel":"0","columns":[{"Name":"id","Type":"bigint"}]}`, "", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service := metadataFixture(t, func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "/api/projects/fixture/tables/report", r.URL.Path)
				assert.Equal(t, "default", r.URL.Query().Get("curr_schema"))
				response := struct {
					XMLName       xml.Name `xml:"Table"`
					Name          string
					Project       string
					Owner         string
					TableID       string `xml:"TableId"`
					CryptoAlgo    string
					Comment       string
					Schema        string
					TableMaskInfo string
					Type          string
				}{
					Name: "report", Project: "fixture", Owner: "fixture-owner", Comment: "from metadata",
					Type: "MANAGED_TABLE", TableID: "table-123", CryptoAlgo: "AES256", Schema: tc.schema, TableMaskInfo: tc.masks,
				}
				assert.NoError(t, xml.NewEncoder(w).Encode(response))
			})
			service.SetCurrentSchemaName("default")
			table := service.Table("report")
			require.NoError(t, table.Load())
			require.Equal(t, "table-123", table.TableID())
			require.Equal(t, "AES256", table.CryptoAlgo())
			require.Equal(t, "fixture-owner", table.Owner())
			require.Equal(t, "from metadata", table.Comment())
			require.Equal(t, tc.maxLabel, table.MaxLabel())
			require.Equal(t, tc.maxEx, table.MaxExtendedLabel())
			masks, err := table.ColumnMaskInfos()
			switch tc.masks {
			case "":
				require.NoError(t, err)
				require.Empty(t, masks)
			case "{":
				require.ErrorContains(t, err, "unexpected end")
			default:
				require.NoError(t, err)
				require.Equal(t, []odps.ColumnMaskInfo{{Name: "sensitive", PolicyNameList: []string{"redact", "hash"}}}, masks)
				second, err := table.ColumnMaskInfos()
				require.NoError(t, err)
				require.Equal(t, masks, second)
			}
		})
	}
}

func TestPartitionMetadataPagination(t *testing.T) {
	service := metadataFixture(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		q := r.URL.Query()
		assert.True(t, q.Has("partitions"))
		assert.Equal(t, "true", q.Get("expectmarker"))
		marker := q.Get("marker")
		if r.URL.Path == "/api/projects/fixture/tables/missing_page" && marker == "next" {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		var item string
		if q.Has("name") {
			if marker == "" {
				item = `<Marker>next</Marker><Partition><Name>ds=20261119</Name></Partition>`
			} else {
				assert.Equal(t, "next", marker)
				item = `<Partition><Name>ds=20261120</Name></Partition>`
			}
		} else if marker == "" {
			item = `<Marker>next</Marker><Partition><Column Name="ds" Value="20261119"/><PartitionRecordCount>7</PartitionRecordCount></Partition>`
		} else {
			assert.Equal(t, "next", marker)
			item = `<Partition><Column Name="ds" Value="20261120"/><PartitionSize>19</PartitionSize></Partition>`
		}
		_, _ = io.WriteString(w, "<Partitions>"+item+"</Partitions>")
	})
	table := service.Table("fixture_partitions")
	values, err := table.GetPartitionValues()
	require.NoError(t, err)
	require.Equal(t, []string{"ds=20261119", "ds=20261120"}, values)
	partitions, err := table.GetPartitions()
	require.NoError(t, err)
	require.Len(t, partitions, 2)
	require.Equal(t, "ds=20261119", partitions[0].Value())
	require.Equal(t, 7, partitions[0].RecordNum())
	require.Equal(t, "ds=20261120", partitions[1].Value())
	require.Equal(t, 19, partitions[1].Size())

	missing := service.Table("missing_page")
	values, err = missing.GetPartitionValues()
	require.Error(t, err)
	require.Nil(t, values)
	partitions, err = missing.GetPartitions()
	require.Error(t, err)
	require.Len(t, partitions, 1)
	require.Equal(t, "ds=20261119", partitions[0].Value())
}

func TestTableTypeXMLRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		value string
		typ   odps.TableType
	}{
		{"MANAGED_TABLE", odps.ManagedTable},
		{"VIRTUAL_VIEW", odps.VirtualView},
		{"EXTERNAL_TABLE", odps.ExternalTable},
		{"MATERIALIZED_VIEW", odps.MaterializedView},
		{"OBJECT_TABLE", odps.ObjectTable},
		{"Unknown", odps.TableTypeUnknown},
	} {
		var decoded struct{ Type odps.TableType }
		require.NoError(t, xml.Unmarshal([]byte("<Table><Type>"+tc.value+"</Type></Table>"), &decoded))
		require.Equal(t, tc.typ, decoded.Type)
		encoded, err := xml.Marshal(struct {
			XMLName xml.Name `xml:"Table"`
			Type    odps.TableType
		}{Type: decoded.Type})
		require.NoError(t, err)
		require.Equal(t, "<Table><Type>"+tc.value+"</Type></Table>", string(encoded))
		require.Error(t, xml.Unmarshal([]byte("<Table><Type>"+tc.value), &decoded))
	}
}
