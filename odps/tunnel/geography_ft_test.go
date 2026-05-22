// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tunnel_test

import (
	"errors"
	"io"
	"os"
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
)

// TestGeographyEndToEnd 真实环境往返：
//   1. 建表（带 GEOGRAPHY 列）
//   2. SQL INSERT 写入 ST_GEOGFROMTEXT 表达式得到的几何数据
//   3. 通过 tunnel CreateDownloadSession 直接从表下载（protobuf）
//   4. 校验列类型为 data.Geography 且 WKB 字节非空
//
// 注：MaxCompute 服务端 instance-result tunnel 暂不支持 GEOGRAPHY 列，必须走表下载。
// 镜像 Java SDK GeographyTest.testInstanceTunnel.
func TestGeographyEndToEnd(t *testing.T) {
	if os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET") == "" {
		t.Skip("ALIBABA_CLOUD_ACCESS_KEY_SECRET not set, skipping FT")
	}

	tableName := "odps_sdk_geography_test_go"
	hints := map[string]string{"odps.sql.type.system.odps2": "true"}

	tables := odpsIns.Tables()
	_ = tables.Delete(tableName, true)

	schema := tableschema.NewSchemaBuilder().
		Name(tableName).
		Columns(
			tableschema.Column{Name: "name", Type: datatype.StringType},
			tableschema.Column{Name: "geo", Type: datatype.GeographyType},
		).
		Lifecycle(1).
		Build()

	if err := tables.Create(schema, true, hints, nil); err != nil {
		t.Fatalf("create table: %+v", err)
	}
	t.Cleanup(func() { _ = tables.Delete(tableName, true) })

	insertSQL := "INSERT INTO " + tableName + " VALUES " +
		"('Point', ST_GEOGFROMTEXT('POINT (10 20)'))," +
		"('Line', ST_GEOGFROMTEXT('LINESTRING (10 20, 30 50, 70 80)'))," +
		"('Polygon', ST_GEOGFROMTEXT('POLYGON ((0 0, 1 1, 1 0, 0 0))'))," +
		"('Multipoint', ST_GEOGFROMTEXT('MULTIPOINT(0 0, 1 1)'))," +
		"('MultiLinestring', ST_GEOGFROMTEXT('MULTILINESTRING ((0 0, 1 1), (2 2, 3 3))'));"

	ins, err := odpsIns.ExecSQl(insertSQL, hints)
	if err != nil {
		t.Fatalf("insert exec: %+v", err)
	}
	if err := ins.WaitForSuccess(); err != nil {
		t.Fatalf("insert wait: %+v", err)
	}

	session, err := tunnelIns.CreateDownloadSession(ProjectName, tableName)
	if err != nil {
		t.Fatalf("create download session: %+v", err)
	}

	rowCount := session.RecordCount()
	t.Logf("download session RecordCount=%d", rowCount)
	if rowCount == 0 {
		t.Fatal("expected non-zero RecordCount from table download session")
	}

	reader, err := session.OpenRecordReader(0, rowCount, nil)
	if err != nil {
		t.Fatalf("open reader: %+v", err)
	}
	defer reader.Close()

	seen := 0
	for {
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("read row %d: %+v", seen, err)
		}

		nameCol := record.Get(0)
		if _, ok := nameCol.(data.String); !ok {
			t.Fatalf("row %d: name column type = %T, want data.String", seen, nameCol)
		}

		geoCol := record.Get(1)
		geo, ok := geoCol.(data.Geography)
		if !ok {
			t.Fatalf("row %d: geo column type = %T, want data.Geography", seen, geoCol)
		}
		if len(geo.AsBinary()) == 0 {
			t.Fatalf("row %d (%v): geo WKB is empty", seen, nameCol)
		}
		t.Logf("row %d: name=%v, geo=%d bytes (%X)", seen, nameCol, len(geo), geo.AsBinary())
		seen++
	}

	if seen != int(rowCount) {
		t.Fatalf("read %d rows, want %d", seen, rowCount)
	}
}
