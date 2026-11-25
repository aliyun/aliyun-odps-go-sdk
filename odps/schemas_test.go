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

package odps_test

import (
	"fmt"
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/internal/livetest"
	"github.com/aliyun/aliyun-odps-go-sdk/odps"
)

func TestSchemas_List(t *testing.T) {
	requireLiveService(t)
	schemas := odps.NewSchemas(odpsIns, defaultProjectName)
	schemas.List(func(schema *odps.Schema, err error) {
		print(schema.Name() + "\n")
	})
}

func TestSchemas_ListWithFilter(t *testing.T) {
	requireLiveService(t)
	schemas := odps.NewSchemas(odpsIns, defaultProjectName)

	// The filter has to point at this run's schemas. Unscoped, that is the
	// historical prefix the shared fixtures were filed under; scoped, it is the
	// namespace, so the callback has something of ours to look at either way.
	prefix := livetest.RunID()
	if prefix == "" {
		prefix = "new"
	}
	schemas.ListWithFilter(func(schema *odps.Schema, err error) {
		if err != nil {
			t.Logf("Error (may be expected): %+v", err)
			return
		}
		t.Logf("Schema name: %s", schema.Name())
	}, odps.SchemaFilter.NamePrefix(prefix))
}

func TestSchemas_GetSchema(t *testing.T) {
	requireLiveService(t)
	schema := odps.NewSchema(odpsIns, defaultProjectName, livetest.Fixture("exist_schema"))
	err := schema.Load()
	if err != nil {
		t.Fatalf("%+v", err)
	}
	print(schema.ModifiedTime().String())
}

func TestSchemas_ListTableBySchema(te *testing.T) {
	requireLiveService(te)
	ts := odps.NewTables(odpsIns, defaultProjectName, livetest.Fixture("exist_schema"))
	f := func(t *odps.Table, err error) {
		if err != nil {
			te.Fatalf("%+v", err)
		}

		println(fmt.Sprintf("%s, %s, %s", t.Name(), t.Owner(), t.Type()))
	}
	ts.List(f, nil)
}

func TestSchemas_CheckExists(t *testing.T) {
	requireLiveService(t)
	schemas := odps.NewSchemas(odpsIns, defaultProjectName)
	// not_exists is the fixture this suite asserts is absent. Naming it through
	// the same rule as the created fixtures is what makes that assertion mean
	// something: a leftover object from an unscoped run cannot answer for it.
	schema := schemas.Get(livetest.Fixture("not_exists"))
	exists, err := schema.Exists()
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if exists {
		t.Errorf("%s exists, the test needs an absent schema", livetest.Fixture("not_exists"))
	}

	schema = odps.NewSchema(odpsIns, defaultProjectName, livetest.Fixture("exist_schema"))
	exists, err = schema.Exists()
	if err != nil {
		t.Fatalf("%+v", err)
	}
	print(exists)
}

func TestSchemas_CreateSchema(t *testing.T) {
	requireLiveService(t)
	schemas := odps.NewSchemas(odpsIns, defaultProjectName)
	created := livetest.Fixture("new_schema")
	err := schemas.Create(created, true, "new comment")
	if err != nil {
		t.Fatalf("%+v", err)
	}
	t.Cleanup(func() {
		if err := schemas.Delete(created); err != nil {
			println("live fixture cleanup: drop schema " + created + ": " + err.Error())
		}
	})

	schema := odps.NewSchema(odpsIns, defaultProjectName, created)
	schema.Load()
	println(schema.Comment())
}

func TestSchemas_DeleteSchema(t *testing.T) {
	requireLiveService(t)
	schemas := odps.NewSchemas(odpsIns, defaultProjectName)

	target := livetest.Fixture("to_delete_schema")
	err := schemas.Create(target, true, "to delete")

	schema := odps.NewSchema(odpsIns, defaultProjectName, target)
	exists, err := schema.Exists()
	print(exists)

	err = schemas.Delete(target)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	exists, err = schema.Exists()
	print(exists)
}
