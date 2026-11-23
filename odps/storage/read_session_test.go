// Licensed under the Apache License, Version 2.0.

package storage

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTableReadSessionWaitsForReadyCount(t *testing.T) {
	payload := makeIPC(t, makeArrowBatch(t, 7, 8))
	var creates, lookups, reads atomic.Int32
	client := localClient(t, func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "projects.test_project.schemas.default.tables.table", q.Get("Target"))
		switch q.Get("Action") {
		case "TableCreateReadSession":
			creates.Add(1)
			w.Header().Set(routeTokenHeader, "initial-route")
			fmt.Fprint(w, `{"SessionId":"snapshot","SessionStatus":"INIT","RecordCount":-1}`)
		case "TableGetReadSession":
			assert.Equal(t, "snapshot", q.Get("SessionId"))
			assert.Equal(t, "false", q.Get("session_refresh"))
			if lookups.Add(1) == 1 {
				assert.Equal(t, "initial-route", r.Header.Get(routeTokenHeader))
				w.Header().Set(routeTokenHeader, "updated-route")
				fmt.Fprint(w, `{"SessionId":"snapshot","SessionStatus":"INIT","RecordCount":0}`)
				return
			}
			assert.Equal(t, "updated-route", r.Header.Get(routeTokenHeader))
			fmt.Fprint(w, `{"SessionId":"snapshot","SessionStatus":"NORMAL","RecordCount":2}`)
		case "TableRead":
			reads.Add(1)
			assert.EqualValues(t, 2, lookups.Load())
			assert.Equal(t, "snapshot", q.Get("SessionId"))
			assert.Equal(t, "updated-route", r.Header.Get(routeTokenHeader))
			assert.Equal(t, "2", q.Get("Count"))
			_, _ = w.Write(payload)
		default:
			t.Errorf("unexpected request %s", r.URL)
			http.NotFound(w, r)
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	session, err := client.CreateTableReadSession(ctx, Table{Name: "table"}, TableReadOptions{})
	require.NoError(t, err)
	require.Equal(t, Session{ID: "snapshot", RouteToken: "updated-route"}, session.Session())
	require.Equal(t, "NORMAL", session.Status())
	require.EqualValues(t, 2, session.RecordCount())
	require.EqualValues(t, 1, creates.Load())
	require.Zero(t, reads.Load())
	reader, err := session.OpenReader(ctx, nil)
	require.NoError(t, err)
	defer reader.Release()
	require.True(t, reader.Next())
	require.Equal(t, []int64{7, 8}, reader.RecordBatch().Column(0).(*array.Int64).Int64Values())
	require.False(t, reader.Next())
	require.NoError(t, reader.Err())
	require.EqualValues(t, 1, reads.Load())
}

func TestTableReadSessionCancellationDuringLookup(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	var lookups atomic.Int32
	client := localClient(t, func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		assert.Equal(t, "TableGetReadSession", q.Get("Action"))
		assert.Equal(t, "snapshot", q.Get("SessionId"))
		assert.Equal(t, "route", r.Header.Get(routeTokenHeader))
		if lookups.Add(1) == 1 {
			fmt.Fprint(w, `{"SessionId":"snapshot","SessionStatus":"INIT","RecordCount":-1}`)
			return
		}
		cancel()
	})
	session, err := client.GetTableReadSession(ctx, Table{Name: "table"}, Session{ID: "snapshot", RouteToken: "route"})
	require.ErrorIs(t, err, context.Canceled)
	require.NotNil(t, session)
	require.Equal(t, "snapshot", session.Descriptor().ID)
	require.Equal(t, "route", session.Descriptor().RouteToken)
	require.Equal(t, "INIT", session.Status())
	require.EqualValues(t, 2, lookups.Load())
}

func TestTableReadSessionTerminalStatus(t *testing.T) {
	for _, status := range []string{"CRITICAL", "EXPIRED"} {
		t.Run(status, func(t *testing.T) {
			client := localClient(t, func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "TableGetReadSession", r.URL.Query().Get("Action"))
				fmt.Fprintf(w, `{"SessionId":"snapshot","SessionStatus":%q,"RecordCount":-1}`, status)
			})
			session, err := client.GetTableReadSession(t.Context(), Table{Name: "table"}, Session{ID: "snapshot"})
			require.ErrorContains(t, err, status)
			require.Nil(t, session)
		})
	}
}

func TestEmptyTableReadUsesSessionSchema(t *testing.T) {
	client := localClient(t, func(w http.ResponseWriter, r *http.Request) {
		if !assert.Equal(t, "TableCreateReadSession", r.URL.Query().Get("Action")) {
			http.Error(w, "offset is outside an empty snapshot", http.StatusBadRequest)
			return
		}
		fmt.Fprint(w, `{"SessionId":"empty","SessionStatus":"NORMAL","RecordCount":0,
			"DataSchema":{"DataColumns":[{"Name":"id","Type":"bigint","Nullable":false}],
			"PartitionColumns":[{"Name":"ds","Type":"string","Nullable":true}]}}`)
	})
	ctx := t.Context()
	snapshot, err := client.CreateTableReadSession(ctx, Table{Name: "table"}, TableReadOptions{
		Columns: []string{"id"}, PartitionColumns: []string{"ds"},
	})
	require.NoError(t, err)
	reader, err := snapshot.OpenReader(ctx, nil)
	require.NoError(t, err)
	defer reader.Release()
	require.Equal(t, []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "ds", Type: arrow.BinaryTypes.String, Nullable: true},
	}, reader.Schema().Fields())
	require.False(t, reader.Next())
	require.NoError(t, reader.Err())
}
