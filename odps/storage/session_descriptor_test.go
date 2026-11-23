// Licensed under the Apache License, Version 2.0.

package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
)

func TestVersionedStorageDescriptorsRecoverScopesAndPlans(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		q := r.URL.Query()
		assert.Equal(t, "batch", q.Get("quotaName"))
		assert.Equal(t, "storage-check", r.Header.Get("x-odps-call-context"))
		switch q.Get("Action") {
		case "TableCreateReadSession":
			w.Header().Set(routeTokenHeader, "read-route")
			fmt.Fprint(w, "{\"SessionId\":\"pending\",\"SessionStatus\":\"INIT\"}")
		case "TableGetReadSession":
			assert.Equal(t, "read-route", r.Header.Get(routeTokenHeader))
			assert.Equal(t, "pending", q.Get("SessionId"))
			w.Header().Set(routeTokenHeader, "refreshed-route")
			fmt.Fprint(w, "{\"SessionStatus\":\"NORMAL\",\"RecordCount\":0,\"DataSchema\":{\"DataColumns\":[{\"Name\":\"i\",\"Type\":\"bigint\"}]}}")
		case "TableCreateWriteSession":
			assert.Equal(t, "Batch", q.Get("WriteMode"))
			w.Header().Set(routeTokenHeader, "write-route")
			fmt.Fprint(w, "{\"SessionId\":\"upload\",\"SessionStatus\":\"NORMAL\"}")
		case "TableGetWriteSession":
			assert.Equal(t, "write-route", r.Header.Get(routeTokenHeader))
			assert.Equal(t, "upload", q.Get("SessionId"))
			assert.Equal(t, "Batch", q.Get("WriteMode"))
			fmt.Fprint(w, "{\"Streams\":{\"worker-b\":2,\"worker-a\":1}}")
		default:
			t.Errorf("unexpected storage action %q", q.Get("Action"))
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	core, err := odps.NewClient(account.NewAliyunAccount("test-ak", "test-sk"), odps.ClientOptions{
		Endpoint: server.URL, Project: "alpha", Schema: "analytics",
	})
	require.NoError(t, err)
	opts := ClientOptions{TunnelEndpoint: server.URL, QuotaName: "batch"}
	client, err := Open(t.Context(), core, opts)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	ctx := odps.WithRequestMetadata(t.Context(), odps.RequestMetadata{CallContext: "storage-check"})
	table := odps.TableRef{Project: "alpha", Schema: "analytics", Name: "events"}
	pending, err := client.StartTableRead(ctx, table, TableReadOptions{})
	require.NoError(t, err)
	require.Equal(t, "INIT", pending.Status())
	require.False(t, pending.Plan().CountKnown)
	_, err = pending.OpenReader(ctx, nil)
	require.ErrorContains(t, err, "not ready")
	done, cancel := context.WithCancel(ctx)
	cancel()
	kept, err := pending.WaitReady(done)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, pending.Descriptor(), kept.Descriptor())
	require.EqualValues(t, 1, requests.Load())

	raw, err := json.Marshal(kept.Descriptor())
	require.NoError(t, err)
	var desc ReadSessionDescriptor
	require.NoError(t, json.Unmarshal(raw, &desc))
	require.Equal(t, table, desc.Table)
	invalid := desc
	invalid.QuotaName = "other"
	_, err = client.ResumeRead(ctx, invalid)
	require.ErrorContains(t, err, "project or quota")
	invalid = desc
	invalid.Version++
	_, err = client.ResumeRead(ctx, invalid)
	require.ErrorContains(t, err, "descriptor version")
	invalid = desc
	invalid.Kind = InstanceSnapshot
	_, err = client.ResumeRead(ctx, invalid)
	require.ErrorContains(t, err, "invalid instance read descriptor")
	require.EqualValues(t, 1, requests.Load())

	other, err := Open(ctx, core, opts)
	require.NoError(t, err)
	t.Cleanup(other.Close)
	active, err := other.ResumeRead(ctx, desc)
	require.NoError(t, err)
	active, err = active.WaitReady(ctx)
	require.NoError(t, err)
	require.Equal(t, "refreshed-route", active.Descriptor().RouteToken)
	plan := active.Plan()
	require.True(t, plan.CountKnown)
	require.Zero(t, plan.RecordCount)
	require.Equal(t, "i", plan.Schema.DataColumns[0].Name)
	plan.Schema.DataColumns[0].Name = "updated-copy"
	reader, err := active.OpenReader(ctx, nil)
	require.NoError(t, err)
	defer reader.Release()
	require.Equal(t, "i", reader.Schema().Field(0).Name)
	require.False(t, reader.Next())
	require.NoError(t, reader.Err())
	require.EqualValues(t, 2, requests.Load())

	writer, err := client.CreateTableWriteSession(ctx, table, TableWriteOptions{})
	require.NoError(t, err)
	writeJSON, err := json.Marshal(writer.Descriptor())
	require.NoError(t, err)
	var writeDesc WriteSessionDescriptor
	require.NoError(t, json.Unmarshal(writeJSON, &writeDesc))
	badWriter := writeDesc
	badWriter.Table.Project = "different"
	_, err = other.ResumeWrite(ctx, badWriter)
	require.ErrorContains(t, err, "project or quota")
	restored, err := other.ResumeWrite(ctx, writeDesc)
	require.NoError(t, err)
	require.Empty(t, restored.Status())
	want := []Stream{{ID: "worker-a", Version: 1}, {ID: "worker-b", Version: 2}}
	require.Equal(t, want, restored.ClosedStreams())
	fresh, err := writer.Refresh(ctx)
	require.NoError(t, err)
	require.Equal(t, want, fresh.ClosedStreams())
	require.Empty(t, writer.ClosedStreams())
}

func TestInstanceReadDescriptorPreservesQueryOptions(t *testing.T) {
	client := localClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("Action") {
		case "InstanceCreateReadSession", "InstanceGetReadSession":
			fmt.Fprint(w, "{\"DownloadID\":\"result\"}")
		default:
			t.Errorf("unexpected action: %s", r.URL)
		}
	})
	query := int64(7)
	snapshot, err := client.OpenSQLResult(t.Context(), odps.SQLJobRef{InstanceID: "job", TaskName: "task"},
		InstanceReadOptions{QueryID: &query, EnableLimit: true})
	require.NoError(t, err)
	query = 99
	descriptor := snapshot.Descriptor()
	require.EqualValues(t, 7, *descriptor.QueryID)
	require.True(t, descriptor.EnableLimit)
	reopened, err := client.ResumeRead(t.Context(), descriptor)
	require.NoError(t, err)
	require.Equal(t, descriptor, reopened.Descriptor())
	require.False(t, reopened.Plan().CountKnown)
}
