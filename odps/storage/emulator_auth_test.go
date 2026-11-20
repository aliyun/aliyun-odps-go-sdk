// Licensed under the Apache License, Version 2.0.

package storage_test

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aliyun/credentials-go/credentials"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/stretchr/testify/require"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/storage"
)

func TestEmulatorRefreshesSTSOnNamedQuota(t *testing.T) {
	endpoint := os.Getenv("MAXCOMPUTE_EMULATOR_AUTH_ENDPOINT")
	if endpoint == "" {
		t.Skip("MAXCOMPUTE_EMULATOR_AUTH_ENDPOINT is unset")
	}
	u, err := url.Parse(endpoint)
	require.NoError(t, err)
	require.True(t, u.Scheme == "http" && net.ParseIP(u.Hostname()).IsLoopback())
	ctx, cancel := context.WithTimeout(t.Context(), 35*time.Second)
	defer cancel()

	control := restclient.NewOdpsRestClient(account.NewAliyunAccount("fixture-go-sdk", "local-go-sdk-fixture"), endpoint)
	defer control.CloseIdleConnections()
	post := func(ctx context.Context, method, path, ttl string) string {
		t.Helper()
		params := url.Values{}
		if ttl != "" {
			params.Set("ttl_seconds", ttl)
		}
		req, err := control.NewRequestWithUrlQuery(method, path, nil, params)
		require.NoError(t, err)
		res, err := control.Do(req.WithContext(ctx))
		require.NoError(t, err)
		defer res.Body.Close()
		if res.StatusCode == http.StatusNoContent {
			return ""
		}
		var fixture struct{ Path string }
		require.NoError(t, json.NewDecoder(res.Body).Decode(&fixture))
		require.True(t, strings.HasPrefix(fixture.Path, "/__test/sts/"))
		return fixture.Path
	}
	path := post(ctx, http.MethodPost, "/__test/sts", "8")
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		post(cleanup, http.MethodDelete, path, "")
	})

	provider, err := credentials.NewCredential((&credentials.Config{}).
		SetType("credentials_uri").SetURLCredential(endpoint + path))
	require.NoError(t, err)
	refreshing := account.NewStsAccountWithCredential(provider, "cn-shanghai")
	original, err := refreshing.Credential()
	require.NoError(t, err)
	require.NotNil(t, original.AccessKeyId)
	require.NotNil(t, original.AccessKeySecret)
	require.NotNil(t, original.SecurityToken)
	newService := func(a account.Account) *odps.Odps {
		service := odps.NewOdps(a, endpoint+"/api")
		service.SetDefaultProjectName("test_project")
		service.SetHttpTimeout(4 * time.Second)
		return service
	}
	service := newService(refreshing)
	instance, err := service.ExecSQLContext(ctx, "select cast(7 as bigint) as id")
	require.NoError(t, err)
	require.NoError(t, instance.WaitForSuccessContext(ctx))
	client, err := storage.NewClient(ctx, service, storage.ClientOptions{QuotaName: "go-batch"})
	require.NoError(t, err)
	defer client.Close()
	session, err := client.CreateInstanceReadSession(ctx, instance.Id(), storage.InstanceReadOptions{})
	require.NoError(t, err)
	require.EqualValues(t, 1, session.RecordCount())

	stale := newService(account.NewStsAccount(*original.AccessKeyId, *original.AccessKeySecret, *original.SecurityToken, "cn-shanghai"))
	require.NoError(t, stale.DefaultProject().Load())
	require.Equal(t, path, post(ctx, http.MethodPost, path, "120"))
	serviceError(t, stale.DefaultProject().Load(), http.StatusUnauthorized, "Unauthorized")

	// The public credentials provider uses its cached generation until the TTL
	// reaches its refresh threshold. Wait past the original eight-second TTL,
	// then ask the same account to sign requests concurrently.
	timer := time.NewTimer(8 * time.Second)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	var readers sync.WaitGroup
	failures := make(chan error, 4)
	for range 4 {
		readers.Go(func() {
			_, err := service.DefaultProject().GetTunnelEndpointContext(ctx, "go-batch")
			failures <- err
		})
	}
	readers.Wait()
	close(failures)
	for err := range failures {
		require.NoError(t, err)
	}
	fresh, err := refreshing.Credential()
	require.NoError(t, err)
	require.True(t, fresh.AccessKeyId != nil && fresh.SecurityToken != nil &&
		*fresh.AccessKeyId != *original.AccessKeyId && *fresh.SecurityToken != *original.SecurityToken)

	plain, err := storage.NewClient(ctx, service, storage.ClientOptions{})
	require.NoError(t, err)
	defer plain.Close()
	_, err = plain.GetInstanceReadSession(ctx, instance.Id(), session.Session(), storage.InstanceReadOptions{})
	serviceError(t, err, http.StatusNotFound, "NoSuchSession")
	_, err = storage.NewClient(ctx, service, storage.ClientOptions{QuotaName: "absent"})
	serviceError(t, err, http.StatusNotFound, "QuotaNotExist")

	resumed, err := client.GetInstanceReadSession(ctx, instance.Id(), session.Session(), storage.InstanceReadOptions{})
	require.NoError(t, err)
	reader, err := resumed.OpenReader(ctx, nil)
	require.NoError(t, err)
	defer reader.Release()
	require.True(t, reader.Next(), "%v", reader.Err())
	require.EqualValues(t, 7, reader.RecordBatch().Column(0).(*array.Int64).Value(0))
	require.False(t, reader.Next())
	require.NoError(t, reader.Err())
}
