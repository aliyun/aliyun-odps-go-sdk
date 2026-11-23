// Licensed under the Apache License, Version 2.0.

package tunnel

import (
	"context"
	"fmt"
	"strings"

	"github.com/aliyun/aliyun-odps-go-sdk/internal/configvalue"
	"github.com/aliyun/aliyun-odps-go-sdk/odps"
)

// ResultOptions selects the Tunnel route for a standard SQL result download.
type ResultOptions struct {
	Endpoint  string
	QuotaName string
	// KeepAlive opts in to pooling SDK connections for repeated reads.
	// Call the session's CloseIdleConnections when done.
	KeepAlive bool
}

// OpenSQLResult opens a Protobuf result download for a completed SQL instance.
// Use InstanceSessionCfg options for cached or incremental result protocols.
func OpenSQLResult(ctx context.Context, service *odps.Client, job odps.SQLJobRef, opts ResultOptions, sessionOptions ...InstanceOption) (*InstanceResultDownloadSession, error) {
	if service == nil {
		return nil, fmt.Errorf("tunnel: an ODPS client is required")
	}
	if job.InstanceID == "" || job.InstanceID == "." || job.InstanceID == ".." || strings.ContainsAny(job.InstanceID, "/\\?#%") {
		return nil, fmt.Errorf("tunnel: invalid instance ID")
	}
	if job.Project == "" {
		job.Project = service.Scope().Project
	}
	scoped, err := service.WithScope(odps.Scope{Project: job.Project, Schema: service.Scope().Schema})
	if err != nil {
		return nil, err
	}
	endpoint := opts.Endpoint
	if endpoint == "" {
		endpoint, err = scoped.TunnelEndpoint(ctx, job.Project, opts.QuotaName)
		if err != nil {
			return nil, err
		}
	}
	resolved, err := configvalue.Endpoint(endpoint, "Tunnel endpoint")
	if err != nil {
		return nil, err
	}
	source := scoped.RestClient()
	client := source.NewForEndpoint(resolved.String())
	client.HttpTimeout, client.TcpConnectionTimeout = source.HttpTimeout, source.TcpConnectionTimeout
	client.SetDefaultProject(job.Project)
	client.SetUserAgent(source.UserAgent())
	client.DisableKeepAlives = !opts.KeepAlive
	session, err := createInstanceResultDownloadSession(ctx, job.Project, job.InstanceID, opts.QuotaName, client, sessionOptions...)
	if err != nil {
		client.CloseIdleConnections()
	}
	return session, err
}
