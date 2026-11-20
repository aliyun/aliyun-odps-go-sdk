// Licensed under the Apache License, Version 2.0.

// Package storage reads and writes MaxCompute data using Storage API V2 and Apache Arrow.
package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
)

const (
	resource         = "api/storage/v3"
	routeTokenHeader = "x-odps-max-storage-route-token"
)

// ClientOptions selects the project and Tunnel route used by a Client.
type ClientOptions struct {
	Project        string // Defaults to the ODPS client's project.
	TunnelEndpoint string // If empty, resolve the endpoint for Project and QuotaName.
	QuotaName      string // Used when resolving a Tunnel endpoint.

	HTTPTimeout          time.Duration // Zero inherits the ODPS client's setting.
	TCPConnectionTimeout time.Duration // Zero inherits the ODPS client's setting.
	UserAgent            string        // Empty inherits the ODPS client's setting.
	DisableKeepAlives    bool
}

// Client shares authentication with an ODPS client and owns a separate HTTP connection pool.
// It can be used concurrently.
type Client struct {
	project string
	schema  string
	rest    restclient.RestClient
}

// NewClient creates a Storage API V2 client bound to one project.
func NewClient(ctx context.Context, service *odps.Odps, options ClientOptions) (*Client, error) {
	if service == nil || service.Account() == nil {
		return nil, fmt.Errorf("storage: an ODPS client with credentials is required")
	}
	project := options.Project
	if project == "" {
		project = service.DefaultProjectName()
	}
	if err := validateIdentifier("project", project); err != nil {
		return nil, err
	}
	endpoint := options.TunnelEndpoint
	if endpoint == "" {
		var err error
		endpoint, err = service.Project(project).GetTunnelEndpointContext(ctx, options.QuotaName)
		if err != nil {
			return nil, fmt.Errorf("storage: resolve Tunnel endpoint for %q: %w", project, err)
		}
	}
	u, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") ||
		u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("storage: invalid Tunnel endpoint")
	}
	u.Path = strings.TrimSuffix(strings.TrimRight(u.Path, "/"), "/api")
	u.RawPath = ""
	schema := service.CurrentSchemaName()
	if schema == "" {
		schema = "default"
	}
	source := service.RestClient()
	rest := restclient.NewOdpsRestClient(service.Account(), u.String())
	rest.SetDefaultProject(project)
	rest.SetUserAgent(source.UserAgent())
	rest.HttpTimeout = source.HttpTimeout
	rest.TcpConnectionTimeout = source.TcpConnectionTimeout
	rest.DisableKeepAlives = options.DisableKeepAlives || source.DisableKeepAlives
	if options.HTTPTimeout != 0 {
		rest.HttpTimeout = options.HTTPTimeout
	}
	if options.TCPConnectionTimeout != 0 {
		rest.TcpConnectionTimeout = options.TCPConnectionTimeout
	}
	if options.UserAgent != "" {
		rest.SetUserAgent(options.UserAgent)
	}
	return &Client{project: project, schema: schema, rest: rest}, nil
}

// Close releases idle HTTP connections. Release active Arrow readers separately.
func (c *Client) Close() {
	c.rest.CloseIdleConnections()
}

// Table identifies a table in the Client's project.
type Table struct {
	Schema string // Defaults to the ODPS client's current schema or "default".
	Name   string
}

func (c *Client) tableTarget(table Table) (string, error) {
	schema := table.Schema
	if schema == "" {
		schema = c.schema
	}
	for _, field := range []struct{ kind, value string }{{"schema", schema}, {"table", table.Name}} {
		if err := validateIdentifier(field.kind, field.value); err != nil {
			return "", err
		}
	}
	return fmt.Sprintf("projects.%s.schemas.%s.tables.%s", c.project, schema, table.Name), nil
}

func validateIdentifier(kind, name string) error {
	if name == "" || strings.ContainsAny(name, "./\\") ||
		strings.IndexFunc(name, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0 {
		return fmt.Errorf("storage: invalid %s %q", kind, name)
	}
	return nil
}

type operation struct {
	action string
	target string
	params url.Values
	route  string
}

func (c *Client) request(ctx context.Context, method string, op operation, body io.Reader, headers map[string]string) (*http.Response, error) {
	params := op.params
	if params == nil {
		params = make(url.Values)
	}
	params.Set("Action", op.action)
	params.Set("Target", op.target)
	req, err := c.rest.NewRequestWithParamsAndHeaders(method, resource, body, params, headers)
	if err != nil {
		return nil, err
	}
	if op.route != "" {
		req.Header.Set(routeTokenHeader, op.route)
	}
	resp, err := c.rest.Do(req.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("storage %s: %w", op.action, err)
	}
	return resp, nil
}

func (c *Client) jsonRequest(ctx context.Context, method string, op operation, input, output any) (http.Header, error) {
	var body io.Reader
	if input != nil {
		encoded, err := json.Marshal(input)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(encoded)
	}
	resp, err := c.request(ctx, method, op, body, map[string]string{"Content-Type": "application/json; charset=utf-8"})
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if output != nil {
		if err := json.NewDecoder(resp.Body).Decode(output); err != nil {
			return nil, fmt.Errorf("storage %s: decode response: %w", op.action, err)
		}
	} else if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		return nil, fmt.Errorf("storage %s: read response: %w", op.action, err)
	}
	return resp.Header, nil
}
