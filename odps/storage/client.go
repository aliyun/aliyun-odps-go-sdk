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

	"github.com/aliyun/aliyun-odps-go-sdk/internal/configvalue"
	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
)

const (
	resource         = "api/storage/v3"
	routeTokenHeader = "x-odps-max-storage-route-token"
)

// ClientOptions selects the project and Tunnel route used by a Client.
type ClientOptions struct {
	Project        string // Defaults to the ODPS client's project.
	TunnelEndpoint string // If set, use this address directly.
	QuotaName      string // Selects the quota for discovery and Storage requests.

	HTTPTimeout          time.Duration // Zero inherits the ODPS client's setting.
	TCPConnectionTimeout time.Duration // Zero inherits the ODPS client's setting.
	UserAgent            string        // Empty inherits the ODPS client's setting.
	DisableKeepAlives    bool
}

// Client shares authentication and any custom HTTP transport with an ODPS
// client. With the default transport it owns a separate connection pool. It
// can be used concurrently.
type Client struct {
	project string
	schema  string
	quota   string
	rest    restclient.RestClient
}

// NewClient creates a Storage API V2 client bound to one project.
func NewClient(ctx context.Context, service *odps.Odps, options ClientOptions) (*Client, error) {
	if service == nil || service.Account() == nil {
		return nil, fmt.Errorf("storage: an ODPS client with credentials is required")
	}
	return newClient(ctx, service.RestClient(), odps.Scope{Project: service.DefaultProjectName(), Schema: service.CurrentSchemaName()}, options)
}

// Open creates a Storage client in the core client's scope, using the supplied
// endpoint or performing quota-aware endpoint discovery with ctx.
func Open(ctx context.Context, service *odps.Client, options ClientOptions) (*Client, error) {
	if service == nil {
		return nil, fmt.Errorf("storage: an ODPS client is required")
	}
	return newClient(ctx, service.RestClient(), service.Scope(), options)
}

func newClient(ctx context.Context, source restclient.RestClient, scope odps.Scope, options ClientOptions) (*Client, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if options.HTTPTimeout < 0 || options.TCPConnectionTimeout < 0 {
		return nil, fmt.Errorf("storage: timeouts must be non-negative")
	}
	project := options.Project
	if project == "" {
		project = scope.Project
	}
	if err := validateIdentifier("project", project); err != nil {
		return nil, err
	}
	endpoint := options.TunnelEndpoint
	quota := strings.TrimSpace(options.QuotaName)
	if endpoint == "" {
		var err error
		endpoint, err = discoverEndpoint(ctx, source, project, quota)
		if err != nil {
			return nil, fmt.Errorf("storage: resolve Tunnel endpoint for %q: %w", project, err)
		}
	}
	u, err := configvalue.Endpoint(strings.TrimSpace(endpoint), "Tunnel endpoint")
	if err != nil {
		return nil, fmt.Errorf("storage: %w", err)
	}
	u.Path = strings.TrimSuffix(strings.TrimRight(u.Path, "/"), "/api")
	u.RawPath = ""
	schema := scope.Schema
	if schema == "" {
		schema = "default"
	}
	rest := source.NewForEndpoint(u.String())
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
	return &Client{project: project, schema: schema, quota: quota, rest: rest}, nil
}

func discoverEndpoint(ctx context.Context, client restclient.RestClient, project, quota string) (string, error) {
	client.SetDefaultProject(project)
	resource := common.NewResourceBuilder(project)
	params := make(url.Values, 1)
	if quota != "" {
		params.Set("quota", quota)
	}
	req, err := client.NewRequestWithUrlQuery(http.MethodGet, resource.Tunnel(), nil, params)
	if err != nil {
		return "", err
	}
	resp, err := client.Do(req.WithContext(ctx))
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	host, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return req.URL.Scheme + "://" + strings.TrimSpace(string(host)), nil
}

// Close releases idle connections in the SDK-managed pool. Release active
// Arrow readers separately and manage any custom transport yourself.
func (c *Client) Close() {
	c.rest.CloseIdleConnections()
}

// Table is the compatibility name for a table reference in the client's project.
type Table = odps.TableRef

func (c *Client) tableRef(table odps.TableRef) (odps.TableRef, error) {
	if table.Project == "" {
		table.Project = c.project
	}
	if table.Project != c.project {
		return odps.TableRef{}, fmt.Errorf("storage: table project does not match the client")
	}
	if table.Schema == "" {
		table.Schema = c.schema
	}
	if err := validateIdentifier("schema", table.Schema); err != nil {
		return odps.TableRef{}, err
	}
	if err := validateIdentifier("table", table.Name); err != nil {
		return odps.TableRef{}, err
	}
	return table, nil
}

func tableResource(table odps.TableRef) string {
	return fmt.Sprintf("projects.%s.schemas.%s.tables.%s", table.Project, table.Schema, table.Name)
}

func (c *Client) tableTarget(table odps.TableRef) (string, error) {
	table, err := c.tableRef(table)
	if err != nil {
		return "", err
	}
	return tableResource(table), nil
}

func validateIdentifier(kind, name string) error {
	if name == "" || strings.ContainsAny(name, "./\\") ||
		strings.IndexFunc(name, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0 {
		return fmt.Errorf("storage: invalid %s %q", kind, name)
	}
	return nil
}

func normalizePartitionSpec(spec string) (string, error) {
	parts := strings.Split(strings.ReplaceAll(spec, ",", "/"), "/")
	for i, part := range parts {
		name, value, ok := strings.Cut(part, "=")
		name = strings.TrimSpace(name)
		value = strings.TrimSpace(value)
		if value != "" && (value[0] == '\'' || value[0] == '"') {
			if len(value) < 2 || value[len(value)-1] != value[0] {
				return "", fmt.Errorf("storage: unmatched quotes in partition component %q", part)
			}
			value = value[1 : len(value)-1]
		}
		if !ok || name == "" || value == "" || strings.ContainsAny(value, "'\"=") {
			return "", fmt.Errorf("storage: invalid partition component %q: expected column=value", part)
		}
		parts[i] = name + "=" + value
	}
	return strings.Join(parts, "/"), nil
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
	if c.quota != "" {
		params.Set("quotaName", c.quota)
	}
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
			return resp.Header, fmt.Errorf("storage %s: decode response: %w", op.action, err)
		}
	} else if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		return resp.Header, fmt.Errorf("storage %s: read response: %w", op.action, err)
	}
	return resp.Header, nil
}
