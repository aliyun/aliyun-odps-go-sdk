// Licensed under the Apache License, Version 2.0.

package odps

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"time"
	"unicode"

	"github.com/aliyun/aliyun-odps-go-sdk/internal/configvalue"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
)

// Scope is the default project and, optionally, schema for a client.
type Scope struct {
	Project string
	Schema  string
}

// ClientOptions configures the core API client.
type ClientOptions struct {
	Endpoint string
	Project  string
	Schema   string
	Region   string

	// Transport is optional. Its owner manages its connection pool.
	Transport http.RoundTripper
	UserAgent string

	// HTTPTimeout bounds one HTTP exchange, including the response body.
	// Zero leaves the deadline to the request context.
	HTTPTimeout time.Duration
	// TCPConnectionTimeout bounds connection establishment. Zero uses the SDK default.
	TCPConnectionTimeout time.Duration
}

// Client provides a fixed default scope and request settings. It can be used
// concurrently with a concurrency-safe account and transport.
type Client struct {
	service *Odps
}

// NewClient validates options and creates a client without making requests.
// Credentials may rotate through the supplied account implementation.
func NewClient(credentials account.Account, opts ClientOptions) (*Client, error) {
	if credentials == nil || reflect.ValueOf(credentials).Kind() == reflect.Pointer && reflect.ValueOf(credentials).IsNil() {
		return nil, fmt.Errorf("ODPS credentials are required")
	}
	endpoint, err := configvalue.Endpoint(opts.Endpoint, "endpoint")
	if err != nil {
		return nil, err
	}
	if err := validateScope(Scope{Project: opts.Project, Schema: opts.Schema}); err != nil {
		return nil, err
	}
	if opts.HTTPTimeout < 0 || opts.TCPConnectionTimeout < 0 {
		return nil, fmt.Errorf("client timeouts must be non-negative")
	}

	service := NewOdpsWithHTTPTransport(credentials, endpoint.String(), opts.Transport)
	service.SetDefaultProjectName(opts.Project)
	service.SetCurrentSchemaName(opts.Schema)
	service.SetHttpTimeout(opts.HTTPTimeout)
	if opts.TCPConnectionTimeout > 0 {
		service.SetTcpConnectTimeout(opts.TCPConnectionTimeout)
	}
	service.SetUserAgent(opts.UserAgent)
	if opts.Region != "" {
		service.Options.RegionId = opts.Region
	}
	return &Client{service: service}, nil
}

// Scope returns the client's defaults.
func (c *Client) Scope() Scope {
	return Scope{Project: c.service.DefaultProjectName(), Schema: c.service.CurrentSchemaName()}
}

// WithScope returns a new client borrowing the account, transport and pool.
// An empty Project inherits c's project. The new Schema is used verbatim.
func (c *Client) WithScope(scope Scope) (*Client, error) {
	if scope.Project == "" {
		scope.Project = c.service.DefaultProjectName()
	}
	if err := validateScope(scope); err != nil {
		return nil, err
	}
	return c.withScope(scope), nil
}

func (c *Client) withScope(scope Scope) *Client {
	if c.Scope() == scope {
		return c
	}
	service := &Odps{account: c.service.account, restClient: c.service.restClient, Options: c.service.Options}
	service.SetDefaultProjectName(scope.Project)
	service.SetCurrentSchemaName(scope.Schema)
	service.projects = NewProjects(service)
	return &Client{service: service}
}

// Ping checks whether the credentials can read the default project.
func (c *Client) Ping(ctx context.Context) error {
	return c.service.DefaultProject().LoadContext(ctx)
}

// RestClient returns a signed transport configuration for protocol integrations.
// Copies with unchanged transport settings share the core client's pool.
func (c *Client) RestClient() restclient.RestClient {
	return c.service.restClient
}

// RequestMetadata carries call and trace information on core and data requests.
type RequestMetadata = restclient.RequestMetadata

// WithRequestMetadata attaches call context, W3C trace headers and a User-Agent
// suffix to requests using ctx. Metadata is applied before request signing.
func WithRequestMetadata(ctx context.Context, metadata RequestMetadata) context.Context {
	return restclient.WithRequestMetadata(ctx, metadata)
}

// CloseIdleConnections closes SDK-owned idle core connections. In-flight
// requests and caller-owned transports remain usable.
func (c *Client) CloseIdleConnections() {
	c.service.restClient.CloseIdleConnections()
}

// TunnelEndpoint resolves a legacy Tunnel route for the specified project and quota.
// An empty project selects the client's default project.
func (c *Client) TunnelEndpoint(ctx context.Context, project, quota string) (string, error) {
	if project == "" {
		project = c.Scope().Project
	}
	if err := validateName("project", project); err != nil {
		return "", err
	}
	client := c.withScope(Scope{Project: project})
	return client.service.Project(project).GetTunnelEndpointContext(ctx, quota)
}

func validateScope(scope Scope) error {
	if err := validateName("project", scope.Project); err != nil {
		return err
	}
	if scope.Schema != "" {
		return validateName("schema", scope.Schema)
	}
	return nil
}

func validateName(kind, name string) error {
	if name == "" || name == "." || name == ".." ||
		strings.ContainsAny(name, "/\\?#%") || strings.IndexFunc(name, func(r rune) bool {
		return unicode.IsControl(r) || unicode.IsSpace(r)
	}) >= 0 {
		return fmt.Errorf("invalid %s name", kind)
	}
	return nil
}
