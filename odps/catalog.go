// Licensed under the Apache License, Version 2.0.

package odps

import (
	"context"
	"encoding/xml"
	"fmt"
	"iter"
	"net/url"
	"strconv"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
)

// PageOptions controls one listing request. Zero PageSize uses the server default.
type PageOptions struct {
	PageSize  int
	PageToken string
}

// Page contains a page of results and the opaque token for the next page.
type Page[T any] struct {
	Items         []T
	NextPageToken string
}

func pageQuery(page PageOptions, namePrefix, owner string) (url.Values, error) {
	if page.PageSize < 0 {
		return nil, fmt.Errorf("page size must be non-negative")
	}
	q := url.Values{"expectmarker": {"true"}}
	if page.PageToken != "" {
		q.Set("marker", page.PageToken)
	}
	if page.PageSize != 0 {
		q.Set("maxitems", strconv.Itoa(page.PageSize))
	}
	if namePrefix != "" {
		q.Set("name", namePrefix)
	}
	if owner != "" {
		q.Set("owner", owner)
	}
	return q, nil
}

func listPages[T any](ctx context.Context, token string, fetch func(string) (Page[T], error)) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		var zero T
		seen := make(map[string]bool)
		for current := token; ; {
			if err := ctx.Err(); err != nil {
				yield(zero, err)
				return
			}
			seen[current] = true
			page, err := fetch(current)
			if err != nil {
				yield(zero, err)
				return
			}
			for _, value := range page.Items {
				if err := ctx.Err(); err != nil {
					yield(zero, err)
					return
				}
				if !yield(value, nil) {
					return
				}
			}
			current = page.NextPageToken
			if current == "" {
				return
			}
			if seen[current] {
				yield(zero, fmt.Errorf("catalog listing returned a repeated page token"))
				return
			}
		}
	}
}

// TableRef identifies a table. Empty Project and Schema use the client's defaults.
type TableRef struct {
	Project string
	Schema  string
	Name    string
}

// ResourceRef identifies a resource using the same scope rules as TableRef.
type ResourceRef TableRef

// FunctionRef identifies a function using the same scope rules as TableRef.
type FunctionRef TableRef

func (c *Client) resolveRef(ref TableRef, kind string) (TableRef, error) {
	scope := c.Scope()
	if ref.Project == "" {
		ref.Project = scope.Project
	}
	if ref.Schema == "" {
		ref.Schema = scope.Schema
	}
	if err := validateScope(Scope{Project: ref.Project, Schema: ref.Schema}); err != nil {
		return TableRef{}, err
	}
	if err := validateName(kind, ref.Name); err != nil {
		return TableRef{}, err
	}
	return ref, nil
}

// TableSummary is one catalog entry. Type is TableTypeUnknown when omitted by the service.
type TableSummary struct {
	Ref     TableRef
	ID      string
	Type    TableType
	Owner   string
	Comment string
}

// TableMetadata is an owned snapshot of the table's schema and identity.
type TableMetadata struct {
	TableSummary
	Schema tableschema.TableSchema
}

// TableListOptions filters tables in the client's scope.
type TableListOptions struct {
	PageOptions
	NamePrefix string
	Owner      string
	Type       *TableType
}

// TableCatalog reads table descriptions and lists tables in a fixed scope.
type TableCatalog struct{ client *Client }

// Tables provides table metadata and pagination for the client's scope.
func (c *Client) Tables() TableCatalog { return TableCatalog{client: c} }

// Get reads table metadata. Use GetExtended when extended statistics are needed.
func (t TableCatalog) Get(ctx context.Context, ref TableRef) (TableMetadata, error) {
	return t.get(ctx, ref, false)
}

// GetExtended reads table metadata and explicitly requests extended information.
func (t TableCatalog) GetExtended(ctx context.Context, ref TableRef) (TableMetadata, error) {
	return t.get(ctx, ref, true)
}

func (t TableCatalog) get(ctx context.Context, ref TableRef, extended bool) (TableMetadata, error) {
	ref, err := t.client.resolveRef(ref, "table")
	if err != nil {
		return TableMetadata{}, err
	}
	client := t.client.withScope(Scope{Project: ref.Project, Schema: ref.Schema})
	table := NewTable(client.service, ref.Project, ref.Schema, ref.Name)
	if err := table.loadBasicContext(ctx); err != nil {
		return TableMetadata{}, err
	}
	if extended {
		if err := table.loadExtendedContext(ctx); err != nil {
			return TableMetadata{}, err
		}
	}
	info := tableSummary(client.Scope(), table.model)
	info.Comment, info.Owner = table.Comment(), table.Owner()
	return TableMetadata{TableSummary: info, Schema: table.Schema()}, nil
}

func tableSummary(scope Scope, m tableModel) TableSummary {
	schema := m.SchemaName
	if schema == "" {
		schema = scope.Schema
	}
	kind := TableTypeUnknown
	if m.Type != nil {
		kind = *m.Type
	}
	return TableSummary{
		Ref: TableRef{Project: scope.Project, Schema: schema, Name: m.Name},
		ID:  m.TableID, Type: kind, Owner: m.Owner, Comment: m.Comment,
	}
}

func tablesPage(ctx context.Context, client restclient.RestClient, project string, q url.Values) (Page[tableModel], error) {
	var result struct {
		XMLName xml.Name     `xml:"Tables"`
		Tables  []tableModel `xml:"Table"`
		Marker  string
	}
	rb := common.NewResourceBuilder(project)
	err := client.GetWithModelContext(ctx, rb.Tables(), q, nil, &result)
	return Page[tableModel]{Items: result.Tables, NextPageToken: result.Marker}, err
}

// ListPage returns one page of table summaries.
func (t TableCatalog) ListPage(ctx context.Context, opts TableListOptions) (Page[TableSummary], error) {
	q, err := pageQuery(opts.PageOptions, opts.NamePrefix, opts.Owner)
	if err != nil {
		return Page[TableSummary]{}, err
	}
	scope := t.client.Scope()
	if scope.Schema != "" {
		q.Set("curr_schema", scope.Schema)
	}
	if opts.Type != nil {
		if *opts.Type < ManagedTable || *opts.Type >= TableTypeUnknown {
			return Page[TableSummary]{}, fmt.Errorf("invalid table type filter")
		}
		q.Set("type", opts.Type.String())
	}
	raw, err := tablesPage(ctx, t.client.RestClient(), scope.Project, q)
	if err != nil {
		return Page[TableSummary]{}, err
	}
	items := make([]TableSummary, 0, len(raw.Items))
	for _, m := range raw.Items {
		items = append(items, tableSummary(scope, m))
	}
	return Page[TableSummary]{Items: items, NextPageToken: raw.NextPageToken}, nil
}

// List iterates pages lazily. An early break prevents further requests.
func (t TableCatalog) List(ctx context.Context, opts TableListOptions) iter.Seq2[TableSummary, error] {
	return listPages(ctx, opts.PageToken, func(token string) (Page[TableSummary], error) {
		current := opts
		current.PageToken = token
		return t.ListPage(ctx, current)
	})
}

// ProjectMetadata contains the stable attributes of a project.
type ProjectMetadata struct {
	Name     string
	Owner    string
	Comment  string
	Type     string
	Status   ProjectStatus
	Region   string
	TenantID string
}

// ProjectListOptions filters the projects visible to the client.
type ProjectListOptions struct {
	PageOptions
	NamePrefix string
	Owner      string
	User       string
	Group      string
}

// ProjectCatalog reads projects visible to the client's account.
type ProjectCatalog struct{ client *Client }

// Projects provides project metadata and pagination.
func (c *Client) Projects() ProjectCatalog { return ProjectCatalog{client: c} }

// Get reads the named project. An empty name uses the client's project.
func (p ProjectCatalog) Get(ctx context.Context, name string) (ProjectMetadata, error) {
	if name == "" {
		name = p.client.Scope().Project
	}
	if err := validateName("project", name); err != nil {
		return ProjectMetadata{}, err
	}
	client := p.client.withScope(Scope{Project: name})
	model, err := client.service.Project(name).loadFromOdps(ctx, optionalParams{})
	if err != nil {
		return ProjectMetadata{}, err
	}
	if model.Name == "" {
		model.Name = name
	}
	return projectMetadata(*model), nil
}

func projectMetadata(m projectModel) ProjectMetadata {
	return ProjectMetadata{
		Name: m.Name, Owner: m.Owner, Comment: m.Comment, Type: m.Type,
		Status: m.Status, Region: m.RegionId, TenantID: m.TenantId,
	}
}

func projectsPage(ctx context.Context, client restclient.RestClient, q url.Values) (Page[projectModel], error) {
	var result struct {
		XMLName  xml.Name       `xml:"Projects"`
		Projects []projectModel `xml:"Project"`
		Marker   string
	}
	rb := common.ResourceBuilder{}
	err := client.GetWithModelContext(ctx, rb.Projects(), q, nil, &result)
	return Page[projectModel]{Items: result.Projects, NextPageToken: result.Marker}, err
}

// ListPage returns one page of visible projects.
func (p ProjectCatalog) ListPage(ctx context.Context, opts ProjectListOptions) (Page[ProjectMetadata], error) {
	q, err := pageQuery(opts.PageOptions, opts.NamePrefix, opts.Owner)
	if err != nil {
		return Page[ProjectMetadata]{}, err
	}
	if opts.User != "" {
		q.Set("user", opts.User)
	}
	if opts.Group != "" {
		q.Set("group", opts.Group)
	}
	raw, err := projectsPage(ctx, p.client.RestClient(), q)
	if err != nil {
		return Page[ProjectMetadata]{}, err
	}
	items := make([]ProjectMetadata, 0, len(raw.Items))
	for _, m := range raw.Items {
		items = append(items, projectMetadata(m))
	}
	return Page[ProjectMetadata]{Items: items, NextPageToken: raw.NextPageToken}, nil
}

// List iterates visible projects in service order.
func (p ProjectCatalog) List(ctx context.Context, opts ProjectListOptions) iter.Seq2[ProjectMetadata, error] {
	return listPages(ctx, opts.PageToken, func(token string) (Page[ProjectMetadata], error) {
		current := opts
		current.PageToken = token
		return p.ListPage(ctx, current)
	})
}

// SchemaMetadata contains catalog attributes without a lazy-loading handle.
type SchemaMetadata struct {
	Project    string
	Name       string
	Owner      string
	Comment    string
	Type       string
	CreatedAt  time.Time
	ModifiedAt time.Time
}

// SchemaListOptions filters the schemas in a project.
type SchemaListOptions struct {
	PageOptions
	NamePrefix string
	Owner      string
}

// SchemaCatalog reads schemas in the client's project.
type SchemaCatalog struct{ client *Client }

// Schemas provides schema metadata and pagination.
func (c *Client) Schemas() SchemaCatalog { return SchemaCatalog{client: c} }

// Get reads a schema in the project. An empty name uses the client's schema.
func (s SchemaCatalog) Get(ctx context.Context, name string) (SchemaMetadata, error) {
	if name == "" {
		name = s.client.Scope().Schema
	}
	if err := validateName("schema", name); err != nil {
		return SchemaMetadata{}, err
	}
	project := s.client.Scope().Project
	schema := NewSchema(s.client.service, project, name)
	if err := schema.loadContext(ctx); err != nil {
		return SchemaMetadata{}, err
	}
	return schemaMetadata(project, schema.model), nil
}

func schemaMetadata(project string, m schemaModel) SchemaMetadata {
	return SchemaMetadata{Project: project, Name: m.Name, Owner: m.Owner, Comment: m.Comment,
		Type: m.Type, CreatedAt: m.CreateTime, ModifiedAt: m.ModifiedTime}
}

func schemasPage(ctx context.Context, client restclient.RestClient, project string, q url.Values) (Page[schemaModel], error) {
	var result struct {
		XMLName xml.Name      `xml:"Schemas"`
		Schemas []schemaModel `xml:"Schema"`
		Marker  string
	}
	rb := common.NewResourceBuilder(project)
	err := client.GetWithModelContext(ctx, rb.Schemas(), q, nil, &result)
	return Page[schemaModel]{Items: result.Schemas, NextPageToken: result.Marker}, err
}

// ListPage returns one page of schemas.
func (s SchemaCatalog) ListPage(ctx context.Context, opts SchemaListOptions) (Page[SchemaMetadata], error) {
	q, err := pageQuery(opts.PageOptions, opts.NamePrefix, opts.Owner)
	if err != nil {
		return Page[SchemaMetadata]{}, err
	}
	project := s.client.Scope().Project
	raw, err := schemasPage(ctx, s.client.RestClient(), project, q)
	if err != nil {
		return Page[SchemaMetadata]{}, err
	}
	items := make([]SchemaMetadata, 0, len(raw.Items))
	for _, m := range raw.Items {
		items = append(items, schemaMetadata(project, m))
	}
	return Page[SchemaMetadata]{Items: items, NextPageToken: raw.NextPageToken}, nil
}

// List iterates schemas lazily.
func (s SchemaCatalog) List(ctx context.Context, opts SchemaListOptions) iter.Seq2[SchemaMetadata, error] {
	return listPages(ctx, opts.PageToken, func(token string) (Page[SchemaMetadata], error) {
		current := opts
		current.PageToken = token
		return s.ListPage(ctx, current)
	})
}

// ResourceMetadata describes a resource in its project and schema.
type ResourceMetadata struct {
	Ref             ResourceRef
	Type            ResourceType
	Owner           string
	Comment         string
	SizeBytes       int64
	ContentMD5      string
	SourceTableName string
}

// ResourceListOptions filters the resources visible in a scope.
type ResourceListOptions struct {
	PageOptions
	NamePrefix string
}

// ResourceCatalog reads resources in the client's scope.
type ResourceCatalog struct{ client *Client }

// Resources provides resource metadata and pagination.
func (c *Client) Resources() ResourceCatalog { return ResourceCatalog{client: c} }

// Get reads the specified resource's metadata.
func (r ResourceCatalog) Get(ctx context.Context, ref ResourceRef) (ResourceMetadata, error) {
	target, err := r.client.resolveRef(TableRef(ref), "resource")
	if err != nil {
		return ResourceMetadata{}, err
	}
	client := r.client.withScope(Scope{Project: target.Project, Schema: target.Schema})
	resource := NewResource(resourceModel{Name: target.Name, SchemaName: target.Schema}, target.Project, client.service)
	if err := resource.loadContext(ctx); err != nil {
		return ResourceMetadata{}, err
	}
	return resourceMetadata(client.Scope(), resource.Model), nil
}

func resourceMetadata(scope Scope, m resourceModel) ResourceMetadata {
	ref := ResourceRef{Project: scope.Project, Schema: m.SchemaName, Name: m.Name}
	if ref.Schema == "" {
		ref.Schema = scope.Schema
	}
	return ResourceMetadata{Ref: ref, Type: ResourceTypeFromStr(string(m.ResourceType)), Owner: m.Owner,
		Comment: m.Comment, SizeBytes: m.ResourceSize, ContentMD5: m.ContentMD5, SourceTableName: m.SourceTableName}
}

func resourcesPage(ctx context.Context, client restclient.RestClient, project string, q url.Values) (Page[resourceModel], error) {
	var result struct {
		XMLName   xml.Name        `xml:"Resources"`
		Resources []resourceModel `xml:"Resource"`
		Marker    string
	}
	rb := common.NewResourceBuilder(project)
	err := client.GetWithModelContext(ctx, rb.Resources(), q, nil, &result)
	return Page[resourceModel]{Items: result.Resources, NextPageToken: result.Marker}, err
}

// ListPage returns one page of resource descriptions.
func (r ResourceCatalog) ListPage(ctx context.Context, opts ResourceListOptions) (Page[ResourceMetadata], error) {
	q, err := pageQuery(opts.PageOptions, opts.NamePrefix, "")
	if err != nil {
		return Page[ResourceMetadata]{}, err
	}
	scope := r.client.Scope()
	if scope.Schema != "" {
		q.Set("curr_schema", scope.Schema)
	}
	raw, err := resourcesPage(ctx, r.client.RestClient(), scope.Project, q)
	if err != nil {
		return Page[ResourceMetadata]{}, err
	}
	items := make([]ResourceMetadata, 0, len(raw.Items))
	for _, m := range raw.Items {
		items = append(items, resourceMetadata(scope, m))
	}
	return Page[ResourceMetadata]{Items: items, NextPageToken: raw.NextPageToken}, nil
}

// List iterates resources lazily.
func (r ResourceCatalog) List(ctx context.Context, opts ResourceListOptions) iter.Seq2[ResourceMetadata, error] {
	return listPages(ctx, opts.PageToken, func(token string) (Page[ResourceMetadata], error) {
		current := opts
		current.PageToken = token
		return r.ListPage(ctx, current)
	})
}

// FunctionMetadata describes an ODPS function and its dependencies.
type FunctionMetadata struct {
	Ref           FunctionRef
	Owner         string
	ClassPath     string
	Resources     []FResource
	SQLDefinition string
	Language      string
}

// FunctionCatalog provides scoped function metadata.
type FunctionCatalog struct{ client *Client }

// Functions provides function metadata for the client's scope.
func (c *Client) Functions() FunctionCatalog { return FunctionCatalog{client: c} }

// Get reads metadata for the specified function.
func (f FunctionCatalog) Get(ctx context.Context, ref FunctionRef) (FunctionMetadata, error) {
	target, err := f.client.resolveRef(TableRef(ref), "function")
	if err != nil {
		return FunctionMetadata{}, err
	}
	client := f.client.withScope(Scope{Project: target.Project, Schema: target.Schema})
	function := &Function{
		ProjectName: target.Project,
		OdpsIns:     client.service,
		Model:       functionModel{Name: target.Name, SchemaName: target.Schema},
	}
	if err := function.loadContext(ctx); err != nil {
		return FunctionMetadata{}, err
	}
	m := function.Model
	return FunctionMetadata{
		Ref:   FunctionRef{Project: target.Project, Schema: m.SchemaName, Name: m.Name},
		Owner: m.Owner, ClassPath: m.ClassPath, Resources: m.Resources,
		SQLDefinition: m.SQLDefinitionText, Language: m.ProgramLanguage,
	}, nil
}
