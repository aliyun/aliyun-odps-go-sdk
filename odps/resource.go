package odps

import (
	"context"
	"encoding/xml"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/pkg/errors"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
)

// ResourceType is  resource type
type ResourceType string

const (
	// ResourceTypeFile is file resource type
	ResourceTypeFile ResourceType = "file"

	// ResourceTypeJar is jar resource type
	ResourceTypeJar ResourceType = "jar"

	// ResourceTypeArchive is archive resource type, support .zip/.tgz/.tar.gz/.tar/.jar
	ResourceTypeArchive ResourceType = "archive"

	// ResourceTypePy is py resource type
	ResourceTypePy ResourceType = "py"

	// ResourceTypeTable is table resource type
	ResourceTypeTable ResourceType = "table"

	// ResourceTypeVolumeFile is volume_file resource type
	ResourceTypeVolumeFile ResourceType = "volume_file"

	// ResourceTypeVolumeArchive is volume_archive resource type
	ResourceTypeVolumeArchive ResourceType = "volume_archive"

	// ResourceTypeUnknown is unknown resource type
	ResourceTypeUnknown ResourceType = "unknown"
)

// ResourceTypeFromStr convert string to ResourceType
func ResourceTypeFromStr(str string) ResourceType {
	lstr := strings.ToLower(str)
	switch lstr {
	case "file":
		return ResourceTypeFile
	case "jar":
		return ResourceTypeJar
	case "archive":
		return ResourceTypeArchive
	case "py":
		return ResourceTypePy
	case "table":
		return ResourceTypeTable
	case "volume_file":
		return ResourceTypeVolumeFile
	case "volume_archive":
		return ResourceTypeVolumeArchive
	default:
		return ResourceTypeUnknown
	}
}

// ResourceTypeToStr convert ResourceType to string
func ResourceTypeToStr(resourceType ResourceType) string {
	switch resourceType {
	case ResourceTypeFile:
		return "file"
	case ResourceTypeJar:
		return "jar"
	case ResourceTypeArchive:
		return "archive"
	case ResourceTypePy:
		return "py"
	case ResourceTypeTable:
		return "table"
	default:
		return "unknown"
	}
}

// Resource is a resource in odps
type Resource struct {
	Model       resourceModel
	ProjectName string
	OdpsIns     *Odps
	beLoaded    bool
}

type resourceModel struct {
	SchemaName       string
	Name             string
	Owner            string
	Comment          string
	ResourceType     ResourceType
	LocalPath        string
	CreationTime     string
	LastModifiedTime string
	LastUpdator      string
	ResourceSize     int64
	reportedSize     *int64
	SourceTableName  string
	ContentMD5       string
	IsTempResource   bool
	VolumePath       string
	TableName        string
}

func (m *resourceModel) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	type model resourceModel
	var entry struct {
		model
		Size *int64 `xml:"ResourceSize"`
	}
	if err := d.DecodeElement(&entry, &start); err != nil {
		return err
	}
	*m = resourceModel(entry.model)
	m.reportedSize = entry.Size
	if entry.Size != nil {
		m.ResourceSize = *entry.Size
	}
	return nil
}

// NewResource create a new Resource
func NewResource(model resourceModel, projectName string, odpsIns *Odps) *Resource {
	return &Resource{
		Model:       model,
		ProjectName: projectName,
		OdpsIns:     odpsIns,
	}
}

// Load load resource meta
func (r *Resource) Load() error {
	return r.loadContext(context.Background())
}

func (r *Resource) loadContext(ctx context.Context) error {
	if r.OdpsIns == nil {
		return errors.New("resource metadata requires a client")
	}
	client := r.OdpsIns.restClient
	rb := common.ResourceBuilder{ProjectName: r.ProjectName}
	resource := rb.Resource(r.Name())
	r.beLoaded = false

	queryArgs := make(url.Values, 4)
	if r.SchemaName() != "" {
		queryArgs.Set("curr_schema", r.SchemaName())
	}
	queryArgs.Set("meta", "")
	err := client.GetWithParseFuncContext(ctx, resource, queryArgs, nil, func(res *http.Response) error {
		header := res.Header
		if schema := header.Get(common.HttpHeaderOdpsSchemaName); schema != "" {
			r.Model.SchemaName = schema
		}
		if name := header.Get(common.HttpHeaderOdpsResourceName); name != "" {
			r.Model.Name = name
		}
		r.Model.Owner = header.Get(common.HttpHeaderOdpsOwner)
		r.Model.Comment = header.Get(common.HttpHeaderOdpsComment)
		r.Model.CreationTime = header.Get(common.HttpHeaderOdpsCreationTime)
		r.Model.LastModifiedTime = header.Get(common.HttpHeaderLastModified)
		r.Model.LastUpdator = header.Get(common.HttpHeaderOdpsUpdator)
		r.Model.ResourceType = ResourceTypeFromStr(header.Get(common.HttpHeaderOdpsResourceType))
		size, err := strconv.ParseInt(header.Get(common.HttpHeaderOdpsResourceSize), 10, 64)
		r.Model.ResourceSize = size
		r.Model.reportedSize = nil
		if err == nil {
			r.Model.reportedSize = &size
		}
		r.Model.ContentMD5 = header.Get(common.HttpHeaderContentMD5)
		r.Model.SourceTableName = header.Get(common.HttpHeaderOdpsCopyTableSource)
		r.Model.TableName = r.Model.SourceTableName
		r.Model.VolumePath = header.Get(common.HttpHeaderOdpsCopyFileSource)
		if header.Get(common.HttpHeaderOdpsResourceIsTemp) != "" {
			r.Model.IsTempResource, _ = strconv.ParseBool(header.Get(common.HttpHeaderOdpsResourceIsTemp))
		}
		return nil
	})
	r.beLoaded = err == nil
	return errors.WithStack(err)
}

// Name get resource name
func (r *Resource) Name() string {
	return r.Model.Name
}

// SetName set resource name
func (r *Resource) SetName(name string) {
	r.Model.Name = name
}

// SchemaName get resource schema name
func (r *Resource) SchemaName() string {
	return r.Model.SchemaName
}

// IsTempResource get whether resource is temp resource
func (r *Resource) IsTempResource() bool {
	return r.Model.IsTempResource
}

// SetIsTempResource set whether resource is temp resource
func (r *Resource) SetIsTempResource(isTempResource bool) {
	r.Model.IsTempResource = isTempResource
}

// Comment get resource comment
func (r *Resource) Comment() string {
	return r.Model.Comment
}

// SetComment set resource comment
func (r *Resource) SetComment(comment string) {
	r.Model.Comment = comment
}

// ResourceType get resource type
func (r *Resource) ResourceType() ResourceType {
	return r.Model.ResourceType
}

// SetResourceType set resource type
func (r *Resource) SetResourceType(resourceType ResourceType) {
	r.Model.ResourceType = resourceType
}

// Exist check whether resource exist
func (r *Resource) Exist() (bool, error) {
	err := r.Load()
	if isNotFound(err) {
		return false, nil
	}
	return err == nil, err
}

// TableResource is a table resource
type TableResource struct {
	Resource
}

// NewTableResource create a new TableResource
func NewTableResource(projectName, tableName string, partition string) TableResource {
	var resource TableResource
	if projectName != "" {
		resource.Model.SourceTableName = projectName + "." + tableName
	} else {
		resource.Model.SourceTableName = tableName
	}
	if partition != "" {
		resource.Model.SourceTableName = resource.Model.SourceTableName + " partition(" + partition + ")"
	}
	resource.Model.ResourceType = ResourceTypeTable
	return resource
}

func (r *TableResource) getSourceTableName() string {
	if r.Model.SourceTableName == "" && r.OdpsIns != nil {
		_ = r.Load()
	}
	return r.Model.SourceTableName
}

// ReaderResource is a resource that created with a reader
type ReaderResource interface {
	SetReader(reader io.Reader)
	Reader() io.Reader
	Name() string
	Comment() string
	IsTempResource() bool
	ResourceType() ResourceType
}

// FileResource is a file resource
type FileResource struct {
	Resource
	reader io.Reader
}

// NewFileResource create a new FileResource
func NewFileResource(resourceName string) *FileResource {
	return &FileResource{
		Resource: Resource{Model: resourceModel{Name: resourceName, ResourceType: ResourceTypeFile}},
	}
}

// SetReader set reader
func (fr *FileResource) SetReader(reader io.Reader) {
	fr.reader = reader
}

// Reader get reader
func (fr *FileResource) Reader() io.Reader {
	return fr.reader
}

// JarResource is a jar resource
type JarResource struct {
	FileResource
}

// NewJarResource create a new JarResource
func NewJarResource(resourceName string) *JarResource {
	return &JarResource{
		FileResource: FileResource{Resource: Resource{Model: resourceModel{Name: resourceName, ResourceType: ResourceTypeJar}}},
	}
}

// PyResource is a py resource
type PyResource struct {
	FileResource
}

// NewPyResource create a new PyResource
func NewPyResource(resourceName string) *PyResource {
	return &PyResource{
		FileResource: FileResource{Resource: Resource{Model: resourceModel{Name: resourceName, ResourceType: ResourceTypePy}}},
	}
}

// ArchiveResource is a archive resource
type ArchiveResource struct {
	FileResource
}

// NewArchiveResource create a new ArchiveResource
func NewArchiveResource(resourceName string) *ArchiveResource {
	return &ArchiveResource{
		FileResource: FileResource{Resource: Resource{Model: resourceModel{Name: resourceName, ResourceType: ResourceTypeArchive}}},
	}
}
