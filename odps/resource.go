package odps

import (
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/pkg/errors"
)

type ResourceType string

const (
	/**
	 * 文件类型资源
	 */
	ResourceTypeFile ResourceType = "file"
	/**
	 * jar类型资源
	 */
	ResourceTypeJar ResourceType = "jar"
	/**
	 * ARCHIVE类型资源。ODPS通过资源名称中的后缀识别压缩类型，支持的压缩文件类型包括：.zip/.tgz/.tar.gz/.tar/.jar。
	 */
	ResourceTypeArchive ResourceType = "archive"
	/**
	 * PY类型资源
	 */
	ResourceTypePy ResourceType = "py"
	/**
	 * TABLE类型资源
	 */
	ResourceTypeTable ResourceType = "table"

	ResourceTypeVolumeFile ResourceType = "volume_file"

	ResourceTypeVolumeArchive ResourceType = "volume_archive"
	/**
	 * 用户设置的或系统暂不支持的资源类型
	 */
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
	SourceTableName  string
	ContentMD5       string
	IsTempResource   bool
	VolumePath       string
	TableName        string
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
	client := r.OdpsIns.restClient
	resource := r.ResourceUrl()
	r.beLoaded = true

	queryArgs := make(url.Values, 4)
	if r.SchemaName() != "" {
		queryArgs.Set("curr_schema", r.SchemaName())
	}
	queryArgs.Set("meta", "")
	err := client.GetWithParseFunc(resource, queryArgs, func(res *http.Response) error {
		header := res.Header
		r.Model.SchemaName = header.Get(common.HttpHeaderOdpsSchemaName)
		r.Model.Name = header.Get(common.HttpHeaderOdpsResourceName)
		r.Model.Owner = header.Get(common.HttpHeaderOdpsOwner)
		r.Model.Comment = header.Get(common.HttpHeaderOdpsComment)
		r.Model.CreationTime = header.Get(common.HttpHeaderOdpsCreationTime)
		r.Model.LastModifiedTime = header.Get(common.HttpHeaderLastModified)
		r.Model.LastUpdator = header.Get(common.HttpHeaderOdpsUpdator)
		r.Model.ResourceType = ResourceTypeFromStr(header.Get(common.HttpHeaderOdpsResourceType))
		r.Model.TableName = header.Get(common.HttpHeaderOdpsResourceType)
		r.Model.ResourceSize, _ = strconv.ParseInt(header.Get(common.HttpHeaderOdpsResourceSize), 10, 64)
		r.Model.ContentMD5 = header.Get(common.HttpHeaderContentMD5)
		r.Model.SourceTableName = header.Get(common.HttpHeaderOdpsCopyTableSource)
		r.Model.VolumePath = header.Get(common.HttpHeaderOdpsCopyFileSource)
		if header.Get(common.HttpHeaderOdpsResourceIsTemp) != "" {
			r.Model.IsTempResource, _ = strconv.ParseBool(header.Get(common.HttpHeaderOdpsResourceIsTemp))
		}
		return nil
	})
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

// ResourceUrl get resource url
func (r *Resource) ResourceUrl() string {
	rb := common.ResourceBuilder{ProjectName: r.ProjectName}
	return rb.Resource(r.Name())
}

// Exist check whether resource exist
func (r *Resource) Exist() (bool, error) {
	err := r.Load()
	var httpErr restclient.HttpError
	if errors.As(err, &httpErr) {
		if httpErr.Status == "404 Not Found" {
			return false, nil
		}
		return false, err
	}

	return true, nil
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
		r.Resource.Load()
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
