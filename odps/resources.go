package odps

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
)

// Resources is a collection of resources
type Resources struct {
	ProjectName string
	schemaName  string
	OdpsIns     *Odps
	ChunkSize   int
}

// NewResources create a new Resources
func NewResources(OdpsIns *Odps) *Resources {
	return &Resources{
		ProjectName: OdpsIns.DefaultProjectName(),
		schemaName:  OdpsIns.CurrentSchemaName(),
		OdpsIns:     OdpsIns,
		ChunkSize:   64 << 20,
	}
}

// List list resources with filters
func (r *Resources) List(f func(*Resource, error), filters ...RFileFunc) {
	queryArgs := make(url.Values, 4)
	queryArgs.Set("expectmarker", "true")
	queryArgs.Set("curr_schema", r.schemaName)

	for _, filter := range filters {
		if filter != nil {
			filter(queryArgs)
		}
	}

	client := r.OdpsIns.restClient

	for {
		page, err := resourcesPage(context.Background(), client, r.ProjectName, queryArgs)
		if err != nil {
			f(nil, err)
			break
		}
		for _, resourceModel := range page.Items {
			if resourceModel.SchemaName == "" {
				resourceModel.SchemaName = r.schemaName
			}
			resource := NewResource(resourceModel, r.ProjectName, r.OdpsIns)
			resource.beLoaded = true
			f(resource, nil)
		}

		if page.NextPageToken != "" {
			queryArgs.Set("marker", page.NextPageToken)
		} else {
			break
		}
	}
}

// Get get a resource
func (r *Resources) Get(resourceName string) *Resource {
	return NewResource(resourceModel{Name: resourceName, SchemaName: r.schemaName}, r.ProjectName, r.OdpsIns)
}

// CreateFileResource create a file resource
func (r *Resources) CreateFileResource(projectName, schemaName string, lfr ReaderResource, overwrite bool) error {
	if projectName == "" {
		projectName = r.ProjectName
	}
	if schemaName == "" {
		schemaName = r.schemaName
	}
	if lfr.Name() == "" {
		return errors.New("resource name cannot be empty")
	}
	reader := lfr.Reader()
	if reader == nil {
		return errors.New("reader cannot be nil")
	}
	if r.ChunkSize <= 0 {
		return errors.New("resource chunk size must be positive")
	}

	partPrefix := lfr.Name()
	if schemaName != "" {
		partPrefix = schemaName + "." + partPrefix
	}
	buffer := make([]byte, r.ChunkSize)
	var totalBytes int64
	var parts []string
	hash := md5.New()
	const maxEmptyReads = 100
	emptyReads := 0

	for {
		n, err := reader.Read(buffer)
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		if n == 0 {
			if err != nil {
				break
			}
			emptyReads++
			if emptyReads == maxEmptyReads {
				return io.ErrNoProgress
			}
			continue
		}
		emptyReads = 0
		part := buffer[:n]
		hash.Write(part)

		tmp := NewFileResource(fmt.Sprintf("%s.part.tmp.%06d", partPrefix, len(parts)))
		tmp.SetIsTempResource(true)
		if err := r.putFileResource(projectName, schemaName, tmp, false, part, url.Values{"rIsPart": {"true"}}, 0); err != nil {
			return err
		}

		parts = append(parts, tmp.Name())
		totalBytes += int64(n)
		if err != nil {
			break
		}
	}

	if len(parts) == 0 {
		return r.putFileResource(projectName, schemaName, lfr, overwrite, nil, url.Values{}, 0)
	}
	manifest := hex.EncodeToString(hash.Sum(nil)) + "|" + strings.Join(parts, ",")
	return r.putFileResource(projectName, schemaName, lfr, overwrite, []byte(manifest), url.Values{"rOpMerge": {"true"}}, totalBytes)
}

// UpdateFileResource update a file resource
func (r *Resources) UpdateFileResource(projectName, schemaName string, lfr ReaderResource) error {
	return r.CreateFileResource(projectName, schemaName, lfr, true)
}

func (r *Resources) putFileResource(projectName, schemaName string, fr ReaderResource, overwrite bool, body []byte, queryArgs url.Values, totalBytes int64) error {
	rb := common.NewResourceBuilder(projectName)
	resource, method := rb.Resources(), common.HttpMethod.PostMethod
	if overwrite {
		resource = rb.Resource(fr.Name())
		method = common.HttpMethod.PutMethod
	}

	digest := md5.Sum(body)
	headers := map[string]string{
		common.HttpHeaderContentType:        "application/octet-stream",
		common.HttpHeaderContentDisposition: "attachment;filename=" + fr.Name(),
		common.HttpHeaderContentMD5:         hex.EncodeToString(digest[:]),
		common.HttpHeaderOdpsResourceType:   ResourceTypeToStr(fr.ResourceType()),
		common.HttpHeaderOdpsResourceName:   fr.Name(),
	}
	if comment := fr.Comment(); comment != "" {
		headers[common.HttpHeaderOdpsComment] = comment
	}
	if fr.IsTempResource() {
		headers[common.HttpHeaderOdpsResourceIsTemp] = "true"
	}
	if queryArgs.Has("rOpMerge") {
		headers[common.HttpHeaderOdpsResourceMergeTotalBytes] = strconv.FormatInt(totalBytes, 10)
	}

	if schemaName != "" {
		queryArgs.Set("curr_schema", schemaName)
	}
	client := r.OdpsIns.restClient
	req, err := client.NewRequestWithParamsAndHeaders(method, resource, bytes.NewReader(body), queryArgs, headers)
	if err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(client.DoWithParseFunc(req, nil))
}

// Delete delete a resource
func (r *Resources) Delete(resourceName string) error {
	rb := common.NewResourceBuilder(r.ProjectName)
	resource := rb.Resource(resourceName)

	queryArgs := make(url.Values, 4)
	if r.schemaName != "" {
		queryArgs.Set("curr_schema", r.schemaName)
	}

	client := r.OdpsIns.restClient
	req, err := client.NewRequestWithUrlQuery(common.HttpMethod.DeleteMethod, resource, nil, queryArgs)
	if err != nil {
		return errors.WithStack(err)
	}
	client.HttpTimeout = 60 * time.Second
	return errors.WithStack(client.DoWithParseFunc(req, nil))
}

// CreateTableResource create a table resource
func (r *Resources) CreateTableResource(projectName, schemaName string, tr TableResource, overwrite bool) error {
	if projectName == "" {
		projectName = r.ProjectName
	}
	if schemaName == "" {
		schemaName = r.schemaName
	}
	if tr.Name() == "" {
		return errors.New("resource name cannot be empty")
	}

	rb := common.NewResourceBuilder(projectName)
	var resource, method string
	if overwrite {
		resource = rb.Resource(tr.Name())
		method = common.HttpMethod.PutMethod
	} else {
		resource = rb.Resources()
		method = common.HttpMethod.PostMethod
	}

	headers := make(map[string]string)
	headers[common.HttpHeaderContentType] = "text/plain"
	headers[common.HttpHeaderOdpsResourceType] = ResourceTypeToStr(tr.ResourceType())
	headers[common.HttpHeaderOdpsResourceName] = tr.Name()
	headers[common.HttpHeaderOdpsCopyTableSource] = tr.getSourceTableName()
	if tr.Comment() != "" {
		headers[common.HttpHeaderOdpsComment] = tr.Comment()
	}

	queryArgs := make(url.Values, 4)
	if schemaName != "" {
		queryArgs.Set("curr_schema", schemaName)
	}

	client := r.OdpsIns.restClient
	req, err := client.NewRequestWithParamsAndHeaders(method, resource, nil, queryArgs, headers)
	if err != nil {
		return errors.WithStack(err)
	}
	client.HttpTimeout = 60 * time.Second
	return errors.WithStack(client.DoWithParseFunc(req, nil))
}

// RFileFunc is a function to filter resources
type RFileFunc func(url.Values)

// ResourceFilter is a filter for resources
var ResourceFilter = struct {
	// Filter out resources with name prefix
	NamePrefix func(string) RFileFunc
}{
	NamePrefix: func(name string) RFileFunc {
		return func(values url.Values) {
			values.Set("name", name)
		}
	},
}
