package odps

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
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

	rb := common.ResourceBuilder{ProjectName: r.ProjectName}
	resource := rb.Resources()
	client := r.OdpsIns.restClient

	type ResModel struct {
		XMLName   xml.Name        `xml:"Resources"`
		Resources []resourceModel `xml:"Resource"`
		Marker    string
		MaxItems  int
	}
	var resModel ResModel

	for {
		err := client.GetWithModel(resource, queryArgs, &resModel)
		if err != nil {
			f(nil, err)
			break
		}
		for _, resourceModel := range resModel.Resources {
			resource := NewResource(resourceModel, r.ProjectName, r.OdpsIns)
			resource.beLoaded = true
			f(resource, nil)
		}

		if resModel.Marker != "" {
			queryArgs.Set("marker", resModel.Marker)
			resModel = ResModel{}
		} else {
			break
		}
	}

}

// Get get a resource
func (r *Resources) Get(resourceName string) *Resource {
	resource := NewResource(resourceModel{Name: resourceName}, r.ProjectName, r.OdpsIns)
	return resource
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
	if lfr.Reader() == nil {
		return errors.New("reader cannot be nil")
	}

	tmpContent := make([]byte, r.ChunkSize)
	totalBytes, cnt := int64(0), 0
	var tmpFiles []string
	hash := md5.New()

	for {
		n, err := lfr.Reader().Read(tmpContent)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		hash.Write(tmpContent[:n])

		tmpReader := bytes.NewBuffer(tmpContent[:n])
		tmpName := ""
		if schemaName != "" {
			tmpName = fmt.Sprintf("%s.%s.part.tmp.%06d", schemaName, lfr.Name(), cnt)
		} else {
			tmpName = fmt.Sprintf("%s.part.tmp.%06d", lfr.Name(), cnt)
		}

		tmp := NewFileResource(tmpName)
		tmp.SetReader(tmpReader)
		tmp.SetIsTempResource(true)
		tmpFiles = append(tmpFiles, tmpName)
		err = r.createTempPartFile(projectName, schemaName, *tmp, tmpReader, int64(n))
		if err != nil {
			return errors.WithStack(err)
		}

		cnt++
		totalBytes += int64(n)
	}

	md5sum := hex.EncodeToString(hash.Sum(nil))
	commitContent := fmt.Sprintf("%s|%s", md5sum, strings.Join(tmpFiles, ","))
	commitReader := bytes.NewBuffer([]byte(commitContent))

	tmp := FileResource{Resource: Resource{Model: resourceModel{ResourceType: ResourceTypeFile}}}
	tmp.SetName(lfr.Name())
	tmp.SetComment(lfr.Comment())
	tmp.SetIsTempResource(lfr.IsTempResource())
	tmp.SetResourceType(lfr.ResourceType())
	return errors.WithStack(r.mergeTempPartFile(projectName, schemaName, tmp, commitReader, overwrite, totalBytes))
}

// UpdateFileResource update a file resource
func (r *Resources) UpdateFileResource(projectName, schemaName string, lfr ReaderResource) error {
	return r.CreateFileResource(projectName, schemaName, lfr, true)
}

func (r *Resources) createTempPartFile(projectName, schemaName string, fr FileResource, reader *bytes.Buffer, length int64) error {
	if fr.Name() == "" {
		return errors.New("Temp part resource name cannot be empty")
	}
	if !fr.IsTempResource() {
		return errors.New("Temp part resource must be temp resource")
	}
	if fr.ResourceType() == ResourceTypeVolumeFile || fr.ResourceType() == ResourceTypeArchive || fr.ResourceType() == ResourceTypeTable || fr.ResourceType() == ResourceTypeUnknown {
		return errors.New("Temp part resource type must be file")
	}

	rb := common.ResourceBuilder{ProjectName: projectName}
	resource := rb.Resources()

	headers := make(map[string]string)
	headers[common.HttpHeaderContentType] = "application/octet-stream"
	headers[common.HttpHeaderContentDisposition] = "attachment;filename=" + fr.Name()
	headers[common.HttpHeaderOdpsResourceType] = ResourceTypeToStr(fr.ResourceType())
	headers[common.HttpHeaderOdpsResourceName] = fr.Name()
	if fr.Comment() != "" {
		headers[common.HttpHeaderOdpsComment] = fr.Comment()
	}
	headers[common.HttpHeaderOdpsResourceIsTemp] = strconv.FormatBool(fr.IsTempResource())
	headers[common.HttpHeaderContentLength] = strconv.FormatInt(length, 10)

	tmpReader := bytes.NewReader(reader.Bytes())
	if headers[common.HttpHeaderContentMD5] == "" {
		hash := md5.New()
		_, _ = io.Copy(hash, reader)
		headers[common.HttpHeaderContentMD5] = hex.EncodeToString(hash.Sum(nil))
	}

	queryArgs := make(url.Values, 4)
	if schemaName != "" {
		queryArgs.Set("curr_schema", schemaName)
	}
	queryArgs.Set("rIsPart", "true")

	client := r.OdpsIns.restClient
	req, err := client.NewRequestWithParamsAndHeaders(common.HttpMethod.PostMethod, resource, tmpReader, queryArgs, headers)
	if err != nil {
		return errors.WithStack(err)
	}
	client.HttpTimeout = 60 * time.Second
	resp, err := client.Do(req)
	if err != nil {
		return errors.WithStack(err)
	}
	if resp.StatusCode != 201 {
		data, _ := io.ReadAll(resp.Body)
		return errors.WithStack(errors.New(string(data)))
	}
	return nil
}

func (r *Resources) mergeTempPartFile(projectName, schemaName string, fr FileResource, reader *bytes.Buffer, overwrite bool, totalBytes int64) error {
	if fr.Name() == "" {
		return errors.New("File resource name cannot be empty")
	}

	rb := common.NewResourceBuilder(projectName)
	var resource, method string
	if overwrite {
		resource = rb.Resource(fr.Name())
		method = common.HttpMethod.PutMethod
	} else {
		resource = rb.Resources()
		method = common.HttpMethod.PostMethod
	}

	headers := make(map[string]string)
	headers[common.HttpHeaderContentType] = "application/octet-stream"
	headers[common.HttpHeaderContentDisposition] = "attachment;filename=" + fr.Name()
	headers[common.HttpHeaderOdpsResourceType] = ResourceTypeToStr(fr.ResourceType())
	headers[common.HttpHeaderOdpsResourceName] = fr.Name()
	if fr.Comment() != "" {
		headers[common.HttpHeaderOdpsComment] = fr.Comment()
	}
	if fr.IsTempResource() {
		headers[common.HttpHeaderOdpsResourceIsTemp] = strconv.FormatBool(fr.IsTempResource())
	}
	headers[common.HttpHeaderOdpsResourceMergeTotalBytes] = strconv.FormatInt(totalBytes, 10)
	headers[common.HttpHeaderContentLength] = strconv.FormatInt(totalBytes, 10)
	tmpReader := bytes.NewReader(reader.Bytes())
	if headers[common.HttpHeaderContentMD5] == "" {
		hash := md5.New()
		_, _ = io.Copy(hash, reader)
		headers[common.HttpHeaderContentMD5] = hex.EncodeToString(hash.Sum(nil))
	}

	queryArgs := make(url.Values, 4)
	if schemaName != "" {
		queryArgs.Set("curr_schema", schemaName)
	}
	queryArgs.Set("rOpMerge", "true")

	client := r.OdpsIns.restClient
	req, err := client.NewRequestWithParamsAndHeaders(method, resource, tmpReader, queryArgs, headers)
	if err != nil {
		return errors.WithStack(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return errors.WithStack(err)
	}
	if resp.StatusCode != 201 {
		data, _ := io.ReadAll(resp.Body)
		return errors.WithStack(errors.New(string(data)))
	}

	return nil
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
	_, err = client.Do(req)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
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
	resp, err := client.Do(req)
	if err != nil {
		return errors.WithStack(err)
	}
	if resp.StatusCode != 201 {
		data, _ := io.ReadAll(resp.Body)
		return errors.WithStack(errors.New(string(data)))
	}

	return nil
}

// RFileFunc is a function to filter resources
type RFileFunc func(url.Values)

// ResourceFilter is a filter for resources
var ResourceFilter = struct {
	// Filter out resources with name prefix
	NamePrefix func(string) TFilterFunc
}{
	NamePrefix: func(name string) TFilterFunc {
		return func(values url.Values) {
			values.Set("name", name)
		}
	},
}
